package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"cargo-slots/internal/domain"

	"github.com/google/uuid"
)

type Config struct {
	LockTTL   time.Duration // TTL del lock en Redis (ej. 2000ms)
	DBTimeout time.Duration // Timeout máximo de la transacción en PostgreSQL (ej. 3000ms)
}

type ReservationService struct {
	repo      domain.ReservationRepository
	locker    domain.Locker
	publisher domain.EventPublisher
	cfg       Config
}

// NewReservationService inicializa el servicio de reservas y valida estrictamente el Invariante 3.
func NewReservationService(repo domain.ReservationRepository, locker domain.Locker, publisher domain.EventPublisher, cfg Config) (*ReservationService, error) {
	// Invariante 3 (Regla absoluta):
	// "El TTL (Time-To-Live) del bloqueo en Redis NUNCA debe ser mayor al timeout de la transacción en PostgreSQL"
	if cfg.LockTTL > cfg.DBTimeout {
		return nil, fmt.Errorf("%w: LockTTL (%v) > DBTimeout (%v)", domain.ErrInvariant3Violation, cfg.LockTTL, cfg.DBTimeout)
	}

	if cfg.LockTTL <= 0 {
		cfg.LockTTL = 2 * time.Second
	}
	if cfg.DBTimeout <= 0 {
		cfg.DBTimeout = 3 * time.Second
	}

	return &ReservationService{
		repo:      repo,
		locker:    locker,
		publisher: publisher,
		cfg:       cfg,
	}, nil
}

type BookSlotInput struct {
	DockID      uuid.UUID
	DockName    string
	ForwarderID string
	StartTime   time.Time
	EndTime     time.Time
}

// BookSlot orquesta la reserva atómica protegiendo a la base de datos mediante el distributed lock en Redis.
// 1. Calcula la key canónica en Redis con taxonomía exacta por franja.
// 2. Intenta Acquire en Redis (SET NX PX). Si falla, retorna de inmediato ("Slot ocupado" / ErrLockBusy) sin tocar DB.
// 3. Si tiene éxito, ejecuta el INSERT en PostgreSQL con timeout acotado por DBTimeout.
// 4. Utiliza defer l.Release(...) con script Lua para garantizar liberación segura.
func (s *ReservationService) BookSlot(ctx context.Context, input BookSlotInput) (*domain.Reservation, error) {
	if input.EndTime.Before(input.StartTime) || input.EndTime.Equal(input.StartTime) {
		return nil, domain.ErrInvalidTimeRange
	}

	dockIdentifier := input.DockName
	if dockIdentifier == "" {
		dockIdentifier = input.DockID.String()
	}

	// 1. Calcular la key de Redis
	lockKey := domain.BuildSlotLockKey(dockIdentifier, input.StartTime, input.EndTime)

	// 2. Intentar Acquire (Fast Bouncer en Redis). Si falla, retornar rápido.
	token, err := s.locker.Acquire(ctx, lockKey, int(s.cfg.LockTTL.Milliseconds()))
	if err != nil {
		return nil, err // Si es domain.ErrLockBusy retorna de inmediato sin tocar DB
	}

	// 4. Usar defer l.Release(...) para liberar el lock pase lo que pase en BD.
	// Se usa un contexto independiente con timeout para asegurar la ejecución del script Lua
	// incluso si el contexto de la petición fue cancelado.
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if relErr := s.locker.Release(releaseCtx, lockKey, token); relErr != nil {
			log.Printf("[WARN] Failed to release distributed lock %s: %v", lockKey, relErr)
		}
	}()

	// Resolver DockID si solo se proporcionó DockName
	dockID := input.DockID
	if dockID == uuid.Nil {
		dock, err := s.repo.GetDockByName(ctx, input.DockName)
		if err != nil {
			return nil, err
		}
		dockID = dock.ID
	}

	res := &domain.Reservation{
		DockID:      dockID,
		ForwarderID: input.ForwarderID,
		StartTime:   input.StartTime,
		EndTime:     input.EndTime,
		Status:      domain.StatusConfirmed,
	}

	// 3. Ejecutar el INSERT en PostgreSQL garantizando context.Context con timeout
	dbCtx, cancelDB := context.WithTimeout(ctx, s.cfg.DBTimeout)
	defer cancelDB()

	if err := s.repo.InsertReservation(dbCtx, res); err != nil {
		return nil, err
	}

	// 5. Propagación de Eventos (Redis Pub/Sub):
	// Trampa evitada: Publicar en Redis Pub/Sub ÚNICAMENTE después de que PostgreSQL
	// confirme la transacción con éxito (cero eventos fantasma en el dashboard).
	if s.publisher != nil {
		event := domain.DockEvent{
			DockID:      res.DockID.String(),
			DockName:    input.DockName,
			ForwarderID: res.ForwarderID,
			Status:      string(res.Status),
			TimeRange:   fmt.Sprintf("[%s, %s)", res.StartTime.UTC().Format(time.RFC3339), res.EndTime.UTC().Format(time.RFC3339)),
			Timestamp:   time.Now().UTC().Format(time.RFC3339),
		}
		if pubErr := s.publisher.Publish(ctx, "docks.events", event); pubErr != nil {
			log.Printf("[WARN] Failed to publish dock event to Redis Pub/Sub: %v", pubErr)
		}
	}

	return res, nil
}
