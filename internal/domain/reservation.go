package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ReservationStatus string

const (
	StatusPending   ReservationStatus = "PENDING"
	StatusConfirmed ReservationStatus = "CONFIRMED"
	StatusExpired   ReservationStatus = "EXPIRED"
	StatusCancelled ReservationStatus = "CANCELLED"
)

// Dock representa un muelle físico de atraque/carga.
type Dock struct {
	ID   uuid.UUID
	Name string
}

// Reservation representa una reserva de franja horaria en un muelle.
type Reservation struct {
	ID          uuid.UUID
	DockID      uuid.UUID
	ForwarderID string
	StartTime   time.Time
	EndTime     time.Time
	Status      ReservationStatus
	CreatedAt   time.Time
}

// Locker abstrae el mecanismo de bloqueo distribuido (Redis).
type Locker interface {
	Acquire(ctx context.Context, key string, ttlMs int) (string, error)
	Release(ctx context.Context, key, token string) error
}

// ReservationRepository abstrae la persistencia en base de datos (PostgreSQL).
type ReservationRepository interface {
	CreateDock(ctx context.Context, name string) (*Dock, error)
	GetDockByName(ctx context.Context, name string) (*Dock, error)
	GetDockByID(ctx context.Context, id uuid.UUID) (*Dock, error)
	InsertReservation(ctx context.Context, res *Reservation) error
	CountReservations(ctx context.Context, dockID uuid.UUID, startTime, endTime time.Time) (int, error)
}

// BuildSlotLockKey genera una clave canónica en Redis con taxonomía exacta por muelle y rango horario [inicio, fin).
// Formato: lock:dock:<dock_identifier>:<start_rfc3339>_<end_rfc3339>
// Ejemplo: lock:dock:M-01:2026-11-01T14:00:00Z_2026-11-01T16:00:00Z
func BuildSlotLockKey(dockIdentifier string, startTime, endTime time.Time) string {
	return fmt.Sprintf("lock:dock:%s:%s_%s",
		dockIdentifier,
		startTime.UTC().Format(time.RFC3339),
		endTime.UTC().Format(time.RFC3339),
	)
}
