package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"cargo-slots/internal/domain"
	"cargo-slots/internal/service"
	"cargo-slots/internal/storage"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Handler struct {
	service     *service.ReservationService
	pgRepo      *storage.PostgresRepository
	redisClient *redis.Client
}

func NewHandler(svc *service.ReservationService, pgRepo *storage.PostgresRepository, redisClient *redis.Client) *Handler {
	return &Handler{
		service:     svc,
		pgRepo:      pgRepo,
		redisClient: redisClient,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/reservations", h.CreateReservation)
	mux.HandleFunc("GET /api/v1/docks/stream", h.StreamDockEvents)
	mux.HandleFunc("GET /health", h.Healthcheck)
}

type CreateReservationRequest struct {
	DockID      string `json:"dock_id"`
	DockName    string `json:"dock_name"`
	ForwarderID string `json:"forwarder_id"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
}

type ReservationResponse struct {
	ID          string `json:"id"`
	DockID      string `json:"dock_id"`
	ForwarderID string `json:"forwarder_id"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
}

// CreateReservation maneja la creación de reservas mediante el endpoint POST /api/v1/reservations.
// Respuestas:
// - 201 Created: Reserva asignada y persistida.
// - 409 Conflict: Double booking en Postgres (SQLSTATE 23P01).
// - 423 Locked: Bloqueo distribuido activo en Redis por colisión concurrente inmediata.
// - 400 Bad Request: Formato JSON o parámetros inválidos.
func (h *Handler) CreateReservation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req CreateReservationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload: " + err.Error()})
		return
	}

	if req.ForwarderID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "forwarder_id is required"})
		return
	}

	if req.DockID == "" && req.DockName == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "either dock_id or dock_name must be provided"})
		return
	}

	startTime, err := time.Parse(time.RFC3339, req.StartTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid start_time format (expected RFC3339)"})
		return
	}

	endTime, err := time.Parse(time.RFC3339, req.EndTime)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid end_time format (expected RFC3339)"})
		return
	}

	var dockUUID uuid.UUID
	if req.DockID != "" {
		parsed, err := uuid.Parse(req.DockID)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid dock_id format (expected UUID)"})
			return
		}
		dockUUID = parsed
	}

	input := service.BookSlotInput{
		DockID:      dockUUID,
		DockName:    req.DockName,
		ForwarderID: req.ForwarderID,
		StartTime:   startTime,
		EndTime:     endTime,
	}

	res, err := h.service.BookSlot(r.Context(), input)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrLockBusy):
			// 423 Locked: Rechazo inmediato por Redis Lock
			w.WriteHeader(http.StatusLocked)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "Slot is currently locked by another concurrent process",
				"details": err.Error(),
			})
			return

		case errors.Is(err, domain.ErrDoubleBooking):
			// 409 Conflict: Superposición temporal detectada por PostgreSQL GiST
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "Double booking: overlapping confirmed reservation exists on this dock",
				"details": err.Error(),
			})
			return

		case errors.Is(err, domain.ErrInvalidTimeRange):
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return

		case errors.Is(err, domain.ErrDockNotFound):
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return

		default:
			log.Printf("[WARN] Infrastructure failure during booking request: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "Service temporarily unavailable: infrastructure failover or connection disruption",
				"details": err.Error(),
			})
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(ReservationResponse{
		ID:          res.ID.String(),
		DockID:      res.DockID.String(),
		ForwarderID: res.ForwarderID,
		StartTime:   res.StartTime.UTC().Format(time.RFC3339),
		EndTime:     res.EndTime.UTC().Format(time.RFC3339),
		Status:      string(res.Status),
		CreatedAt:   res.CreatedAt.UTC().Format(time.RFC3339),
	})
}

// StreamDockEvents implementa Server-Sent Events (SSE) para broadcast en tiempo real
// mediante suscripción distribuida a Redis Pub/Sub (canal docks.events).
// Detecta la desconexión del cliente (r.Context().Done()) para evitar memory leaks masivos.
func (h *Handler) StreamDockEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Suscripción a Redis Pub/Sub en el canal docks.events
	pubsub := h.redisClient.Subscribe(r.Context(), "docks.events")
	defer pubsub.Close() // CRÍTICO: Evita fugar la conexión a Redis

	// Confirmación inicial de handshake SSE
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	ch := pubsub.Channel()

	for {
		select {
		case <-r.Context().Done():
			// El cliente (navegador/dashboard/curl) cerró la conexión
			// Si no manejas esto, la goroutine vivirá para siempre (Memory Leak).
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			// Formato estricto de SSE: "data: {json}\n\n"
			fmt.Fprintf(w, "data: %s\n\n", msg.Payload)
			flusher.Flush()
		}
	}
}

// Healthcheck verifica de forma estricta la salud de PostgreSQL y Redis.
// Retorna 200 OK únicamente si ambos responden al ping, o 503 Service Unavailable si alguno falla.
func (h *Handler) Healthcheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	pgErr := h.pgRepo.Ping(ctx)
	redisErr := h.redisClient.Ping(ctx).Err()

	if pgErr != nil || redisErr != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		res := map[string]any{
			"status":   "UNHEALTHY",
			"postgres": "UP",
			"redis":    "UP",
		}
		if pgErr != nil {
			res["postgres"] = fmt.Sprintf("DOWN: %v", pgErr)
		}
		if redisErr != nil {
			res["redis"] = fmt.Sprintf("DOWN: %v", redisErr)
		}
		_ = json.NewEncoder(w).Encode(res)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":   "UP",
		"postgres": "UP",
		"redis":    "UP",
	})
}
