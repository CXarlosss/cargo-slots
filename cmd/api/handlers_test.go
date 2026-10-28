package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cargo-slots/internal/domain"
	"cargo-slots/internal/service"
	"cargo-slots/internal/storage"
)

func setupTestApp(t *testing.T) (*Handler, *storage.PostgresRepository, *storage.RedisLocker, func()) {
	ctx := context.Background()
	dbURL := getDatabaseURL()
	pgRepo, err := storage.NewPostgresPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to test postgres: %v", err)
	}

	redisAddr := getRedisAddr()
	redisClient, err := storage.NewRedisClient(ctx, redisAddr, "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to test redis: %v", redisAddr)
	}

	locker := storage.NewRedisLocker(redisClient)
	svcCfg := service.Config{
		LockTTL:   2000 * time.Millisecond,
		DBTimeout: 3000 * time.Millisecond,
	}

	svc, err := service.NewReservationService(pgRepo, locker, locker, svcCfg)
	if err != nil {
		t.Fatalf("Failed to create reservation service: %v", err)
	}

	handler := NewHandler(svc, pgRepo, redisClient)

	cleanup := func() {
		redisClient.Close()
		pgRepo.Close()
	}

	return handler, pgRepo, locker, cleanup
}

func TestHealthcheckHandler(t *testing.T) {
	handler, _, _, cleanup := setupTestApp(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.Healthcheck(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /health, got: %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("Failed to decode healthcheck response: %v", err)
	}

	if body["status"] != "UP" || body["postgres"] != "UP" || body["redis"] != "UP" {
		t.Fatalf("Unexpected healthcheck body: %+v", body)
	}
}

func TestCreateReservationHandler_SuccessAndConflict(t *testing.T) {
	handler, pgRepo, _, cleanup := setupTestApp(t)
	defer cleanup()

	ctx := context.Background()
	dockName := fmt.Sprintf("DOCK-HTTP-%d", time.Now().UnixNano())
	dock, err := pgRepo.CreateDock(ctx, dockName)
	if err != nil {
		t.Fatalf("Failed to create dock: %v", err)
	}

	startTime := time.Now().Add(24 * time.Hour).Truncate(time.Hour).UTC().Format(time.RFC3339)
	endTime := time.Now().Add(26 * time.Hour).Truncate(time.Hour).UTC().Format(time.RFC3339)

	// 1. Primera petición: 201 Created
	payload := CreateReservationRequest{
		DockID:      dock.ID.String(),
		DockName:    dock.Name,
		ForwarderID: "FORWARDER-HTTP-1",
		StartTime:   startTime,
		EndTime:     endTime,
	}
	bodyBytes, _ := json.Marshal(payload)

	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", bytes.NewReader(bodyBytes))
	rec1 := httptest.NewRecorder()
	handler.CreateReservation(rec1, req1)

	if rec1.Code != http.StatusCreated {
		t.Fatalf("Expected 201 Created on first booking, got: %d, body: %s", rec1.Code, rec1.Body.String())
	}

	// 2. Segunda petición (misma franja, posterior a la liberación del lock de Redis):
	// Debe saltar la restricción de exclusión de PostgreSQL -> 409 Conflict
	payload2 := CreateReservationRequest{
		DockID:      dock.ID.String(),
		DockName:    dock.Name,
		ForwarderID: "FORWARDER-HTTP-2",
		StartTime:   startTime,
		EndTime:     endTime,
	}
	bodyBytes2, _ := json.Marshal(payload2)

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/reservations", bytes.NewReader(bodyBytes2))
	rec2 := httptest.NewRecorder()
	handler.CreateReservation(rec2, req2)

	if rec2.Code != http.StatusConflict {
		t.Fatalf("Expected 409 Conflict on overlapping booking, got: %d, body: %s", rec2.Code, rec2.Body.String())
	}
}

func TestStreamDockEvents_SSE(t *testing.T) {
	handler, _, locker, cleanup := setupTestApp(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/docks/stream", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.StreamDockEvents(rec, req)
	}()

	// Esperamos a que la suscripción esté lista
	time.Sleep(100 * time.Millisecond)

	// Publicar un evento en docks.events
	testEvent := domain.DockEvent{
		DockID:      "test-dock-id",
		DockName:    "M-01",
		ForwarderID: "TEST-FWD",
		Status:      "CONFIRMED",
		TimeRange:   "[2026-11-01T14:00:00Z, 2026-11-01T16:00:00Z)",
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
	}

	if err := locker.Publish(context.Background(), "docks.events", testEvent); err != nil {
		t.Fatalf("Failed to publish test event: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	cancel() // Simular desconexión del cliente
	<-done

	responseStr := rec.Body.String()
	if !strings.Contains(responseStr, ": connected") {
		t.Fatalf("Expected SSE stream to contain ': connected', got: %s", responseStr)
	}
	if !strings.Contains(responseStr, "test-dock-id") || !strings.Contains(responseStr, "CONFIRMED") {
		t.Fatalf("Expected SSE stream to contain published event data, got: %s", responseStr)
	}
}
