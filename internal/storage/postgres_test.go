package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"cargo-slots/internal/domain"
	"cargo-slots/internal/storage"
)

func getTestDatabaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	pool, err := storage.NewPostgresPool(ctx, "postgres://admin:password@localhost:5432/slotsdb?sslmode=disable")
	if err == nil {
		pool.Close()
		return "postgres://admin:password@localhost:5432/slotsdb?sslmode=disable"
	}
	cmd := exec.Command("wsl", "hostname", "-I")
	out, err := cmd.Output()
	if err == nil {
		fields := strings.Fields(string(out))
		if len(fields) > 0 {
			return fmt.Sprintf("postgres://admin:password@%s:5432/slotsdb?sslmode=disable", fields[0])
		}
	}
	return "postgres://admin:password@172.31.186.244:5432/slotsdb?sslmode=disable"
}

// TestInsertReservation_NoRetryOnExclusionViolation comprueba que el error 23P01
// de PostgreSQL jamás se reintente, retornando inmediatamente domain.ErrDoubleBooking.
func TestInsertReservation_NoRetryOnExclusionViolation(t *testing.T) {
	ctx := context.Background()
	dbURL := getTestDatabaseURL()
	pgRepo, err := storage.NewPostgresPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to db: %v", err)
	}
	defer pgRepo.Close()

	dockName := fmt.Sprintf("DOCK-RETRY-%d", time.Now().UnixNano())
	dock, err := pgRepo.CreateDock(ctx, dockName)
	if err != nil {
		t.Fatalf("Failed to create dock: %v", err)
	}

	slotStart := time.Now().Add(50 * time.Hour).Truncate(time.Hour).UTC()
	slotEnd := slotStart.Add(2 * time.Hour)

	res1 := &domain.Reservation{
		DockID:      dock.ID,
		ForwarderID: "FORWARDER-1",
		StartTime:   slotStart,
		EndTime:     slotEnd,
		Status:      domain.StatusConfirmed,
	}

	if err := pgRepo.InsertReservation(ctx, res1); err != nil {
		t.Fatalf("First reservation failed: %v", err)
	}

	// Intento de inserción idéntica (violación de exclusión 23P01)
	res2 := &domain.Reservation{
		DockID:      dock.ID,
		ForwarderID: "FORWARDER-2",
		StartTime:   slotStart,
		EndTime:     slotEnd,
		Status:      domain.StatusConfirmed,
	}

	start := time.Now()
	err = pgRepo.InsertReservation(ctx, res2)
	duration := time.Since(start)

	if !errors.Is(err, domain.ErrDoubleBooking) {
		t.Fatalf("Expected ErrDoubleBooking, got: %v", err)
	}

	// Si hubiera reintentado con backoff (100ms + 200ms + 400ms = 700ms),
	// la duración habría sido > 700ms. Verificamos que abortó inmediatamente (< 150ms).
	if duration > 200*time.Millisecond {
		t.Fatalf("Exclusion violation must fail fast without retries, took: %v", duration)
	}
	t.Logf("Exclusion violation 23P01 failed fast in %v without retries (OK)", duration)
}
