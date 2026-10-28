package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cargo-slots/internal/service"
	"cargo-slots/internal/storage"
)

func main() {
	log.Println("[INFO] Starting Cargo-Slots API service...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:password@localhost:5432/slotsdb?sslmode=disable"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// 1. Conexión a PostgreSQL (pgxpool)
	pgRepo, err := storage.NewPostgresPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("[FATAL] Could not connect to PostgreSQL: %v", err)
	}
	defer pgRepo.Close()
	log.Println("[INFO] Connected to PostgreSQL with connection pool")

	// 2. Conexión a Redis
	redisClient, err := storage.NewRedisClient(ctx, redisAddr, "", 0)
	if err != nil {
		log.Fatalf("[FATAL] Could not connect to Redis: %v", err)
	}
	defer redisClient.Close()
	log.Println("[INFO] Connected to Redis")

	// 3. Inicializar Locker y Servicio Core
	locker := storage.NewRedisLocker(redisClient)
	svcCfg := service.Config{
		LockTTL:   2000 * time.Millisecond,
		DBTimeout: 3000 * time.Millisecond,
	}

	svc, err := service.NewReservationService(pgRepo, locker, svcCfg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize ReservationService: %v", err)
	}
	_ = svc

	log.Println("[INFO] Cargo-Slots service initialized and ready to accept bookings")
	<-ctx.Done()
	log.Println("[INFO] Shutting down Cargo-Slots gracefully...")
}
