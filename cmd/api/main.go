package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cargo-slots/internal/service"
	"cargo-slots/internal/storage"
)

func getDatabaseURL() string {
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
	// Fallback para entornos Windows/WSL2
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

func getRedisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	c, err := storage.NewRedisClient(ctx, "localhost:6379", "", 0)
	if err == nil {
		c.Close()
		return "localhost:6379"
	}
	cmd := exec.Command("wsl", "hostname", "-I")
	out, err := cmd.Output()
	if err == nil {
		fields := strings.Fields(string(out))
		if len(fields) > 0 {
			return fmt.Sprintf("%s:6379", fields[0])
		}
	}
	return "172.31.186.244:6379"
}

func main() {
	log.Println("[INFO] Starting Cargo-Slots API service...")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := getDatabaseURL()
	redisAddr := getRedisAddr()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := RunApp(ctx, dbURL, redisAddr, port); err != nil {
		log.Fatalf("[FATAL] Application terminated with error: %v", err)
	}
}

// RunApp inicializa todos los subsistemas, arranca el servidor HTTP y gestiona el ciclo de vida
// de Graceful Shutdown al cancelarse el contexto (por SIGINT/SIGTERM o context.CancelFunc).
func RunApp(ctx context.Context, dbURL, redisAddr, port string) error {
	// 1. Conexión a PostgreSQL (pgxpool)
	pgRepo, err := storage.NewPostgresPool(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("could not connect to PostgreSQL at %s: %w", dbURL, err)
	}
	defer pgRepo.Close()
	log.Println("[INFO] Connected to PostgreSQL with connection pool")

	// 2. Conexión a Redis
	redisClient, err := storage.NewRedisClient(ctx, redisAddr, "", 0)
	if err != nil {
		return fmt.Errorf("could not connect to Redis at %s: %w", redisAddr, err)
	}
	defer redisClient.Close()
	log.Println("[INFO] Connected to Redis")

	// 3. Inicializar Locker, EventPublisher y Servicio Core
	locker := storage.NewRedisLocker(redisClient)
	svcCfg := service.Config{
		LockTTL:   2000 * time.Millisecond,
		DBTimeout: 3000 * time.Millisecond,
	}

	svc, err := service.NewReservationService(pgRepo, locker, locker, svcCfg)
	if err != nil {
		return fmt.Errorf("failed to initialize ReservationService: %w", err)
	}

	// 4. Configurar Enrutador HTTP y Handlers
	mux := http.NewServeMux()
	handler := NewHandler(svc, pgRepo, redisClient)
	handler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	serverErrCh := make(chan error, 1)
	go func() {
		log.Printf("[INFO] Cargo-Slots HTTP Server listening on port :%s", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrCh <- err
		}
	}()

	select {
	case err := <-serverErrCh:
		return fmt.Errorf("http server failure: %w", err)
	case <-ctx.Done():
		log.Println("[INFO] Graceful shutdown iniciado (señal de terminación recibida)...")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[ERROR] Server forced to shutdown: %v", err)
	} else {
		log.Println("[INFO] Servidor HTTP detenido correctamente sin interrumpir peticiones en vuelo")
	}

	// Cierre ordenado de los pools de infraestructura
	pgRepo.Close()
	_ = redisClient.Close()
	log.Println("[INFO] Conexiones cerradas a PostgreSQL y Redis.")
	log.Println("[INFO] Graceful shutdown completado exitosamente.")
	return nil
}
