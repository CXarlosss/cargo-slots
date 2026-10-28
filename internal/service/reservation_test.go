package service_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cargo-slots/internal/domain"
	"cargo-slots/internal/service"
	"cargo-slots/internal/storage"
)

// SpyRepository envuelve un domain.ReservationRepository e instrumenta
// un contador atómico para verificar exactamente cuántas llamadas alcanzaron la base de datos.
type SpyRepository struct {
	domain.ReservationRepository
	insertCalls atomic.Int64
}

func (s *SpyRepository) InsertReservation(ctx context.Context, res *domain.Reservation) error {
	s.insertCalls.Add(1)
	// Simulamos la latencia de I/O en PostgreSQL (200ms) dentro del margen de LockTTL (2000ms)
	// para garantizar que la ráfaga concurrente de las 99 goroutines colisione contra el lock de Redis.
	time.Sleep(200 * time.Millisecond)
	return s.ReservationRepository.InsertReservation(ctx, res)
}

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

func getTestRedisAddr() string {
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

// TestBookSlot_100ConcurrentGoroutines valida el criterio central de aceptación:
// Lanzar 100 goroutines concurrentes llamando a BookSlot para el mismo muelle y rango exacto.
// Debe verificarse que EXACTAMENTE 1 petición entra a Postgres y las otras 99 son
// rechazadas rápidamente por Redis (sin tocar DB), ejecutándose con 0 condiciones de carrera.
func TestBookSlot_100ConcurrentGoroutines(t *testing.T) {
	ctx := context.Background()

	dbURL := getTestDatabaseURL()
	pgRepo, err := storage.NewPostgresPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to test postgres at %s: %v", dbURL, err)
	}
	defer pgRepo.Close()

	redisAddr := getTestRedisAddr()
	redisClient, err := storage.NewRedisClient(ctx, redisAddr, "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to test redis at %s: %v", redisAddr, err)
	}
	defer redisClient.Close()

	// Crear muelle exclusivo para este test
	dockName := fmt.Sprintf("DOCK-BURST-%d", time.Now().UnixNano())
	dock, err := pgRepo.CreateDock(ctx, dockName)
	if err != nil {
		t.Fatalf("Failed to create test dock: %v", err)
	}

	// Instrumentamos el repositorio con un Spy para auditar inserciones en DB
	spyRepo := &SpyRepository{ReservationRepository: pgRepo}
	locker := storage.NewRedisLocker(redisClient)

	cfg := service.Config{
		LockTTL:   2000 * time.Millisecond,
		DBTimeout: 3000 * time.Millisecond,
	}

	svc, err := service.NewReservationService(spyRepo, locker, locker, cfg)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	slotStart := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	slotEnd := time.Date(2026, 11, 1, 16, 0, 0, 0, time.UTC)

	// Aseguramos que no queden locks residuales en Redis para esta franja
	lockKey := domain.BuildSlotLockKey(dock.Name, slotStart, slotEnd)
	redisClient.Del(ctx, lockKey)

	const totalGoroutines = 100
	var (
		startBarrier    = make(chan struct{})
		wg              sync.WaitGroup
		successCount    atomic.Int64
		redisBusyCount  atomic.Int64
		dbErrorCount    atomic.Int64
		otherErrorCount atomic.Int64
	)

	wg.Add(totalGoroutines)

	for i := 0; i < totalGoroutines; i++ {
		forwarderID := fmt.Sprintf("FORWARDER-%03d", i+1)
		go func(fid string) {
			defer wg.Done()

			// Barrera de sincronización: todas las goroutines esperan aquí
			<-startBarrier

			_, bookErr := svc.BookSlot(context.Background(), service.BookSlotInput{
				DockID:      dock.ID,
				DockName:    dock.Name,
				ForwarderID: fid,
				StartTime:   slotStart,
				EndTime:     slotEnd,
			})

			switch {
			case bookErr == nil:
				successCount.Add(1)
			case errors.Is(bookErr, domain.ErrLockBusy):
				redisBusyCount.Add(1)
			case errors.Is(bookErr, domain.ErrDoubleBooking):
				dbErrorCount.Add(1)
			default:
				t.Logf("Unexpected error for %s: %v", fid, bookErr)
				otherErrorCount.Add(1)
			}
		}(forwarderID)
	}

	// Disparamos la estampida simultánea
	close(startBarrier)
	wg.Wait()

	t.Logf("--- RESULTADOS DEL TEST DE CONCURRENCIA (100 goroutines) ---")
	t.Logf("Total Goroutines lanzadas    : %d", totalGoroutines)
	t.Logf("Reservas exitosas            : %d", successCount.Load())
	t.Logf("Rechazos rápidos por Redis   : %d (ErrLockBusy)", redisBusyCount.Load())
	t.Logf("Llamadas reales a Postgres DB: %d (auditoría SpyRepo)", spyRepo.insertCalls.Load())
	t.Logf("Violaciones en Postgres (DB) : %d (ErrDoubleBooking)", dbErrorCount.Load())
	t.Logf("Otros errores                : %d", otherErrorCount.Load())

	// Verificación de invariantes:
	// 1. Exactamente 1 petición tiene éxito
	if successCount.Load() != 1 {
		t.Fatalf("Esperado 1 reserva exitosa, obtenido: %d", successCount.Load())
	}

	// 2. Exactamente 99 peticiones fueron frenadas por el Redis bouncer
	if redisBusyCount.Load() != 99 {
		t.Fatalf("Esperado 99 rechazos en Redis (ErrLockBusy), obtenido: %d", redisBusyCount.Load())
	}

	// 3. Exactamente 1 petición alcanzó el método de inserción en PostgreSQL
	if spyRepo.insertCalls.Load() != 1 {
		t.Fatalf("Esperado exactamente 1 llamada de inserción a Postgres, obtenido: %d", spyRepo.insertCalls.Load())
	}

	// 4. Cero errores colaterales
	if dbErrorCount.Load() != 0 {
		t.Fatalf("Esperado 0 errores de base de datos, obtenido: %d", dbErrorCount.Load())
	}
	if otherErrorCount.Load() != 0 {
		t.Fatalf("Esperado 0 errores no controlados, obtenido: %d", otherErrorCount.Load())
	}

	// 5. Verificación física en la base de datos PostgreSQL
	dbCount, err := pgRepo.CountReservations(ctx, dock.ID, slotStart, slotEnd)
	if err != nil {
		t.Fatalf("Error consultando conteo de reservas en base de datos: %v", err)
	}
	if dbCount != 1 {
		t.Fatalf("Esperado exactamente 1 registro confirmado en PostgreSQL, encontrado: %d", dbCount)
	}
}

// TestInvariant3_Enforcement verifica que el Invariante 3 se cumpla por diseño:
// "El TTL del bloqueo en Redis NUNCA debe ser mayor al timeout de la transacción en PostgreSQL"
func TestInvariant3_Enforcement(t *testing.T) {
	// Caso inválido: LockTTL (5000ms) > DBTimeout (2000ms)
	invalidCfg := service.Config{
		LockTTL:   5000 * time.Millisecond,
		DBTimeout: 2000 * time.Millisecond,
	}

	svc, err := service.NewReservationService(nil, nil, nil, invalidCfg)
	if err == nil {
		t.Fatalf("Esperado error por violación del Invariante 3, pero NewReservationService tuvo éxito")
	}
	if !errors.Is(err, domain.ErrInvariant3Violation) {
		t.Fatalf("Esperado ErrInvariant3Violation, obtenido: %v", err)
	}
	t.Logf("Invariante 3 validado correctamente: %v", err)

	// Caso válido: LockTTL (2000ms) <= DBTimeout (3000ms)
	validCfg := service.Config{
		LockTTL:   2000 * time.Millisecond,
		DBTimeout: 3000 * time.Millisecond,
	}
	svc, err = service.NewReservationService(nil, nil, nil, validCfg)
	if err != nil {
		t.Fatalf("Configuración válida falló inesperadamente: %v", err)
	}
	if svc == nil {
		t.Fatalf("El servicio no debería ser nil con configuración válida")
	}
}

// TestRedisLocker_SafeReleaseWithLua verifica que un proceso no pueda borrar un lock
// perteneciente a otro proceso o un lock expirado.
func TestRedisLocker_SafeReleaseWithLua(t *testing.T) {
	ctx := context.Background()
	redisAddr := getTestRedisAddr()
	client, err := storage.NewRedisClient(ctx, redisAddr, "", 0)
	if err != nil {
		t.Fatalf("Failed to connect to redis: %v", err)
	}
	defer client.Close()

	locker := storage.NewRedisLocker(client)
	testKey := fmt.Sprintf("test:lock:%d", time.Now().UnixNano())

	// Adquirir lock con proceso A
	tokenA, err := locker.Acquire(ctx, testKey, 3000)
	if err != nil {
		t.Fatalf("Failed to acquire lock for Process A: %v", err)
	}

	// Intento de liberación con token erróneo (Proceso B intentando robar/liberar lock ajeno)
	fakeToken := "invalid-token-12345"
	err = locker.Release(ctx, testKey, fakeToken)
	if err == nil {
		t.Fatalf("Se esperaba error al intentar liberar lock con token ajeno, pero no dio error")
	}
	if !errors.Is(err, domain.ErrLockNotOwned) {
		t.Fatalf("Esperado ErrLockNotOwned, obtenido: %v", err)
	}

	// Liberación legítima con token original
	err = locker.Release(ctx, testKey, tokenA)
	if err != nil {
		t.Fatalf("Error al liberar legítimamente con token correcto: %v", err)
	}

	// Segundo intento de liberación legítima tras haber sido ya eliminado (debe retornar ErrLockNotOwned)
	err = locker.Release(ctx, testKey, tokenA)
	if !errors.Is(err, domain.ErrLockNotOwned) {
		t.Fatalf("Esperado ErrLockNotOwned en lock ya liberado, obtenido: %v", err)
	}
}
