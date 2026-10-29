package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"cargo-slots/internal/domain"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresPool crea e inicializa un pool de conexiones pgx hacia PostgreSQL.
func NewPostgresPool(ctx context.Context, connString string) (*PostgresRepository, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database config: %w", err)
	}

	cfg.MaxConns = 50
	cfg.MinConns = 5
	cfg.MaxConnLifetime = 1 * time.Hour
	cfg.MaxConnIdleTime = 15 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresRepository{pool: pool}, nil
}

// Close cierra el pool de conexiones.
func (r *PostgresRepository) Close() {
	if r.pool != nil {
		r.pool.Close()
	}
}

// CreateDock inserta un nuevo muelle físico.
func (r *PostgresRepository) CreateDock(ctx context.Context, name string) (*domain.Dock, error) {
	query := `INSERT INTO docks (name) VALUES ($1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id, name`
	dock := &domain.Dock{}
	err := r.pool.QueryRow(ctx, query, name).Scan(&dock.ID, &dock.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create or get dock: %w", err)
	}
	return dock, nil
}

// GetDockByName obtiene un muelle por su nombre único.
func (r *PostgresRepository) GetDockByName(ctx context.Context, name string) (*domain.Dock, error) {
	query := `SELECT id, name FROM docks WHERE name = $1`
	dock := &domain.Dock{}
	err := r.pool.QueryRow(ctx, query, name).Scan(&dock.ID, &dock.Name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrDockNotFound
		}
		return nil, fmt.Errorf("query dock by name failed: %w", err)
	}
	return dock, nil
}

// GetDockByID obtiene un muelle por su UUID.
func (r *PostgresRepository) GetDockByID(ctx context.Context, id uuid.UUID) (*domain.Dock, error) {
	query := `SELECT id, name FROM docks WHERE id = $1`
	dock := &domain.Dock{}
	err := r.pool.QueryRow(ctx, query, id).Scan(&dock.ID, &dock.Name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrDockNotFound
		}
		return nil, fmt.Errorf("query dock by id failed: %w", err)
	}
	return dock, nil
}

// InsertReservation inserta una nueva reserva en PostgreSQL.
// Si el rango se superpone con otra reserva confirmada en el mismo muelle,
// PostgreSQL arrojará el error 23P01 (exclusion_violation), el cual se mapea a domain.ErrDoubleBooking.
func (r *PostgresRepository) InsertReservation(ctx context.Context, res *domain.Reservation) error {
	if res.EndTime.Before(res.StartTime) || res.EndTime.Equal(res.StartTime) {
		return domain.ErrInvalidTimeRange
	}

	query := `
		INSERT INTO reservations (dock_id, forwarder_id, time_range, status)
		VALUES ($1, $2, tstzrange($3, $4, '[)'), $5)
		RETURNING id, created_at
	`

	statusStr := string(res.Status)
	if statusStr == "" {
		statusStr = string(domain.StatusConfirmed)
	}

	backoffs := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	const maxAttempts = 3

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := r.pool.QueryRow(
			ctx,
			query,
			res.DockID,
			res.ForwarderID,
			res.StartTime.UTC(),
			res.EndTime.UTC(),
			statusStr,
		).Scan(&res.ID, &res.CreatedAt)

		if err == nil {
			res.Status = domain.ReservationStatus(statusStr)
			return nil
		}

		// Invariante y Regla de Dominio:
		// SQLSTATE 23P01 (exclusion_violation) NUNCA se reintenta, es una verdad absoluta del modelo de negocio.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == "23P01" {
				return domain.ErrDoubleBooking
			}
			// Errores de restricción o sintaxis no son transitorios
			if strings.HasPrefix(pgErr.Code, "23") || strings.HasPrefix(pgErr.Code, "42") {
				return fmt.Errorf("postgres domain constraint error [%s]: %w", pgErr.Code, err)
			}
		}

		// Si el contexto ya fue cancelado, abortar inmediatamente
		if ctx.Err() != nil {
			return fmt.Errorf("context aborted during database operation: %w", ctx.Err())
		}

		lastErr = err
		if attempt < maxAttempts {
			backoff := backoffs[attempt-1]
			log.Printf("[WARN] Error transitorio en PostgreSQL (intento %d/%d): %v. Reintentando en %v...", attempt, maxAttempts, err, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry backoff: %w", ctx.Err())
			}
		}
	}

	return fmt.Errorf("failed to insert reservation after %d attempts: %w", maxAttempts, lastErr)
}

// CountReservations cuenta las reservas existentes para un muelle y rango de tiempo en PostgreSQL.
func (r *PostgresRepository) CountReservations(ctx context.Context, dockID uuid.UUID, startTime, endTime time.Time) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM reservations
		WHERE dock_id = $1
		  AND time_range && tstzrange($2, $3, '[)')
		  AND status = 'CONFIRMED'
	`
	var count int
	err := r.pool.QueryRow(ctx, query, dockID, startTime.UTC(), endTime.UTC()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count reservations: %w", err)
	}
	return count, nil
}

// Ping comprueba la conectividad con el pool de PostgreSQL.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ListDocks obtiene todos los muelles ordenados por nombre.
func (r *PostgresRepository) ListDocks(ctx context.Context) ([]domain.Dock, error) {
	query := `SELECT id, name FROM docks ORDER BY name ASC`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list docks: %w", err)
	}
	defer rows.Close()

	var docks []domain.Dock
	for rows.Next() {
		var d domain.Dock
		if err := rows.Scan(&d.ID, &d.Name); err != nil {
			return nil, err
		}
		docks = append(docks, d)
	}
	return docks, rows.Err()
}

// ListReservations obtiene las reservas registradas ordenadas por inicio de franja horaria.
func (r *PostgresRepository) ListReservations(ctx context.Context) ([]domain.Reservation, error) {
	query := `
		SELECT id, dock_id, forwarder_id, lower(time_range), upper(time_range), status, created_at
		FROM reservations
		ORDER BY lower(time_range) ASC
		LIMIT 150
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list reservations: %w", err)
	}
	defer rows.Close()

	var res []domain.Reservation
	for rows.Next() {
		var rItem domain.Reservation
		var statusStr string
		if err := rows.Scan(&rItem.ID, &rItem.DockID, &rItem.ForwarderID, &rItem.StartTime, &rItem.EndTime, &statusStr, &rItem.CreatedAt); err != nil {
			return nil, err
		}
		rItem.Status = domain.ReservationStatus(statusStr)
		res = append(res, rItem)
	}
	return res, rows.Err()
}

// SeedDefaultDocks asegura la existencia de los muelles estándar de M-01 a M-06 si no existen.
func (r *PostgresRepository) SeedDefaultDocks(ctx context.Context) error {
	defaultNames := []string{"M-01", "M-02", "M-03", "M-04", "M-05", "M-06"}
	for _, name := range defaultNames {
		query := `INSERT INTO docks (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`
		if _, err := r.pool.Exec(ctx, query, name); err != nil {
			return fmt.Errorf("failed to seed dock %s: %w", name, err)
		}
	}
	return nil
}
