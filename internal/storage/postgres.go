package storage

import (
	"context"
	"errors"
	"fmt"
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

	err := r.pool.QueryRow(
		ctx,
		query,
		res.DockID,
		res.ForwarderID,
		res.StartTime.UTC(),
		res.EndTime.UTC(),
		statusStr,
	).Scan(&res.ID, &res.CreatedAt)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// SQLSTATE 23P01 = exclusion_violation
			if pgErr.Code == "23P01" {
				return domain.ErrDoubleBooking
			}
		}
		return fmt.Errorf("failed to insert reservation: %w", err)
	}

	res.Status = domain.ReservationStatus(statusStr)
	return nil
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
