package domain

import "errors"

var (
	// ErrLockBusy se produce cuando Redis ya tiene un lock activo para este muelle y rango de tiempo (Invariante 1 & Fast Bouncer).
	ErrLockBusy = errors.New("slot lock is busy: slot already locked by another process")

	// ErrLockNotOwned se produce cuando el script Lua intenta liberar el lock pero el token no coincide o expiró.
	ErrLockNotOwned = errors.New("cannot release lock: lock expired or owned by another process")

	// ErrDoubleBooking se produce si PostgreSQL rechaza la reserva por superposición temporal (SQLSTATE 23P01).
	ErrDoubleBooking = errors.New("dock already has a confirmed overlapping reservation")

	// ErrDockNotFound se produce si el muelle no existe en la base de datos.
	ErrDockNotFound = errors.New("dock not found")

	// ErrInvalidTimeRange se produce si la hora de inicio es igual o posterior a la hora de fin.
	ErrInvalidTimeRange = errors.New("invalid time range: end time must be after start time")

	// ErrInvariant3Violation se produce si el TTL de Redis supera el timeout transaccional de PostgreSQL.
	ErrInvariant3Violation = errors.New("invariant 3 violated: Redis lock TTL must not exceed DB transaction timeout")
)
