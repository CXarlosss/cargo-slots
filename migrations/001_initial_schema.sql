-- Migration 001: Initial schema with exclusion constraint for double-booking prevention

-- Necesario para índices GiST con tipos de datos estándar
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE docks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dock_id UUID REFERENCES docks(id) ON DELETE CASCADE,
    forwarder_id VARCHAR(100) NOT NULL, -- Identificador del transitario/agencia
    time_range TSTZRANGE NOT NULL,      -- Rango de tiempo [inicio, fin)
    status VARCHAR(20) DEFAULT 'CONFIRMED',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    -- ESTO ES EL NÚCLEO DEL PROYECTO:
    -- Impide (EXCLUDE) que el mismo muelle (=) tenga tiempos que se superpongan (&&)
    -- solo si la reserva está confirmada.
    CONSTRAINT prevent_double_booking EXCLUDE USING gist (
        dock_id WITH =, 
        time_range WITH &&
    ) WHERE (status = 'CONFIRMED')
);
