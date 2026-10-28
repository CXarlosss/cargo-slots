# CARGO-SLOTS

Sistema de reserva de muelles de carga (*cargo slots*) para transitarios y terminales logísticas portuarias de alta concurrencia.

## Invariantes del Sistema

1. **Exclusividad física del muelle**: Un muelle físico no puede tener dos reservas confirmadas con rangos de tiempo superpuestos bajo ninguna circunstancia.
2. **Flujo unidireccional de estados**: El estado de un slot fluye en una sola dirección durante una transacción: `PENDING` (Redis) -> `CONFIRMED` (PostgreSQL) o `EXPIRED`.
3. **Jerarquía de Timeouts**: El TTL del bloqueo en Redis **NUNCA** debe ser mayor al timeout de la transacción en PostgreSQL ($TTL_{Redis} \le Timeout_{PG}$).

## Objetivos Medibles (SLOs)

- **p99 Latencia de reserva**: $< 50\text{ ms}$ bajo carga sostenida de 1,000 TPS.
- **Sobreasignación**: $0\%$ bajo pruebas de estrés concurrentes.

---

## Arquitectura y Blindaje ACID

Para garantizar $0\%$ de sobreasignación bajo contención transaccional extrema (múltiples goroutines/transitarios intentando reservar el mismo muelle al mismo milisegundo), la exclusividad temporal está blindada a nivel de motor de base de datos en **PostgreSQL** mediante:

- Tipo nativo continuo `TSTZRANGE` con intervalos semiabiertos `[inicio, fin)`.
- Extensión `btree_gist` para soporte de operadores de igualdad `=` junto con operadores de superposición `&&` en índices GiST.
- Restricción de exclusión parcial:
  ```sql
  CONSTRAINT prevent_double_booking EXCLUDE USING gist (
      dock_id WITH =,
      time_range WITH &&
  ) WHERE (status = 'CONFIRMED');
  ```

Documentación completa en [ADR-001](docs/ADR-001-database-level-constraints.md).

---

## Puesta en Marcha

### Prerrequisitos
- Docker & Docker Compose
- Go 1.22+

### 1. Iniciar servicios
```bash
docker compose up -d
```

### 2. Verificar base de datos y extensión
```bash
docker compose exec db psql -U admin -d slotsdb -c "\dx"
```

### 3. Prueba de Invariante (Double-Booking Prevention)
Intentar insertar dos reservas confirmadas superpuestas genera el error de violación de exclusión (`SQLSTATE 23P01`):
```text
ERROR:  conflicting key value violates exclusion constraint "prevent_double_booking"
DETAIL:  Key (dock_id, time_range)=(...) conflicts with existing key (...)
```

---

## Capa API y Real-Time Streaming (SSE)

### Endpoints HTTP

- `POST /api/v1/reservations`: Creación atómica de reservas con protección por distributed lock y persistencia en PostgreSQL.
  - Retorna `201 Created` en reserva confirmada.
  - Retorna `423 Locked` si Redis lock está ocupado por una petición concurrente.
  - Retorna `409 Conflict` si PostgreSQL GiST detecta superposición temporal.
- `GET /api/v1/docks/stream`: Server-Sent Events (SSE) alimentado por Redis Pub/Sub (`docks.events`) con prevención de memory leaks por desconexión de clientes.
- `GET /health`: Healthcheck estricto con ping a PostgreSQL y Redis (200 OK / 503 Service Unavailable).

### Ejecución de Pruebas de Carga
```bash
k6 run scripts/load_test.js
```

