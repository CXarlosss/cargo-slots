# ADR 001: Database-Level Constraints for Double-Booking Prevention

## Status
**Accepted**

## Date
2026-10-28

## Context and Problem Statement
In a port/logistics cargo slot reservation system, multiple freight forwarders (*transitarios*) and automated dispatch agents compete for loading docks (*muelles de carga*). Under peak operations, hundreds of requests attempt to claim the exact same dock (e.g., `M-01`) at the exact same millisecond.

A naive pattern of `SELECT ... WHERE dock_id = ... AND time_overlap` followed by `INSERT` in application code (Go/Redis) introduces race conditions (Time-of-Check to Time-of-Use / TOCTOU). Concurrent transactions running concurrently will read the dock as available and both execute an `INSERT`, creating a critical double-booking bug.

### System Invariants
1. **Physical Dock Exclusivity**: A physical dock cannot have two confirmed reservations with overlapping time ranges under any circumstance.
2. **Monotonic Slot State Transition**: State moves strictly in one direction during transaction lifecycle: `PENDING` (transient in Redis) -> `CONFIRMED` (durable in DB) or `EXPIRED`.
3. **Lease/Timeout Hierarchy**: The TTL of any transient lock in Redis must **never** exceed the transaction timeout in PostgreSQL ($TTL_{Redis} \le Timeout_{PG}$).

### Target SLOs
- **p99 Reservation Latency**: $< 50\text{ ms}$ under 1,000 TPS.
- **Overbooking Rate**: Strictly $0\%$ under concurrent stress testing.

---

## Considered Alternatives

### 1. Application-Level Locking (Go Mutex / Distributed Redis Locks)
- **Mechanism**: Use Redis `SET NX EX` (Redlock or single-instance distributed lock) or Go sync primitives.
- **Drawbacks**: 
  - Application nodes can crash, experience GC pauses, or suffer network partitions between lock acquisition and database commit.
  - If a Redis key expires while the Postgres transaction is still committing (TTL drift), another worker can acquire the lock and write an overlapping slot.
  - Application bugs or direct DB access bypass the guard completely.

### 2. Pessimistic Row Locking (`SELECT FOR UPDATE`)
- **Mechanism**: Lock a parent row (e.g. `docks` table) or granular pre-split slot rows before inserting.
- **Drawbacks**:
  - Locking the dock row serializes **all** reservations for that dock across different days and hours, creating extreme lock convoying and reducing throughput far below the 1,000 TPS target.
  - Pre-split discrete slot rows eliminate flexibility for dynamic, variable-length appointment windows.

### 3. Serializable Transaction Isolation Level (`SERIALIZABLE`)
- **Mechanism**: Execute transactions with `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` and rely on predicate locks (SIREAD).
- **Drawbacks**:
  - PostgreSQL will abort conflicting transactions with `SQLSTATE 40001` (serialization failure). Under high concurrency, retry storms drastically degrade throughput and inflate p99 latencies well beyond 50ms.
  - Demands complex client-side retry logic and backoff jitter.

### 4. PostgreSQL Exclusion Constraint (`EXCLUDE USING gist`) with `TSTZRANGE` and `btree_gist`
- **Mechanism**: Use PostgreSQL native `TSTZRANGE` data type representing `[start, end)` continuous timestamps and delegate exclusivity to the GiST index using the overlap operator `&&` and equality `=` on `dock_id`.
- **Constraint Definition**:
  ```sql
  CONSTRAINT prevent_double_booking EXCLUDE USING gist (
      dock_id WITH =,
      time_range WITH &&
  ) WHERE (status = 'CONFIRMED');
  ```

---

## Decision
We delegate the mathematical guarantee of non-overlapping bookings directly to PostgreSQL at the storage engine level using `EXCLUDE USING gist` with the `btree_gist` extension, combined with `READ COMMITTED` transaction isolation.

Redis will serve as a fast transient buffer for `PENDING` reservations to absorb read-heavy availability queries and short reservation holds, but the ultimate source of truth and invariant enforcer is the database index engine.

---

## Consequences

### Positive
- **Mathematical Invariant Guarantee**: Impossible to create overlapping confirmed reservations regardless of concurrency, goroutine count, application bugs, or Redis failure.
- **High Concurrency**: Transactions for non-overlapping windows on the same dock execute concurrently without blocking.
- **Zero Lock Convoy**: No table-level or row-level serialization bottlenecks on the `docks` table.
- **Fail-Fast Error Handling**: Any attempt to insert an overlapping interval fails immediately with PostgreSQL error `23P01 (exclusion_violation)`, which can be mapped directly to HTTP `409 Conflict` or domain-specific conflict errors.

### Negative / Trade-offs
- GiST indexes have slightly higher write overhead and index size compared to standard B-Tree indexes.
- Requires enabling the `btree_gist` extension in PostgreSQL.
- Requires application drivers to map native range types (e.g. using `pgx` native support for PostgreSQL ranges).
