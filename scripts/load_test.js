import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

export const status201 = new Counter('http_status_201');
export const status423 = new Counter('http_status_423');
export const status409 = new Counter('http_status_409');
export const statusOther = new Counter('http_status_other');

export const options = {
  scenarios: {
    // Escenario 1: Estampida concurrente masiva (50 VUs bombardeando el mismo slot)
    stampede: {
      executor: 'shared-iterations',
      vus: 50,
      iterations: 500,
      maxDuration: '10s',
      exec: 'stampede',
    },
    // Escenario 2: Peticiones tardías tras la liberación del lock para verificar el blindaje 409 en DB
    postLockConflict: {
      executor: 'per-vu-iterations',
      vus: 10,
      iterations: 5,
      startTime: '3s', // Inicia tras la liberación del lock de Redis
      maxDuration: '10s',
      exec: 'conflictPhase',
    },
  },
};

const payload = JSON.stringify({
  dock_name: 'M-01',
  forwarder_id: 'FORWARDER-LOAD-TEST-STAMPEDE',
  start_time: '2026-11-15T14:00:00Z',
  end_time: '2026-11-15T16:00:00Z',
});

const params = {
  headers: {
    'Content-Type': 'application/json',
  },
};

export function stampede() {
  const res = http.post('http://localhost:8080/api/v1/reservations', payload, params);

  if (res.status === 201) {
    status201.add(1);
  } else if (res.status === 423) {
    status423.add(1);
  } else if (res.status === 409) {
    status409.add(1);
  } else {
    statusOther.add(1);
  }

  check(res, {
    'valid status (201, 423, or 409)': (r) => [201, 423, 409].includes(r.status),
  });
}

export function conflictPhase() {
  const res = http.post('http://localhost:8080/api/v1/reservations', payload, params);

  if (res.status === 201) {
    status201.add(1);
  } else if (res.status === 423) {
    status423.add(1);
  } else if (res.status === 409) {
    status409.add(1);
  } else {
    statusOther.add(1);
  }

  check(res, {
    'conflict status 409': (r) => r.status === 409,
  });
}
