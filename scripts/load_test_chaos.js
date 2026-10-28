import http from 'k6/http';
import { Counter } from 'k6/metrics';

export const status201 = new Counter('status_201_success');
export const status423 = new Counter('status_423_locked');
export const status409 = new Counter('status_409_conflict');
export const status503 = new Counter('status_503_unavailable');
export const statusOther = new Counter('status_other_error');

export const options = {
  scenarios: {
    continuous_traffic: {
      executor: 'constant-arrival-rate',
      rate: 20, // 20 peticiones por segundo de forma continua
      timeUnit: '1s',
      duration: '12s',
      preAllocatedVUs: 15,
      maxVUs: 40,
    },
  },
};

export default function () {
  // Seleccionamos slots de tiempo entre varias franjas para observar 201, 423 y 503 durante la caída
  const slotOffset = Math.floor(Math.random() * 5);
  const hour = 10 + (slotOffset * 2);
  const payload = JSON.stringify({
    dock_name: 'M-01',
    forwarder_id: `CHAOS-AGENT-${__VU}`,
    start_time: `2026-12-05T${String(hour).padStart(2, '0')}:00:00Z`,
    end_time: `2026-12-05T${String(hour + 2).padStart(2, '0')}:00:00Z`,
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
    timeout: '3s',
  };

  const res = http.post('http://localhost:8080/api/v1/reservations', payload, params);

  switch (res.status) {
    case 201:
      status201.add(1);
      break;
    case 423:
      status423.add(1);
      break;
    case 409:
      status409.add(1);
      break;
    case 503:
      status503.add(1);
      break;
    default:
      statusOther.add(1);
      break;
  }
}
