// Sudden traffic spike: 30 s of single creates at 300 req/s plus two
// 100-item batches per second, ~15,000 notifications spread over the three
// channels with a priority mix. Run with loadtest/run.sh.
import http from 'k6/http';
import { check } from 'k6';

const API = __ENV.API_URL || 'http://localhost:8080';
const DURATION = __ENV.DURATION || '30s';

export const options = {
  scenarios: {
    singles: {
      executor: 'constant-arrival-rate', exec: 'single',
      rate: 300, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 300,
    },
    batches: {
      executor: 'constant-arrival-rate', exec: 'batch',
      rate: 2, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 5, maxVUs: 50,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{scenario:singles}': ['p(95)<250'],
    'http_req_duration{scenario:batches}': ['p(95)<1000'],
  },
};

const recipients = { sms: '+905551234567', email: 'customer@example.com', push: 'device-token-123' };
const channels = Object.keys(recipients);

// 10% high, 60% normal, 30% low.
function priority() {
  const r = Math.random();
  return r < 0.1 ? 'high' : r < 0.7 ? 'normal' : 'low';
}

function notification() {
  const channel = channels[Math.floor(Math.random() * channels.length)];
  return {
    channel,
    recipient: recipients[channel],
    content: 'Flash sale: 50% off for the next hour',
    priority: priority(),
  };
}

const headers = { 'Content-Type': 'application/json' };

export function single() {
  const res = http.post(`${API}/api/v1/notifications`, JSON.stringify(notification()), { headers });
  check(res, { 'single accepted': (r) => r.status === 202 });
}

export function batch() {
  const items = Array.from({ length: 100 }, notification);
  const res = http.post(`${API}/api/v1/notifications/batch`, JSON.stringify({ notifications: items }), { headers });
  check(res, { 'batch accepted': (r) => r.status === 202 });
}
