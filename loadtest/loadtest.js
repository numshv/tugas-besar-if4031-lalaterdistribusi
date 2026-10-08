// Mode via env SCENARIO:
//  - latency (kriteria 1): 10 VU seismik + 10 VU vulkanik, threshold p95 seismik < 300 ms
//  - load    (kriteria 2): 50 VU seismik + 20 VU vulkanik (70 paralel), tanpa threshold latensi,
//                          hanya error rate < 1% di luar 429
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const CLIENT_API = __ENV.CLIENT_API_URL || 'http://localhost:8080';
const AUTH = __ENV.AUTH_URL || 'http://localhost:8084';
const DURATION = __ENV.DURATION || '60s';
const MODE = __ENV.SCENARIO || 'load';

// 429 dianggap respons terkontrol: tidak dihitung gagal, tapi tetap dicatat
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 429));
const rejected429 = new Counter('rejected_429');

const VUS = MODE === 'latency'
  ? { seismic: 10, volcanic: 10 }
  : { seismic: 50, volcanic: 20 };

const thresholds = {
  http_req_failed: ['rate<0.01'],
};
if (MODE === 'latency') {
  thresholds['http_req_duration{kind:seismic}'] = ['p(95)<300'];
}

export const options = {
  scenarios: {
    seismic: {
      executor: 'constant-vus', vus: VUS.seismic, duration: DURATION, exec: 'seismic',
    },
    volcanic: {
      executor: 'constant-vus', vus: VUS.volcanic, duration: DURATION, exec: 'volcanic',
    },
  },
  thresholds,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// Login sekali. Jalankan auth-service dengan ACCESS_TOKEN_TTL_SECONDS >= 600
// supaya token tidak kedaluwarsa di tengah test (TTL 60 dipakai untuk demo kriteria 3).
export function setup() {
  const r = http.post(
    `${AUTH}/login`,
    JSON.stringify({ username: __ENV.LT_USER, password: __ENV.LT_PASS }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (r.status !== 200) {
    throw new Error(`login gagal: status ${r.status} body ${r.body}`);
  }
  const token = r.json('access_token');
  if (!token) {
    throw new Error('access_token tidak ada di respons login');
  }
  return { token };
}

function hit(data, type, kind) {
  const r = http.get(`${CLIENT_API}/hazards?type=${type}`, {
    headers: { Authorization: `Bearer ${data.token}` },
    tags: { kind },
  });
  if (r.status === 429) rejected429.add(1);
  if (r.status !== 200 && r.status !== 429) {
    console.error(`unexpected status=${r.status} body=${(r.body || '').slice(0, 200)}`);
  }
  check(r, {
    'status 200 atau 429': (x) => x.status === 200 || x.status === 429,
    'body tidak kosong saat 200': (x) => x.status !== 200 || x.body.length > 20,
  });
}

export function seismic(data) { hit(data, 'SEISMIC', 'seismic'); }
export function volcanic(data) { hit(data, 'VOLCANIC', 'volcanic'); }