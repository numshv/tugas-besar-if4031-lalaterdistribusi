import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const MODE = __ENV.SCENARIO || 'load';
const CLIENT = __ENV.CLIENT_API_URL;
const AUTH = __ENV.AUTH_URL;
const USER = __ENV.LT_USER;
const PASS = __ENV.LT_PASS;
const DURATION = __ENV.DURATION || '60s';
const SEISMIC_VUS = Number.parseInt(__ENV.SEISMIC_VUS || '10', 10);
const VOLCANIC_VUS = Number.parseInt(__ENV.VOLCANIC_VUS || '10', 10);

const seismicLatency = new Trend('seismic_latency', true);
const volcanicLatency = new Trend('volcanic_latency', true);
const apiLatency = new Trend('api_latency', true);
const unexpectedStatus = new Rate('unexpected_status');
const rejected429 = new Counter('rejected_429');
const apiRequests = new Counter('api_requests');

const scenarios = {
  seismic: {
    executor: 'constant-vus',
    vus: SEISMIC_VUS,
    duration: DURATION,
    gracefulStop: '10s',
    exec: 'seismic',
  },
};
if (VOLCANIC_VUS > 0) {
  scenarios.volcanic = {
    executor: 'constant-vus',
    vus: VOLCANIC_VUS,
    duration: DURATION,
    gracefulStop: '10s',
    exec: 'volcanic',
  };
}

const thresholds = { unexpected_status: ['rate<0.01'] };
if (MODE !== 'load') {
  thresholds.seismic_latency = ['p(95)<300'];
}

export const options = {
  scenarios,
  thresholds,
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

let accessToken = '';
let refreshBefore = 0;

function login() {
  const response = http.post(
    `${AUTH}/login`,
    JSON.stringify({ username: USER, password: PASS }),
    { headers: { 'Content-Type': 'application/json' }, tags: { kind: 'auth' } },
  );
  accessToken = response.status === 200 ? response.json('access_token') : '';
  refreshBefore = Date.now() + 40000;
}

function hit(type, trend) {
  if (!accessToken || Date.now() >= refreshBefore) login();

  const response = http.get(`${CLIENT}/hazards?type=${type}&limit=20`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    tags: { kind: type.toLowerCase() },
    responseCallback: http.expectedStatuses(200, 429),
    timeout: '10s',
  });

  apiRequests.add(1);
  apiLatency.add(response.timings.duration);
  trend.add(response.timings.duration);
  if (response.status === 429) rejected429.add(1);
  unexpectedStatus.add(response.status !== 200 && response.status !== 429);
  check(response, {
    'status is 200 or controlled 429': (r) => r.status === 200 || r.status === 429,
    'successful response has a body': (r) => r.status !== 200 || r.body.length > 2,
  });
}

export function seismic() {
  hit('SEISMIC', seismicLatency);
}

export function volcanic() {
  hit('VOLCANIC', volcanicLatency);
}

function value(data, name, stat) {
  const metric = data.metrics[name];
  if (!metric || metric.values[stat] === undefined) return 'NA';
  return metric.values[stat];
}

function countOrZero(data, name) {
  const metric = data.metrics[name];
  return metric ? value(data, name, 'count') : 0;
}

export function handleSummary(data) {
  const metrics = [
    `mode=${MODE}`,
    `test_run_duration_ms=${Math.round(data.state.testRunDurationMs)}`,
    `seismic_vus=${SEISMIC_VUS}`,
    `volcanic_vus=${VOLCANIC_VUS}`,
    `vus_max=${value(data, 'vus_max', 'max')}`,
    `api_requests=${value(data, 'api_requests', 'count')}`,
    `api_p95_ms=${value(data, 'api_latency', 'p(95)')}`,
    `seismic_p95_ms=${value(data, 'seismic_latency', 'p(95)')}`,
    `seismic_max_ms=${value(data, 'seismic_latency', 'max')}`,
    `unexpected_rate=${value(data, 'unexpected_status', 'rate')}`,
    `rejected_429=${countOrZero(data, 'rejected_429')}`,
  ];
  return { stdout: `P2METRIC ${metrics.join('\nP2METRIC ')}\n` };
}
