// P2.6 and P2.8: sustained load of >= 10,000 VUs for >= 120 s through the single entry point,
// while loadtest/m2/p2-chaos.sh scales the aggregator 3 -> 5 -> 2 and rolls out a new version.
// Error rate outside controlled refusals (429) must stay below 1%.
import { check, sleep } from 'k6';
import { hazards } from './common.js';

const PEAK = Number(__ENV.PEAK_VUS || 10000);
const HOLD = __ENV.HOLD || '120s';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    sustained: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP || '60s', target: PEAK },
        { duration: HOLD, target: PEAK },
        { duration: '15s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:hazards}': ['p(95)<2000'],
  },
};

const ROLES = ['field', 'field', 'internal', 'media'];
const TYPES = ['SEISMIC', 'VOLCANIC', 'COASTAL', ''];

export default function () {
  const role = ROLES[__VU % ROLES.length];
  const res = hazards(role, TYPES[__ITER % TYPES.length]);
  check(res, { 'ok or controlled refusal': (r) => r.status === 200 || r.status === 429 });
  sleep(1);
}
