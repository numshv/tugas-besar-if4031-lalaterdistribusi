// P5.5: when load exceeds capacity, Tim Lapangan is still served while Media is refused
// with a controlled 429. Run: k6 run -e MEDIA_ID=.. -e MEDIA_SECRET=.. -e FIELD_ID=.. -e FIELD_SECRET=.. p5-shedding.js
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { hazards } from './common.js';

const DURATION = __ENV.DURATION || '40s';
const served = new Counter('served');
const shed = new Counter('shed');

export const options = {
  scenarios: {
    media: { executor: 'constant-vus', vus: Number(__ENV.MEDIA_VUS || 300), duration: DURATION, exec: 'media' },
    field: { executor: 'constant-vus', vus: Number(__ENV.FIELD_VUS || 20), duration: DURATION, exec: 'field' },
  },
  thresholds: {
    'checks{role:field}': ['rate>0.99'],
    'http_req_duration{role:field}': ['p(95)<1000'],
  },
};

function call(role) {
  const res = hazards(role, 'SEISMIC');
  check(res, { 'served or shed': (r) => r.status === 200 || (role === 'media' && r.status === 429) }, { role });
  (res.status === 429 ? shed : served).add(1, { role });
}

export function media() { call('media'); }
export function field() { call('field'); }
