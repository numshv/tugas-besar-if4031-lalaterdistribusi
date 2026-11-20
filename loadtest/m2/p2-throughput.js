// P2.7: saturating closed-loop load to compare throughput with 1 and 3 aggregator replicas.
// Run twice (kubectl scale deploy/aggregator --replicas=1, then 3) and compare http_reqs/s.
import { check } from 'k6';
import { hazards } from './common.js';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    saturate: { executor: 'constant-vus', vus: Number(__ENV.VUS || 400), duration: __ENV.DURATION || '60s' },
  },
};

export default function () {
  const res = hazards('internal', '');
  check(res, { '200': (r) => r.status === 200 });
}
