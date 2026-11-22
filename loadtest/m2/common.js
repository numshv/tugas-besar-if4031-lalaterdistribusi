import http from 'k6/http';

export const BASE = __ENV.BASE_URL || 'http://localhost:8090';

// Access tokens live 60 s (ACCESS_TOKEN_TTL_SECONDS), so each VU logs in again before expiry.
const REFRESH_MS = Number.parseInt(__ENV.TOKEN_REFRESH_MS || '45000', 10);
const tokens = {};

export function token(role) {
  const cached = tokens[role];
  if (cached && Date.now() - cached.at < REFRESH_MS) {
    return cached.value;
  }
  const user = __ENV[`${role.toUpperCase()}_ID`];
  const pass = __ENV[`${role.toUpperCase()}_SECRET`];
  const res = http.post(`${BASE}/login`, JSON.stringify({ username: user, password: pass }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'login' },
    responseType: 'text', // the token must be read even when the test discards bodies
  });
  if (res.status !== 200) {
    return '';
  }
  tokens[role] = { value: res.json('access_token'), at: Date.now() };
  return tokens[role].value;
}

export function hazards(role, type) {
  const query = type ? `?type=${type}&limit=20` : '?limit=20';
  return http.get(`${BASE}/hazards${query}`, {
    headers: { Authorization: `Bearer ${token(role)}` },
    tags: { name: 'hazards', role },
  });
}

// 429 is a controlled refusal (load shedding), so it does not count as a failed request.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 429));
