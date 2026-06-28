import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const errorRate = new Rate('error_rate');
const approvedTransactions = new Counter('approved_transactions');
const rejectedTransactions = new Counter('rejected_transactions');

export const options = {
  stages: [
    { duration: '30s', target: 50  },
    { duration: '60s', target: 100 },
    { duration: '60s', target: 200 },
    { duration: '30s', target: 0   },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],
    error_rate: ['rate<0.05'],
  },
};

const BASE_URL = 'http://localhost:8080';

export function setup() {
  ['ACC001','ACC002','ACC003','ACC004','ACC005'].forEach(id => {
    http.post(`${BASE_URL}/api/v1/accounts/${id}/seed?balance=100000`);
  });
}

export default function() {
  const accounts = ['ACC001','ACC002','ACC003','ACC004','ACC005'];
  const accountId = accounts[Math.floor(Math.random() * accounts.length)];

  if (Math.random() < 0.7) {
    const res = http.post(`${BASE_URL}/api/v1/transactions`,
      JSON.stringify({ accountId, amount: Math.floor(Math.random() * 100) + 1, type: 'DEBIT' }),
      { headers: { 'Content-Type': 'application/json' } }
    );
    const ok = check(res, { 'status 200': r => r.status === 200 });
    errorRate.add(!ok);
    if (ok) {
      const body = JSON.parse(res.body);
      if (body.status === 'APPROVED') approvedTransactions.add(1);
      else rejectedTransactions.add(1);
    }
  } else {
    const res = http.get(`${BASE_URL}/api/v1/accounts/${accountId}/balance`);
    errorRate.add(res.status !== 200);
  }
  sleep(0.1);
}
