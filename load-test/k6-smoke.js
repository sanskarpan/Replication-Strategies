// k6 smoke test for the replication-strategies backend.
//
//   k6 run load-test/k6-smoke.js
//   BASE_URL=http://localhost:8080 WRITES=50 VUS=5 DURATION=30s k6 run load-test/k6-smoke.js
//
// It creates a leaderless cluster, performs N writes + reads, and checks 2xx.
import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const WRITES = parseInt(__ENV.WRITES || "20", 10);

const writeErrors = new Counter("replsim_write_errors");
const readErrors = new Counter("replsim_read_errors");

export const options = {
  vus: parseInt(__ENV.VUS || "1", 10),
  duration: __ENV.DURATION || "10s",
  thresholds: {
    // Fail the run if more than 1% of any checked request is non-2xx.
    checks: ["rate>0.99"],
    http_req_failed: ["rate<0.01"],
  },
};

const JSON_HEADERS = { headers: { "Content-Type": "application/json" } };

// setup() runs once: create a cluster the VUs will hammer.
export function setup() {
  const payload = JSON.stringify({
    strategy: "leaderless",
    node_count: 3,
    quorum_n: 3,
    quorum_w: 2,
    quorum_r: 2,
  });

  const res = http.post(`${BASE_URL}/api/v1/clusters`, payload, JSON_HEADERS);
  check(res, {
    "cluster created (201)": (r) => r.status === 201,
    "cluster has id": (r) => {
      try {
        return typeof r.json("id") === "string" && r.json("id").length > 0;
      } catch (_e) {
        return false;
      }
    },
  });

  const clusterID = res.json("id");
  if (!clusterID) {
    throw new Error(
      `failed to create cluster: status=${res.status} body=${res.body}`
    );
  }
  return { clusterID };
}

export default function (data) {
  const clusterID = data.clusterID;
  const clientID = `k6-vu-${__VU}`;

  for (let i = 0; i < WRITES; i++) {
    const key = `k6-key-${__VU}-${i}`;
    const value = `v-${__ITER}-${i}`;

    const writeRes = http.post(
      `${BASE_URL}/api/v1/clusters/${clusterID}/write`,
      JSON.stringify({ key: key, value: value, client_id: clientID }),
      JSON_HEADERS
    );
    const writeOK = check(writeRes, {
      "write 2xx": (r) => r.status >= 200 && r.status < 300,
    });
    if (!writeOK) {
      writeErrors.add(1);
    }

    const readRes = http.get(
      `${BASE_URL}/api/v1/clusters/${clusterID}/read?key=${encodeURIComponent(
        key
      )}&client_id=${encodeURIComponent(clientID)}`
    );
    const readOK = check(readRes, {
      "read 2xx": (r) => r.status >= 200 && r.status < 300,
    });
    if (!readOK) {
      readErrors.add(1);
    }
  }

  sleep(0.1);
}

// teardown() runs once: clean up the cluster we created.
export function teardown(data) {
  if (data && data.clusterID) {
    http.del(`${BASE_URL}/api/v1/clusters/${data.clusterID}`);
  }
}
