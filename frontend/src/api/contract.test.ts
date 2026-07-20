import { test, expect } from "bun:test";
import { readFileSync } from "fs";
import { join } from "path";
import yaml from "js-yaml";

// Verify the OpenAPI YAML contains the schemas and operations that generated.ts
// depends on. This catches accidental spec renames before they silently break
// the generated types.

const specPath = join(import.meta.dir, "../../../gateway/openapi.yaml");
const spec = yaml.load(readFileSync(specPath, "utf8")) as Record<string, unknown>;
const schemas = ((spec.components as Record<string, unknown>)?.schemas ?? {}) as Record<string, unknown>;
const paths = (spec.paths ?? {}) as Record<string, unknown>;

const REQUIRED_SCHEMAS = [
  "ClusterState",
  "NodeStatus",
  "KVEntry",
  "WriteResult",
  "ReadResult",
  "ConvergenceReport",
  "KeyDivergence",
  "Scenario",
  "Partition",
  "ReplicationStrategy",
  "ReplicationMode",
  "ConflictResolver",
  "ReadYourWritesReport",
  "MonotonicReadsReport",
  "ConsistentPrefixReport",
  "LinearizabilityReport",
];

const REQUIRED_PATHS = [
  "/api/v1/clusters",
  "/api/v1/clusters/{id}/state",
  "/api/v1/clusters/{id}/write",
  "/api/v1/clusters/{id}/read",
  "/api/v1/clusters/{id}/convergence",
  "/api/v1/scenarios",
];

test("OpenAPI spec exports all schemas consumed by generated.ts", () => {
  for (const name of REQUIRED_SCHEMAS) {
    expect(schemas, `schema "${name}" missing from openapi.yaml`).toHaveProperty(name);
  }
});

test("OpenAPI spec exports all API paths consumed by client.ts", () => {
  for (const path of REQUIRED_PATHS) {
    expect(paths, `path "${path}" missing from openapi.yaml`).toHaveProperty(path);
  }
});
