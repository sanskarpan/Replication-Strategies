# Security Policy

## Scope

**Replication Strategies is an educational distributed-systems simulator.** It runs a
cluster of simulated nodes *in a single process*, over an in-memory network fabric,
with no persistence, no authentication, and no real network exposure beyond a local
HTTP/WebSocket API intended for a developer's own machine.

It is **not** production infrastructure and must not be deployed as a multi-tenant or
internet-facing service. The REST and WebSocket endpoints are unauthenticated by
design and trust the local operator. Please treat any "vulnerability" that depends on
exposing the server to untrusted clients in that light — the recommended mitigation is
simply not to expose it.

That said, we still care about defects that could harm a user running the tool
locally (for example: a crafted request that crashes the process, an unbounded
resource-consumption bug, or a dependency with a known CVE), and we welcome reports.

## Supported versions

The project is developed on `main`. Security fixes are applied to the latest commit on
`main`; there are no separately maintained release branches.

| Version | Supported |
|---------|-----------|
| `main` (latest) | ✅ |
| Older tags/commits | ❌ (please update to the latest `main`) |

## Reporting a vulnerability

Please report vulnerabilities **privately** — do not open a public issue or pull
request that discloses the problem before it has been addressed.

- Preferred: open a [GitHub private security advisory](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
  ("Report a vulnerability" under the repository's **Security** tab).
- Alternatively, contact the maintainer directly through the email associated with the
  repository's commit history.

When reporting, please include:

- A description of the issue and its potential impact.
- Steps to reproduce (the strategy, cluster configuration, and the exact request or
  input that triggers it).
- Any suggested remediation, if you have one.

### What to expect

- We aim to acknowledge a report within a few days.
- We will investigate, confirm the issue, and keep you updated on remediation.
- Once a fix is available on `main`, we will credit the reporter (unless you prefer to
  remain anonymous) and, where appropriate, publish an advisory.

Thank you for helping keep the project and its users safe.
