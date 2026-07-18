# 1. Record architecture decisions

Date: 2025-07-18

## Status

Accepted

## Context

We need to record the architectural decisions made on this project, so that the
reasoning behind non-obvious choices — especially around concurrency, the network
model, and the replication protocols — is preserved for future contributors instead of
living only in commit messages or in a maintainer's head.

## Decision

We will use Architecture Decision Records, as [described by Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

Each record is a short Markdown file in `docs/adr/`, numbered sequentially and named
`NNNN-title-with-dashes.md`. A record captures a single decision and its context, and
has the following sections:

- **Title** — a short noun phrase.
- **Status** — proposed, accepted, deprecated, or superseded (with a link).
- **Context** — the forces at play: technical, educational, and project constraints.
- **Decision** — the change we are proposing or have agreed to, in active voice.
- **Consequences** — what becomes easier or harder as a result, including any tradeoffs.

Records are immutable once accepted: rather than editing a past decision, we add a new
record that supersedes it.

## Consequences

- New contributors can read `docs/adr/` in order and understand *why* the system is
  shaped the way it is, not just *what* it does.
- Decisions become reviewable artifacts in pull requests, encouraging deliberate design.
- There is a small ongoing cost to writing and maintaining the records, which we accept
  in exchange for durable institutional memory.

See Michael Nygard's article, linked above, for the original description of this
lightweight format.
