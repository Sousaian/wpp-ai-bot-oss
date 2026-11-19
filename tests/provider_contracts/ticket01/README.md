# Ticket 01 provider-contract harness

Scope: exact pinned-image source proof, synthetic Evolution presence transport,
and disposable Chatwoot private-note API checks. This harness does not import
the repository `.env`, join the shared project-scoped `<project>_wpp-net`, publish ports, pair WhatsApp, call an
LLM, or modify provider state outside its disposable Compose project.

Run:

```bash
tests/provider_contracts/ticket01/run.sh
tests/provider_contracts/ticket01/cleanup_fault_contract.sh
```

Requirements:

- Docker daemon
- pinned images already present locally
- `python:3.12-slim`, `pgvector/pgvector:pg15`, and `redis:7-alpine` present
  locally for the disposable fixture

The runner fails if it finds any container belonging to the shared
`traycer-wpp-ai-bot-soft-ocelot` Compose project. Every exit attempts teardown,
verifies source/client and Compose resources are absent, rechecks shared label
count zero, and returns nonzero if cleanup cannot be proved. Fault injection
checks preservation of an early status and cleanup-error propagation.

Evidence classes:

1. `PINNED_SOURCE_PROOF` → files copied from the exact image digests and checked
   against required routes, schemas, model limits, pagination, and response
   fields.
2. `SYNTHETIC_PRESENCE` → executable in-process HTTP behavior matching the
   source-proven control flow, including timeout/cancellation ambiguity.
3. `CHATWOOT_LIVE` → HTTP requests against the exact pinned Chatwoot image with
   synthetic account/inbox/contact/conversation data in disposable Postgres.

The Evolution source proof is not paired-device validation. It cannot prove
that WhatsApp renders a visible typing indicator, nor its client-side expiry.
