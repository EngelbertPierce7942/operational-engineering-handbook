# 4 Forensic Checks for a Budget Next.js SaaS — Structured Logging Platform

TL;DR: A scheduled import that emits no results is an evidence-design problem before it is a log-search problem. For a Next.js customer-support SaaS, record an immutable run identity, a separate idempotent attempt identity, an explicit terminal outcome, and an independently evaluated freshness signal. Then compare Sentry Logs, Axiom, Better Stack Logtail, and a generic hosted logs API by replaying the same missing-run investigation, including export and deletion, rather than by comparing headline ingestion prices.

A conventional error alert misses the dangerous case: nothing throws because the scheduler never starts the import, a queue accepts work that no worker completes, or an upstream feed legitimately contains zero records. Absence has no stack trace. The system needs a durable expectation that can be compared with observed outcomes outside the execution path being watched.

## What should a budget Next.js SaaS require from structured logging?

A Next.js route returning 200 proves only that one request completed. It does not prove that the 02:00 UTC tenant import was scheduled, claimed, committed, or reflected in the support index. Those are separate transitions, and collapsing them into `import finished` destroys distinctions needed during an incident.

For a tenant whose import is due every 15 minutes, the useful question at 02:31 is: which scheduled run should exist, and what is the last durable state we can prove for it? That wording forces the architecture to represent expected work. A control table can hold `tenant_id`, `schedule_slot`, `run_id`, `expected_by`, and `terminal_state`; a monitor alerts when `expected_by` passes without a terminal record. The logging platform receives evidence, but it does not define truth.

Zero results need their own terminal outcome. An empty upstream response may be valid, while a parser that silently discards every row is not. Counts at the input, accepted, rejected, and committed boundaries make that difference reconstructable. Do not put customer message bodies, email addresses, or access tokens in those fields. GDPR Article 17 creates an erasure obligation in applicable circumstances, so deletion and retention belong in the design review.

Fail closed.

## Four evidence rules for an auditable run

First, assign the scheduled occurrence a deterministic `run_id`, derived from stable coordinates such as tenant and schedule slot, and assign each delivery a separate `attempt_id`. Reusing the run identity lets retries converge on one logical import; preserving attempt identity keeps the audit trail honest. An exactly-once mindset does not require pretending networks deliver exactly once. It requires an idempotency boundary at commit and enough evidence to prove how duplicate attempts were handled.

Second, model states rather than prose. `scheduled`, `claimed`, `fetched`, `validated`, `committed`, `empty`, and `failed` are queryable. Emit an event timestamp and, where transport can delay delivery, an ingestion timestamp. Keep clocks synchronized, but never use timestamp equality as the deduplication key.

Third, make terminality explicit. A run ends in `committed`, `empty`, or `failed`; a timeout is an observation by the monitor, not a fabricated worker outcome. That distinction matters because an alert may race with a late worker. Append correction events or update a separate current-state projection while retaining original evidence under the approved retention policy.

Fourth, calculate freshness independently. If the same worker performs the import and declares itself healthy, a dead worker produces neither results nor alarms. Evaluate `now > expected_by` from a separate monitor in a different failure domain. No log query should be the sole source of both the expectation and its proof.

```go
package evidence

import "time"

type ImportEvent struct {
	SchemaVersion  int       `json:"schema_version"`
	RunID          string    `json:"run_id"`
	AttemptID      string    `json:"attempt_id"`
	TenantRef      string    `json:"tenant_ref"`
	ScheduleSlot   time.Time `json:"schedule_slot"`
	ObservedAt     time.Time `json:"observed_at"`
	State          string    `json:"state"`
	InputCount     int       `json:"input_count"`
	AcceptedCount  int       `json:"accepted_count"`
	RejectedCount  int       `json:"rejected_count"`
	CommittedCount int       `json:"committed_count"`
}
```

The schema carries a pseudonymous tenant reference rather than customer content. `schema_version` supports controlled evolution, while bounded state values keep queries predictable. Prometheus instrumentation guidance warns against labels with unbounded cardinality; apply that caution to indexed dimensions and metric labels. Keep `run_id` in logs and traces where investigation needs it, but do not turn every run into a metrics label.

## Reconstruct the incident before comparing platforms

Create a fixed fixture with four runs: one commits 240 records on its first attempt; one validly returns zero; one fails validation after receiving 17 records; and one is expected but never starts. These numbers are synthetic test inputs, not production measurements. They make the evaluation repeatable and expose semantic gaps that a polished demo can hide.

For each candidate, ask an engineer unfamiliar with the setup to determine which run is missing, whether it was never scheduled or merely never completed, whether any attempt crossed the idempotent commit boundary, and which records require reconciliation. Then test machine-readable export and locating data by tenant reference for an erasure workflow. Record the query, investigation steps, ambiguous results, and manual joins. Visual preference is not forensic completeness.

Treat Sentry Logs, Axiom, Better Stack Logtail, and a generic hosted logs API as candidates with different integration boundaries, not as a ranking. Test Sentry Logs in a workflow that may also contain application errors and traces. Test Axiom around ingestion and query analysis. Test Better Stack Logtail as a log-management workflow. Charge a bare hosted API for the engineering around it: authentication, buffering, query tooling, alert evaluation, access control, export, and deletion remain system responsibilities unless its contract supplies them. Capabilities and limits change, so verify them against current documentation and written service terms.

Each choice has a limitation. A workflow centered on error correlation may be unsuitable when the dominant task is broad log analytics; an analytics-oriented store still cannot repair a missing expectation model; a managed log interface cannot prove an import happened if the application never emitted the run identity; and a bare API leaves more operational machinery with the team. Those are architectural trade-offs, not verdicts about product quality. The acceptance fixture reveals which boundary matters here.

Evidence first.

| Decision evidence | Acceptance test | Disqualifying ambiguity |
|---|---|---|
| Missing-run detection | Omit the 02:00 run and pass `expected_by` | Silence resembles a valid empty run |
| Correlation | Search one `run_id` across two attempts | Retries resemble separate imports |
| Audit export | Export fields, timestamps, and schema version | Reconstruction fields disappear |
| Privacy operation | Locate and delete one tenant reference | Scope or completion is unprovable |
| Failure isolation | Stop worker while monitor remains active | Monitor disappears with workload |

Budget belongs in the model, but price should not drive the architecture. Estimate daily bytes from measured event size and run frequency, then include retention, query frequency, indexed-field choices, export traffic, and labor required for missing pieces. A low ingestion quote can coexist with an expensive incident if a responder must join incomplete evidence by hand. Retaining every payload indefinitely, meanwhile, creates privacy liabilities without improving a well-designed audit record.

## Error handling, deployment, and reconciliation

Instrumentation participates in failure handling without becoming the transaction. Buffer events with bounded memory, define behavior under backpressure, and expose dropped-event counts through a separate signal. The business commit should not roll back because a remote log endpoint is unavailable. Where audit requirements demand durable evidence, persist a local outbox record in the same transaction, then deliver it asynchronously with an idempotency key. Stronger reconstruction consumes storage and attention; coupling the customer-data commit to telemetry creates a larger failure surface.

Deploy schema changes for mixed-version readers. Add fields before requiring them, keep old states queryable for the retention window, and include both versions in alert tests. A canary should prove that the expected-run monitor recognizes old and new producers. Rollback then becomes a schema-compatibility exercise, not a hope that yesterday's query parses today's event.

Reconciliation should be boring. Given a `run_id`, compare the committed count with the destination's durable count, check whether another attempt already crossed the idempotency boundary, and only then retry. If those answers require reading raw customer content, the evidence model is too weak and the privacy exposure too broad.

Operational access needs an audit trail too: restrict tenant-scoped queries, record administrative access where policy requires it, and test retention and deletion procedures. Compliance limits are system constraints. They cannot be delegated to a dashboard label.

## A compact rollout that preserves evidence

Start in shadow mode for one schedule interval: write versioned events and evaluate freshness without paging. Replay the four-run fixture outside production and verify exported evidence, deletion, and duplicate-attempt behavior. Then enable paging for a small tenant cohort while retaining the existing detector long enough to compare outcomes. Remove the old path only after both detectors agree on expected, empty, failed, and missing runs, and document the rollback condition before widening deployment.

The decision should fit in an architecture record: evidence contract, idempotency boundary, monitor failure domain, retention and erasure procedure, export format, operational owner, and measured investigation result for each candidate. That record is more durable than a feature matrix, and it keeps the obligation visible: a missed support import must be reconstructable without guessing.

## Sources

References:

- Prometheus, "Instrumentation": https://prometheus.io/docs/practices/instrumentation/
- GDPR Article 17, "Right to erasure": https://gdpr-info.eu/art-17-gdpr/
- Sentry documentation, "Logs": https://docs.sentry.io/product/explore/logs/
- Axiom documentation: https://axiom.co/docs
- Better Stack documentation, "Logs": https://betterstack.com/docs/logs/
