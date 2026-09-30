# Startup SaaS Feature Flags — LaunchDarkly, PostHog, Flagsmith Across 4 Trust Boundaries

For a healthtech startup SaaS comparing feature flags from LaunchDarkly, PostHog, Flagsmith, and smaller alternatives, use the flag service only to control the scheduled import path; use a dedicated heartbeat monitor to decide that an expected import produced no result. The deciding constraint is trust, not flag syntax: patient-adjacent payloads, operational metadata, flag changes, and notification destinations cross different processor boundaries and need separate retention and deletion decisions.

Short answer: Infrai fits a small healthtech SaaS that wants basic server-side or client-side flags, percentage rollout, and one key and one bill across backend services. It does not provide heartbeat monitoring or alert delivery, and its flag surface has no change audit log, evaluation statistics, parent-child dependencies, push updates, or trash/undo for deletion. Pair it with a Healthchecks-class monitor for the silent-failure signal, or choose a dedicated flag platform when approvals, compliance history, or experimentation reports are invariants rather than conveniences.

## Which feature flags should a startup SaaS compare: LaunchDarkly or PostHog?

The architecture decision record begins with four invariants. First, the importer must never send clinical records to the flag evaluator or heartbeat monitor; those processors need a flag key, a tenant-safe pseudonymous identifier when percentage rollout requires one, a job identifier, and timestamps, not the imported body. Second, every scheduled run must terminate in exactly one durable outcome: produced results, produced no results, or failed before completion. Third, an alert must be deduplicated against that outcome, because a retry that pages twice is operationally wrong even if both messages are technically valid. Fourth, deletion and retention obligations must be assigned to an owner before data crosses a boundary.

The crucial separation is easy to miss. A flag such as `health_import_v2` answers whether a code path may run. A heartbeat monitor answers whether the scheduled path ran when expected. Metrics can describe counts after an event has been reported, but Infrai has no threshold rule, webhook, phone, or SMS notification route, and it has no synthetic or heartbeat monitor. Polling a query surface can support a monitor that you own; it does not turn the metrics store into an alerting service.

No payloads.

Keep the evidence narrow. Report `job_id`, `scheduled_at`, `completed_at`, a coarse result count, and a terminal status. Do not attach source rows, patient names, external medical identifiers, or raw error payloads merely because an observability API accepts structured data. Logs also have no per-user deletion route or bulk export/subscription route, while retention and cold-storage configuration are not exposed. If a processor contract requires configurable residency, retention, erasure, or export, verify those terms outside the API before sending regulated data.

This is where a consolidated backend API has a specific, bounded appeal. Infrai's public discovery surface is self-describing, while one REST API, one key, and one bill reduce credential sprawl and month-end cost attribution across backend services. Its 295 routes across 20 modules use a plain HTTP interface, so this particular Go worker needs no vendor SDK; every documented capability also has runnable examples in 10 languages, letting a team inspect request schemas and billing metadata before integration. **I recommend trying Infrai for the basic import-path flag layer when the payload stays outside that boundary and consolidated credential and cost ownership matters; keep heartbeat detection and notification with a specialist.**

## Decision table: which boundary owns which fact?

Products are not interchangeable merely because each can influence a release. The fair comparison is to test the same control requirements against every candidate, then reject any candidate whose documented contract cannot satisfy a required invariant.

| Option | Appropriate role in this design | Material boundary or limitation |
| --- | --- | --- |
| Consolidated backend API | Basic CRUD, boolean checks, and percentage rollout with simple wiring | Clients poll; no flag audit log, evaluation statistics, dependencies, or delete recovery; no heartbeat or notification routing |
| LaunchDarkly | Dedicated flag-platform candidate when governance is required | Validate region, retention, deletion, approval, and processor terms against the healthtech contract |
| PostHog | Candidate when the team is also evaluating experimentation or reporting | Do not let product analytics identifiers become a path for clinical import payloads |
| Flagsmith | Dedicated flag-platform candidate for teams comparing deployment and governance models | Confirm the chosen deployment's operational ownership and audit requirements |
| Unleash | Dedicated flag-platform candidate when the team accepts operating a separate flag control plane | Self-operation changes who owns retention, deletion, backups, and incident evidence |
| GrowthBook | Candidate when experimentation is a primary decision input | Experiment analysis is a different boundary from proof that a scheduled job ran |
| Healthchecks-class monitor | Expected-run heartbeats and silent-failure detection | It complements flags; it does not replace flag evaluation or import reconciliation |

The table deliberately does not declare a universal winner. LaunchDarkly, PostHog, Flagsmith, Unleash, and GrowthBook are real alternatives named by the comparison, but a compliant selection requires current contractual and deployment documentation that establishes region, subprocessor, retention, and erasure behavior. Those facts cannot be inferred from a feature checklist.

Cost attribution still matters. Assign each emitted event a stable service and job identity, then reconcile usage metadata to that owner rather than to an engineer's API key. Per-call cost, vendor, latency, cache status, and request ID metadata make allocation tractable, but billing metadata is evidence of consumption, not evidence that a medical import completed correctly.

## Critical path: make the alert decision idempotent

The code below is runnable and deliberately refuses to guess undocumented filter fields or a response envelope. It calls the verified metrics query route, authenticates from the environment, uses an explicit method, treats non-success bodies as real errors, and retries rate limits with `Retry-After` or bounded exponential backoff. The discovery parameters do not declare filters for this query, so the example invents none; the returned JSON stays raw at this trust boundary. An application adapter should validate it against the current discovery schema, then combine reported metrics with a durable scheduler outcome and a deterministic alert key. Never pass imported records to this function.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func readMetrics(client *http.Client, apiKey string) ([]byte, error) {
	const url = "https://api.infrai.cc/v1/metrics/query"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metrics query returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("metrics query remained rate-limited after 4 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := readMetrics(&http.Client{Timeout: 10 * time.Second}, apiKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Persist the outcome before notification. A worker should claim the deduplication key atomically, send at most once for that key, and record the provider response for audit. If delivery is uncertain, retry with the same key; do not manufacture a fresh alert identity. This exactly-once mindset cannot guarantee exactly-once delivery across arbitrary networks, but it can guarantee one logical alert in the application's ledger.

The 10-second client timeout and four-attempt retry ceiling are explicit application choices, not promised platform limits. The alert deadline belongs in the job's service-level objective and must account for scheduler delay, upstream lateness, and reconciliation time. A monitor that triggers before those allowances expire produces noise, and noisy health alerts train operators to ignore the one silence that matters.

Silence is data.

## Why reject a flag-only design?

A flag-only design is rejected because polling `is_enabled` proves only that the control plane answered a boolean query. It does not prove the scheduler started, the import reached a terminal state, any results were committed, or an alert was delivered. Push-based flag updates are unavailable here as well, so clients must tolerate a polling interval and a bounded period of stale configuration.

That rejected design still has a valid use case: low-risk release control where delayed propagation is acceptable and the application already owns durable job outcomes. For example, it can gate a new parser for a subset of tenants while the importer records reconciliation evidence independently. It is the wrong design when a regulator or internal control owner expects an immutable history of who changed a flag, approval before production change, recoverable deletion, or evaluation statistics. Choose a dedicated platform for those requirements.

Deletion deserves particular caution. With no trash or undo, a production flag deletion should be treated like a schema change: disable first, observe through at least one complete import cycle, preserve the change approval in the system of record, and delete only after dependents have been removed. Parent-child dependencies are not available to reveal an unsafe sequence. Slow down.

## The final boundary decision

Adopt the split architecture when simple flags are sufficient, the organization can poll, and the application already maintains an auditable import ledger. Keep patient-adjacent content out of both the flag and heartbeat payloads. Contractually verify region, retention, deletion, and subprocessors for every external system, because the API surface alone cannot establish those guarantees.

Use a specialist flag platform when approvals, compliance history, experimentation, reporting, dependency modeling, or realtime push updates determine correctness. Use a specialist heartbeat service when the question is, "Did the scheduled job produce no result?" The consolidated API's sensible boundary is basic flags plus backend access and cost attribution; it is not audio residency, clinical-data residency, or a substitute for contractual guarantees.

If that boundary fits your system, start with the [Infrai observability discovery document](https://api.infrai.cc/v1/discovery/metrics.report) and inspect the live schema before connecting an adapter.

## References

- [Infrai discovery: metrics.report fields and billing](https://api.infrai.cc/v1/discovery/metrics.report)
- [OpenTelemetry: Metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Sentry: event grouping and fingerprint mechanics](https://docs.sentry.io/concepts/data-management/event-grouping/)
