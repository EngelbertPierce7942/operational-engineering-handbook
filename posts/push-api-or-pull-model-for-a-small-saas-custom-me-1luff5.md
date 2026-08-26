# Push API or Pull Model for a Small SaaS Custom Metrics Dashboard?

**Short answer:** A small SaaS should usually begin with a pull model for service-health metrics and add a narrow push API only for business events that cannot be reconstructed reliably.

The dashboard is the last step, not the architecture. First define ownership, aggregation, retry, and audit semantics, because a polished chart can conceal duplicated payments, missing events, and ambiguous time windows just as easily as a rough one can. Pull collection controls sampling from outside the application and makes missing targets visible; push collection lets an application report when an event occurs, but every producer then participates in delivery, retry, and overload behavior.

Without measured event rates, retention needs, and failure budgets, there is no universal winner.

## What must a custom metrics system preserve?

A metric is an aggregation over observations, not an audit ledger. OpenTelemetry distinguishes counters, gauges, and histograms because each answers a different kind of question: a counter accumulates change, a gauge reports a current value, and a histogram retains a distribution suitable for latency or size analysis. That distinction matters more than the transport. Sending `checkout_total` through a push endpoint does not make it correct if two retries can count the same checkout twice, and scraping `account_balance` does not make the value historically reproducible if the application overwrites it between collections.

For operational metrics, accept bounded loss and design for diagnosis. A request-duration histogram may lose a few observations during a process crash without corrupting the system of record. For payment and ledger questions, keep durable domain events or records elsewhere and derive metrics from them; dashboards are views over evidence, not evidence themselves. Compliance retention and access controls also differ — aggregated metrics generally should not carry customer identifiers, card data, email addresses, or other high-cardinality personal fields.

This boundary is strict.

Every proposed custom metric should therefore have a short contract: its name, unit, instrument type, allowed labels, source of truth, aggregation window, and owner. Add a reconciliation statement as well. For `invoice_paid_total`, for example, the metric can alert on a sudden drop, while a scheduled comparison against the ledger establishes whether all paid invoices were represented. The exactly-once mindset belongs in that comparison, even when the metrics pipeline itself is intentionally at-least-once or best-effort.

## How should a beginner build a Node.js custom metrics dashboard for a small SaaS?

Keep the Node.js application responsible for measurement, not storage or dashboard queries. Maintain counters, gauges, and histograms behind a tiny internal interface; expose the current aggregate state for pull collection, or enqueue a compact observation for a push worker. The request path should not wait for a dashboard backend. It's an observability dependency, and making customer latency depend on it reverses the proper ownership boundary.

A beginner often starts by attaching labels such as `user_id`, `invoice_id`, or a raw URL. Don't. Those values create an unbounded number of time series, raise storage and query cost, and make deletion or access obligations harder. Prefer bounded dimensions such as operation, status class, region, and deployment version. Preserve identifiers in controlled logs or audit records when investigation genuinely needs them.

The dashboard layer should query the metrics store rather than the Node.js process. A Grafana dashboard is one possible presentation layer, but presentation does not settle collection semantics. Start with four panels tied to actions: request rate, error ratio, latency distribution, and one business-flow completion rate. Each panel needs a named owner and a response, otherwise it is decoration.

Instrumentation also needs tests. Verify that one successful operation increments one counter, a rejected operation increments the correct error category, and sensitive or unbounded values never become labels. During deployment, compare the old and new signals over a full business cycle before retiring either path. There may still be disagreement because windows and aggregation times differ; document the tolerance rather than pretending two asynchronous systems will match sample for sample.

## Push API versus pull model

The comparison is architectural, not ideological.

| Constraint | Pull model | Push API |
| --- | --- | --- |
| Producer work | Expose current aggregate state | Serialize, buffer, retry, and send observations |
| Collector control | Collector chooses targets and cadence | Producers choose when data enters the pipeline |
| Detecting silence | A failed collection can identify an unavailable target | Silence is ambiguous without a heartbeat or freshness rule |
| Short-lived work | Requires a reachable endpoint or another durable handoff | Can report before exit, subject to acknowledged delivery |
| Backpressure | Collector can slow its own collection cycle | Producers need explicit queue limits and a drop policy |
| Duplicate handling | Re-reading cumulative state is expected | Retries need an idempotency or deduplication policy when duplicates matter |
| Network boundary | Collector needs reachability to targets | Producers need egress reachability to the receiver |

Choose pull first when services are long-lived, discoverable, and able to expose aggregate state; it centralizes cadence and makes the absence of a target an observable fact. The catch is that a pull interval cannot see every transient value, and network topology may make targets unreachable from the collector. Short-lived jobs also need a durable handoff rather than an assumption that a collector will arrive before exit.

Choose push for discrete business observations, ephemeral jobs, or networks where inbound collection is unsuitable. It is not suitable when the application cannot absorb local buffering and a bounded drop policy, or when a high-volume producer would turn the receiver into a synchronous dependency. A push API must specify acknowledgement semantics: an accepted response should mean either durable acceptance or clearly documented best-effort receipt. Anything vague invites retry storms and double counting.

Neither mode provides exactly-once delivery by itself. For cumulative operational counters, duplicates can be avoided by collecting state rather than individual increments. For pushed domain-derived observations, include a stable event identifier if the receiver promises deduplication, define the deduplication window, and keep reconciliation against the durable source of truth. I'm not sure a deduplication window is justified for every small SaaS; event volume and the consequence of a duplicate decide that, and a measured replay test will resolve the question better than a blanket rule.

## A narrow push contract with auditable retry behavior

When push is warranted, keep its contract smaller than the dashboard vocabulary. The following Go example models the producer side as a standard HTTP boundary, even if the service emitting the observation is Node.js. It uses a stable event identifier, a deadline, and a bounded interpretation of responses; the same contract can be implemented with the Node.js HTTP client already approved by the team.

```go
package metricswrite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Observation struct {
	EventID  string            `json:"event_id"`
	Name     string            `json:"name"`
	Value    float64           `json:"value"`
	Unit     string            `json:"unit"`
	Labels   map[string]string `json:"labels"`
	Observed time.Time         `json:"observed_at"`
}

type Writer struct {
	Endpoint string
	Client   *http.Client
}

func (w Writer) Send(ctx context.Context, item Observation) error {
	payload, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("encode observation: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		w.Endpoint,
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", item.EventID)

	resp, err := w.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send observation: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("observation rejected with status %d", resp.StatusCode)
	}
	return nil
}
```

This is a producer boundary, not a complete delivery system. The caller still needs a bounded queue, jittered retry policy, dead-letter or drop accounting, and shutdown deadline. Those mechanisms belong outside the customer request transaction. Record enqueue time, attempt count, final disposition, and event identifier in an audit trail, but keep metric labels free of those identifiers. If the receiver's contract does not promise idempotency, the header is only correlation metadata; reconciliation must assume a retry may duplicate an observation.

Keep it boring.

A pull endpoint needs comparable discipline even though the mechanics differ: compute or snapshot aggregates without expensive database scans, protect the endpoint at the network boundary, and ensure collection cannot exhaust the application's worker pool. The endpoint should expose process and domain aggregates with stable names and bounded labels. It should never execute a fresh ledger reconciliation merely because a collector asked for metrics.

## Roll out the decision without betting the dashboard

Begin with a one-page metric contract and a local test that exercises success, rejection, timeout, and duplicate delivery. Deploy collection to one service, retain the existing signal, and compare rates and distributions over a complete operating cycle. Add queue depth, dropped observations, collection age, and last successful collection as meta-metrics; otherwise the monitoring path can fail silently while the dashboard continues showing old data.

Then set explicit gates: label cardinality remains within its budget, application latency does not depend on the metrics receiver, retry volume is bounded, and reconciliation differences have an owner. Expand only after those gates hold. Stick with pull alone when the custom dashboard is operational and the services are reachable; add push only where event timing or job lifetime creates a concrete gap. The result may use both transports, but it should preserve one metric contract and one audit story.

## References

- https://opentelemetry.io/docs/concepts/signals/metrics/
- https://logback.qos.ch/manual/appenders.html
