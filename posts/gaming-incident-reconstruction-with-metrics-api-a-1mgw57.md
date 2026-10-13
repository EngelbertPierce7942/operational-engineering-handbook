# Gaming Incident Reconstruction with Metrics API Alerting Under 429 Rate Limits

A game notification system has an awkward constraint: the telemetry endpoint can become least available during the burst that matters most. A tournament result, guild event, or security message can create both delivery failures and enough monitoring traffic to trigger HTTP 429 responses. Treating each missed poll as an empty result turns rate limiting into a blind spot.

**Short answer:** make the cron worker a stateful, idempotent consumer. Query overlapping time windows, honor `Retry-After` when it is present, use bounded exponential backoff with jitter otherwise, and advance a durable watermark only after the observations and resulting alert decision are committed. Metrics should detect the incident; an immutable delivery-attempt trail should reconstruct it.

A counter can show that failures increased, but it cannot establish which notification attempt failed before a retry succeeded, which channel was involved, or whether an alert was already emitted. Aggregated telemetry is evidence of a condition, not the system of record.

## How should a cron worker poll a metrics API for error alerting?

Consider a worker scheduled every minute that asks for the previous minute of failure counts. If the 12:01 poll receives 429 and the next run asks only for 12:01 through 12:02, the earlier interval has vanished from the worker's view. Retrying the request helps, but only until the process restarts or another scheduled invocation races it. The defect is in cursor ownership, not the delay formula.

Replay is expected.

The safer model resembles ledger processing. Store a committed watermark, derive the next query from that watermark rather than wall-clock schedule time, and include a small overlap to tolerate delayed ingestion. Deduplicate observations by a stable identity such as `(series, bucket_start)` and alert decisions by an incident key such as `(rule_id, window_start, threshold_revision)`. Only then move the watermark.

Order matters. Query, normalize, persist evidence, evaluate, record the alert decision, and commit the cursor. A crash before the final step causes replay; uniqueness constraints make replay harmless. A crash after it cannot strand an unrecorded decision because that decision was committed in the same transaction. This is an exactly-once mindset implemented over at-least-once execution, not a claim that networks deliver exactly once.

Picture the full failure window. The 12:00 poll reads through 11:59, a tournament ends at 12:00:20, and the resulting notification burst reaches the telemetry service after its normal ingestion delay. The 12:01 request is throttled. At 12:01:40 the worker process is replaced during deployment, so its in-memory retry count disappears; at 12:02 a new process acquires the lease. A schedule-derived query would begin at 12:01 and miss the first forty seconds of the burst, while a watermark-derived query begins at the last committed boundary, reads the overlap again, and collapses already-seen buckets through their stable identities. If the response is persisted but the process stops before alert evaluation, the next owner repeats evaluation against the same evidence and encounters the decision key rather than sending a second page. Each individual mechanism is mundane. Their ordering is the control.

Keep the underlying notification attempt trail append-oriented. A useful record carries an attempt ID, notification ID, intended channel, event time, provider outcome class, retry ordinal, and correlation ID. Avoid recipient addresses or message bodies in metric labels: every unique label combination creates another time series, and sensitive payloads do not belong in operational dimensions. Retention and access must follow the applicable audit and privacy obligations.

## A poller that treats 429 as flow control

This Go fragment shows the network boundary. It deliberately returns observations without updating a cursor; cursor advancement belongs to the caller's database transaction. The retry cap prevents an unattended invocation from waiting forever, while jitter reduces synchronized retries across replicas.

```go
package polling

import (
    "context"
    "encoding/json"
    "errors"
    "io"
    "math/rand/v2"
    "net/http"
    "strconv"
    "time"
)

type Bucket struct {
    Start    time.Time `json:"start"`
    Failures uint64    `json:"failures"`
}

type Response struct {
    Buckets []Bucket `json:"buckets"`
}

func retryDelay(h http.Header, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    base := time.Second << min(attempt, 5)
    return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

func Poll(ctx context.Context, client *http.Client, url string) (Response, error) {
    for attempt := 0; attempt < 6; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
        if err != nil {
            return Response{}, err
        }
        res, err := client.Do(req)
        if err != nil {
            return Response{}, err
        }

        if res.StatusCode == http.StatusTooManyRequests {
            io.Copy(io.Discard, res.Body)
            res.Body.Close()
            timer := time.NewTimer(retryDelay(res.Header, attempt))
            select {
            case <-ctx.Done():
                timer.Stop()
                return Response{}, ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if res.StatusCode != http.StatusOK {
            res.Body.Close()
            return Response{}, errors.New("metrics endpoint returned a non-success status")
        }

        var out Response
        err = json.NewDecoder(res.Body).Decode(&out)
        res.Body.Close()
        return out, err
    }
    return Response{}, errors.New("retry budget exhausted")
}
```

RFC 9110 defines 429 as a signal that the client sent too many requests in a period and permits a `Retry-After` response indicating how long to wait. The header can contain either delay-seconds or an HTTP date, so production parsing should support both forms; the compact example handles delay-seconds and falls back safely. A per-request timeout and an invocation deadline remain necessary. Backoff without a deadline converts a stuck poll into a stuck worker.

Deadlines win.

Do not launch a second scheduled run while the first owns the lease. A database advisory lock or lease row with an expiry is sufficient, provided ownership is checked again when committing. Reuse the HTTP client, and expose counters for throttled polls, retry exhaustion, decoded buckets, and deduplicated buckets. Track watermark lag too. It is decisive: a successful request can still return stale or incomplete data.

## From threshold crossing to defensible reconstruction

A single failure counter is cheap and useful, but an incident timeline needs more structure.

| Artifact | Question answered | Identity |
|---|---|---|
| Failure metric bucket | Is delivery health degrading? | series plus bucket start |
| Poll receipt | What did the worker observe, and when? | request ID plus query window |
| Alert decision | Why did a page fire or remain quiet? | rule, window, revision |
| Delivery attempt | What happened to one notification? | immutable attempt ID |

The alert decision should capture the evaluated value, comparator, threshold revision, source window, and evidence checksum. That is more data than a boolean, yet it is the difference between "the graph looked high" and a reproducible decision. Corrections should append a superseding decision rather than rewrite the old one. Audit trails lose their force when history is mutable.

There is a cardinality trap. `game_id`, channel, outcome class, and region may be bounded enough for metrics after deliberate review; `player_id`, notification ID, and raw error text usually are not. Put high-cardinality identifiers in the attempt trail or structured logs, then connect them through a correlation ID recorded with the poll receipt. Prometheus recommends avoiding overuse of labels and suggests another approach when a metric could exceed roughly 100 cardinality or grow without bound. That figure is a review trigger, not a universal capacity limit.

The trade-off is operational weight. Durable receipts, leases, uniqueness constraints, and correction revisions demand database capacity and careful retention; this design is a poor fit for a low-consequence notification stream where a delayed or duplicate alert has no material impact and coarse dashboard inspection is enough. Direct threshold evaluation over a local counter is simpler there. Conversely, polling alone is unsuitable when the source cannot provide stable historical windows: no retry policy can reconstruct data the source has already discarded, so the boundary needs durable events or an append-only attempt store before alerting can be auditable.

Late data needs a declared policy. Evaluate a closed interval only after an ingestion allowance, then re-evaluate it during a finite correction horizon. If late buckets change the result, append a revised decision with the same incident key and a higher revision. Derive the allowance from the observed ingestion-delay distribution and test it during burst traffic.

## Testing the gaps rather than the happy path

Unit tests for backoff arithmetic are weak evidence by themselves. Useful tests control time and inject failures at transaction boundaries. Return 429 twice, then 200, and verify that the query window has not moved. Terminate the process after persisting evidence but before cursor commit, restart it, and verify that replay creates neither a second decision nor a missing bucket. Run two workers against the same lease and prove that only its current owner can commit.

Also test malformed JSON, an oversized response body, an HTTP date in `Retry-After`, clock skew, delayed buckets, and a permanently throttled endpoint. The last case should leave the watermark unchanged, increment retry-exhaustion telemetry, and make lag visible to a separate monitor. Otherwise the mechanism that detects delivery failures can fail silently itself.

The alert rule deserves versioned fixtures. Feed known buckets into the evaluator and assert the full decision record, not merely whether it fired. This protects reconciliation when a threshold changes: an operator can rerun the old revision against the recorded window and obtain the same result. Keep tests free of real recipient data. Observability fixtures tend to live longer and spread farther than their authors expect.

## Roll out with a shadow watermark

Begin with a read-only worker that writes poll receipts and proposed decisions under a shadow watermark. Compare its windows with the existing alert path across normal and burst traffic, including intentional 429 responses. Then enable decision persistence while suppressing outbound pages, verify deduplication under restarts, and finally grant one leased worker permission to emit alerts.

The migration is complete when three invariants hold: committed windows have durable evidence, replay does not duplicate an alert decision, and lag cannot increase without producing a separate health signal. A throttled telemetry endpoint then remains operationally inconvenient, but it no longer erases the history needed to explain a gaming notification incident.

## References

- https://www.rfc-editor.org/rfc/rfc9110.html#name-429-too-many-requests
- https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
- https://prometheus.io/docs/practices/instrumentation/
- https://opentelemetry.io/docs/specs/otel/logs/data-model/
