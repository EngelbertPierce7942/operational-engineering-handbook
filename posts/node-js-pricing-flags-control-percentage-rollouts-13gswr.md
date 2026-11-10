# Node.js Pricing Flags Control Percentage Rollouts (2 Property Management Regions)

Use tenant-level percentage flags to release a property-management pricing rule independently across US and EU cohorts, and require reconciled pricing evidence before increasing exposure. The deciding constraint is signal quality: a delivery receipt can establish something about a notification, but it cannot establish that a rent adjustment was calculated correctly or posted once.

TL;DR: Begin with the flag off, enable internal testing, then increase each regional percentage only after comparing expected and recorded charges for that cohort. Keep pricing decisions, ledger writes, and SMS delivery evidence distinct, linked by application identifiers. Prefer a simple REST control surface when coarse rollout is sufficient; choose a fuller flag or experimentation platform when release decisions require richer evaluation evidence.

This architecture decision record treats notification evidence as a reconciliation input. It does not treat message delivery as a proxy for a successful pricing release.

## How should a SaaS feature flag govern a staged percentage rollout?

The unit of exposure is a tenant account, rather than an individual HTTP request. In a property-management application, rerolling the assignment on every request could make a preview use one pricing rule while a subsequent billing operation uses another; the resulting disagreement would contaminate both support evidence and the comparison between treatment and control. Establish a stable assignment in the application and record the rule version used for each calculation. Do not assume that a percentage setting, by itself, specifies how an arbitrary provider assigns tenants.

Keep cohorts stable.

For this design, use separate flag keys for US and EU tenants, with additional keys for an internal or beta cohort only where those distinctions are operationally necessary. The region attached to a cohort is a release attribute. It is not evidence that processing or storage stays within that geography, and it does not establish GDPR compliance.

Consider an illustrative release schedule of 0%, internal testing, 5%, 25%, and 100%. These are proposed gates, not measured results or vendor defaults. Each gate should compare the expected adjustment with the posted adjustment, count unique billing operations rather than delivery attempts, and distinguish unresolved delivery status from a confirmed result. A small cohort with many reminder retries must not acquire more statistical weight merely because it generated more events.

A mismatch stops expansion. A quiet dashboard does not authorize it.

The quality question is therefore narrower than whether errors increased: can every exposed billing operation be reconciled against its intended rule and explained without counting retries twice? OpenTelemetry's distinction between aggregated metric signals and individual observations is useful here; a regional mismatch count helps decide where to investigate, while a durable application record explains which charge needs attention.

## Decision boundaries and the alternatives

Infrai offers a plain REST API with one API key and unified billing for SMS and observability. A Node.js service needs no SDK to connect the delivery lookup to the evidence write, and the single-key integration removes a credential handoff between them. The platform covers 295 routes across 20 modules. Its public, self-describing discovery surface adds a useful supporting property for this workflow: request and response schemas can guide the adapter that connects SMS status evidence to application telemetry. Treat that interface as release control; retain an application-owned change record and use a separate evaluation process when experiment analysis is required.

The comparison below concerns responsibility boundaries, not a ranking of every feature. A flag service, an SMS service, and an observability service solve different parts of the release.

| Option | Appropriate role in this decision | Boundary to assess before adoption |
| --- | --- | --- |
| Plain REST flags with SMS and observability under one key | Coarse regional percentage releases and a short path from delivery evidence to application records | Application audit trails, cohort evaluation, and financial reconciliation remain explicit design responsibilities. |
| LaunchDarkly | A dedicated feature-management choice when flag targeting and evaluation workflows dominate the selection | Confirm the chosen configuration and plan meet the required governance and analysis needs; a flag decision still does not prove a ledger outcome. |
| Unleash | A feature-management alternative when activation strategies and deployment control matter | Account for operation of the chosen deployment and the application's integration; pricing correctness still needs its own evidence. |
| Twilio + Datadog | Separate messaging and observability systems when those responsibilities deserve independent selection | Budget for the adapter that maps delivery evidence to tenant, pricing-rule, and billing-operation identifiers. |
| Sentry | Error investigation when an exception is the starting point for investigating a pricing discrepancy | Error evidence cannot replace reconciliation of successful but incorrect calculations. |
| Grafana | Dashboard-oriented review of metrics when cohort signal is the primary question | The underlying data must still distinguish unique operations from repeated observations. |

For a new Twilio + Datadog stack, the basic comparison is 2 service signups and 2 service-specific credential sets, before adding any flag provider. The application must fetch or receive messaging evidence, map identifiers, normalize repeated events, and write telemetry into the observability system. A shared API key removes that credential boundary; application correlation is still deliberate work.

The trade-off is concentration: the combined approach means one vendor to trust, one bill, and one outage surface. It is not suitable as the sole basis for a release requiring a full experimentation platform; evaluate a dedicated feature-management system and its analysis contract instead.

## Invariants for pricing, retries, and audit trails

The first invariant is that a billing operation retains the rule version selected for it, even if a rollout percentage changes while a retry is pending. The second is that the financial write has an application-level idempotency identity, so another attempt at the same operation cannot create another adjustment. The third is that an operator can reconstruct who changed exposure, which cohort changed, and which operations used the resulting rule. Keep this change history in the release administration workflow rather than trying to infer it from a graph after the fact.

Exactly-once is an accounting objective, not an inference from a successful HTTP response.

An idempotency key on a telemetry write protects that write within the applicable deduplication contract. It does not establish exactly-once execution of a charge, an SMS send, and a telemetry submission as one transaction. Persist the business operation independently, reconcile its states, and assign different identities to genuinely different observations. In particular, reusing a key for an updated delivery status could hide the very transition the investigation needs to see.

There is also a compliance boundary. Delivery identifiers, property references, and pricing explanations should contain only the data necessary for reconciliation; keep tenant contact details out of metric labels, and define retention, deletion, access control, and regional processing requirements before selecting the storage destination. A region-specific flag cannot satisfy these requirements. Do not assume a telemetry store provides a per-user erasure interface, configurable retention, bulk export, or the audit evidence required by the applicable compliance regime; establish those contracts separately.

Alerting, distributed trace analysis, crash symbolication, session replay, and scheduled-job heartbeat checks are separate selection criteria. Designate systems for those responsibilities when they are required. A delivery-to-log handoff is not evidence that any of them has been covered.

## Critical path from SMS status to release evidence

The Go program below isolates the seam that a Node.js pricing backend must preserve: retrieve the status of an existing SMS, then put that response into a schema-shaped log request using the same base URL and bearer key. The implementation language is incidental to the HTTP contract. It deliberately starts with an existing message identifier, so running the example does not send another tenant notification.

Save it as `main.go`. Configure `API_BASE_URL` with the platform's versioned base URL, `INFRAI_API_KEY` with its bearer credential, and `SMS_ID` with an existing message identifier. Set `LOG_BODY_TEMPLATE` to a complete log-ingestion request constructed from the current discovery schema, containing the literal token `__SMS_STATUS_JSON__` inside one string value. That token is an application placeholder, not an API field. Include the billing-operation and pricing-rule identifiers in schema-supported application content. Set `OBSERVATION_ID` to a persisted identifier for this particular observation, preserving it when retrying the same write.

No request-body fields are guessed here. The complete envelope comes from the discovered schema; the program substitutes only the SMS response text. Run with `go run main.go` after configuring those inputs.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("missing environment variable: " + name)
	}
	return v
}

func request(client *http.Client, method, target, key, id string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if id != "" {
			req.Header.Set("Idempotency-Key", id)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			retryAfter := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
				if d := time.Duration(seconds) * time.Second; d > delay {
					delay = d
				}
			} else if at, err := http.ParseTime(retryAfter); err == nil {
				if d := time.Until(at); d > delay {
					delay = d
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	base := strings.TrimRight(required("API_BASE_URL"), "/")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		panic("API_BASE_URL must be an HTTPS base URL")
	}
	key := required("INFRAI_API_KEY")
	client := &http.Client{
		Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	status, err := request(client, "GET", base+"/sms/status/"+url.PathEscape(required("SMS_ID")), key, "", nil)
	if err != nil {
		panic(err)
	}
	if !json.Valid(status) {
		panic("SMS status response is not JSON")
	}
	template := required("LOG_BODY_TEMPLATE")
	if !json.Valid([]byte(template)) || strings.Count(template, "__SMS_STATUS_JSON__") != 1 {
		panic("log template must be valid JSON with exactly one status placeholder")
	}
	quoted, err := json.Marshal(string(status))
	if err != nil {
		panic(err)
	}
	body := []byte(strings.Replace(template, "__SMS_STATUS_JSON__", string(quoted[1:len(quoted)-1]), 1))
	if !json.Valid(body) {
		panic("status placeholder must occur inside a JSON string")
	}
	_, err = request(client, "POST", base+"/logs/ingest", key, required("OBSERVATION_ID"), body)
	if err != nil {
		panic(err)
	}
	fmt.Println("Delivery observation recorded; reconcile the pricing operation separately.")
}
```

The first response becomes content in the second request; it is not merely printed next to an unrelated telemetry call. Both requests use the same credential and base URL. The example checks status codes, reports error bodies, bounds attempts, honors `Retry-After`, and uses exponential backoff for rate limiting. Its 20-second request timeout and 4-attempt budget are illustrative application choices.

Transport uncertainty still requires reconciliation. If the process loses the response after a write, preserve the observation identifier for a later retry; do not manufacture a new identity simply because the network outcome is unknown. The platform's default idempotency deduplication window is 24 hours, so a permanent audit trail must have its own durable identity and retention policy.

Writing raw status JSON is useful for an initial adapter because it avoids guessing response fields. A production reducer should subsequently validate the discovered response schema and map recognized states into application-owned metrics. Keep the original observation available for authorized investigation, avoid contact details in that record, and do not translate an unknown state into delivered. That last mistake would improve the apparent success rate while degrading the signal used to authorize the next percentage increase.

## Rejected option and its valid use case

Reject automatic promotion based solely on SMS delivery success. It is an attractive shortcut because messaging produces visible activity, but notification retries and delayed receipts change that activity independently of pricing correctness; meanwhile, a perfectly delivered message can describe an incorrectly posted adjustment. For this release, progression requires reconciled business operations and an accountable exposure change.

Delivery success is a valid gate for a notification-template rollout whose acceptance criterion is delivery itself, provided the cohort, denominator, and observation window are defined. It is also useful supporting evidence in this pricing release. It simply answers a narrower question.

Choose dedicated experimentation tooling when the decision becomes a causal comparison of tenant behavior. Choose separate messaging and observability providers when their independent operational controls outweigh the integration work. For the bounded property-pricing rollout described here, begin off, test internally, increase regional percentages gradually, and let reconciliation determine whether the next increase is justified.

## References

- [OpenTelemetry, metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Twilio Messaging documentation](https://www.twilio.com/docs/messaging)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Sentry documentation](https://docs.sentry.io/)
- [Grafana documentation](https://grafana.com/docs/)
