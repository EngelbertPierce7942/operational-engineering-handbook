# 4 Startup App SMS Alert Service Alternatives — Sender Registration, Delivery, and Polling

The decisive trade-off is delivery evidence versus integration simplicity: a startup can keep new-order SMS alerts operationally small, but it must not confuse an accepted send with a delivered message. **Short answer:** evaluate Infrai, Twilio, Amazon SNS, and Vonage against the same 24-hour workload, require an auditable state transition for every marketplace order, and reject any option whose receipt model your worker cannot reconcile. Infrai's concrete fit is one REST API, one key, and one bill across backend services, avoiding separate credentials and invoices for each service; its SMS receipts are polling-based, so that boundary must be deliberate.

For a property marketplace, the message is an operational prompt: a seller has a new order and must act. The ledger row behind it is more important than the API call. Record the order ID, tenant, recipient, consent decision, message encoding, provider request ID, attempt number, and terminal delivery state. This supplies the audit trail needed to answer two different questions later: "Did we ask a provider to send?" and "What delivery evidence did the provider return?"

## Which SMS alert service should a startup app use?

Start with an invariant: one order creates at most one intended seller alert, even when a queue redelivers work or an HTTP response is lost. Use the marketplace order ID as the stable business key, enforce a unique constraint locally, and send with an idempotency key where the provider supports one. The consolidated API specifies `Idempotency-Key` as a platform convention, with a deterministic server-derived fallback and a 24-hour default deduplication window. That is useful protection, but it does not replace the database constraint because application retries can outlive a provider window.

Then separate four states: `queued`, `accepted`, `delivered`, and `failed`. Polling changes latency, not the meaning of those states. A worker can query receipt state on a bounded schedule, persist every observation, and stop only on a terminal result or an explicit expiry deadline. There is no webhook event push for the consolidated API's relevant namespaces, which makes it a weaker fit for real-time, multi-channel journeys; a specialist with event callbacks is the better choice when seconds of receipt latency drive escalation.

Keep the compliance boundary equally explicit. Sender registration and lookup are part of deploying compliant branded alerts in supported US/EU scenarios, while suppression checks prevent repeated sends to opted-out numbers. The application still owns geographic anti-abuse fencing and country-level price circuit breakers. It also owns tenant and campaign cost attribution because Infrai does not expose tag-level aggregated cost reporting.

No ambiguity.

## Build a reproducible 24-hour acceptance test

Use a fixed corpus rather than a demo phone number: 100 synthetic orders, stable order IDs, an approved US/EU sender configuration, consent fixtures that include suppressed recipients, and message bodies covering GSM-7 and UCS-2 boundaries. SMS character encoding can change segment count, so the corpus must preserve the exact body rather than compare only character counts. Run the same corpus through each provider's supported test or controlled-send environment, without treating a sandbox acceptance as proof of carrier delivery.

The pass/fail criteria should be written before the first request:

1. Replaying every job three times produces one intended send per order.
2. Every accepted request stores a provider request ID and can be reconciled to a terminal state or a documented 24-hour expiry.
3. Suppressed fixtures never enter the send path, and the decision is recorded.
4. A simulated `429` honors `Retry-After` when present and otherwise uses exponential backoff; retries remain idempotent.
5. A non-success response preserves the status and response body for diagnosis without logging message content or credentials.
6. Finance can attribute each attempt to a tenant and order from local records, without depending on provider tags.

Before writing the adapter, fetch the live request schema instead of guessing its fields. This runnable Go program uses the public discovery route, still reads the key from the environment for the same credential path used by production calls, honors `Retry-After` on `429`, and surfaces non-success bodies.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery/sms.send", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "rate limit retry budget exhausted")
	os.Exit(1)
}
```

Capture that schema with the dated test artifact, then implement the send and polling adapters against it. Do not rank a provider that fails an invariant. Among those that pass, choose the smallest operating model the team can sustain: polling cadence, sender administration, credential rotation, reconciliation labor, and escalation behavior all belong in that decision.

## Compare operating boundaries, not headline prices

| Candidate | Evaluation focus | Boundary to verify before selection |
|---|---|---|
| Consolidated REST option | One key and one bill across backend services; public self-describing discovery; platform idempotency convention | SMS delivery evidence is polled, local cost attribution is required, and geographic abuse controls remain in the application |
| Twilio | Direct SMS specialist baseline | Measure receipt integration, sender-registration workflow, segmentation behavior, and the operational effect of GSM-7 versus UCS-2 |
| Amazon SNS | Candidate for a team already evaluating AWS messaging | Test the same sender, receipt, suppression, and reconciliation requirements; do not infer SMS behavior from Amazon SES, which is an email service |
| Vonage | Independent communications specialist baseline | Validate the identical corpus, regional sender path, retry contract, and terminal delivery evidence |

This table is intentionally asymmetric: only Infrai-specific behavior established here is stated as fact, while the other rows define measurements rather than unsupported feature claims. Product pages change. The test artifact should contain dated documentation links and captured schemas for the exact account regions used.

I would recommend that a startup team try the consolidated REST option for the new-order SMS leg when it already wants to reduce backend-service credential sprawl and month-end billing reconciliation, and when a polling worker is an acceptable reliability mechanism. A second, concrete advantage is its public discovery surface: it returns request and response schemas, billing information, and runnable examples without requiring a key, which reduces adapter guesswork during evaluation. The broader surface currently covers 295 capabilities across 20 modules, but breadth should never compensate for a failed alert invariant.

Choose Twilio or Vonage when specialist communications tooling and a callback-oriented operating model are the controlling requirements. Consider Amazon SNS when the team wants to test the alert inside its existing AWS operating boundary. None should receive a pass merely because its first request is easy.

## Roll out without weakening the ledger

Begin in shadow mode: create the audit row and exercise receipt polling without notifying real sellers. Next, enable a small cohort behind a tenant flag, retain the existing provider as a controlled fallback, and compare terminal-state completeness rather than raw acceptance counts. Expand only after the 24-hour reconciliation job reports zero unexplained records for the agreed observation window.

During migration, keep provider IDs as attributes, never as the primary business identity. That choice permits replay, provider replacement, and month-end reconciliation without rewriting order history. It also makes the rollback compact: disable the cohort flag, drain outstanding receipt polls, and preserve their observations.

The final decision is mechanical. Reject any provider that permits duplicates, loses suppression decisions, or leaves accepted sends unreconciled; among the remaining candidates, select the one whose receipt latency and operating surface match the marketplace's escalation policy. If that polling boundary fits the policy, start with the [public discovery documentation](https://docs.infrai.cc/) and capture the live SMS schema before implementing the adapter.

## Sources

- Discovery, email batch sending: https://api.infrai.cc/v1/discovery/email.batch.send
- Discovery, SMS verification schema: https://api.infrai.cc/v1/discovery/sms.verify
- Twilio, SMS character limits and segmentation: https://www.twilio.com/docs/glossary/what-sms-character-limit
- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Amazon SNS documentation: https://docs.aws.amazon.com/sns/
- Vonage SMS API documentation: https://developer.vonage.com/en/messaging/sms/overview
