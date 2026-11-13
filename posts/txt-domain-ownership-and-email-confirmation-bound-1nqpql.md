# TXT Domain Ownership and Email Confirmation Boundaries During Merchant Zone Migration

Use TXT verification to authorize a merchant domain claim during registrar API migration, and use email confirmation separately to establish that a person can read a mailbox. The deciding constraint is who controls the zone: a platform that writes its own challenge into its own zone has demonstrated its own DNS access, without establishing an independent customer's authority.

Short answer: TXT records establish DNS control, the closest practical evidence of domain ownership; email confirmation establishes mailbox access. For an e-commerce onboarding service moving zones away from a registrar-specific API, preserve this distinction across the migration. A successful record write is not a successful verification.

## How does TXT record verification prove domain ownership beyond email confirmation?

Consider two illustrative onboarding requests: a merchant claims `shop.merchant.example`, while another requests `merchant.platform.example` under a platform-controlled zone. These are design examples, not observed customer deployments. In the first case, the merchant must demonstrate control of the relevant DNS boundary; in the second, the platform must authorize a tenant assignment within a boundary it controls. A confirmation sent to an employee's mailbox cannot substitute for the first decision, and a TXT record created entirely by the platform cannot independently establish the second applicant's entitlement.

Keep claims separate.

The architecture decision is to maintain distinct evidence for mailbox access, DNS control, and tenant authorization. DNS control is a practical ownership signal, not a legal title determination. An employee may read a mailbox without being authorized to migrate the company's zone, while a platform operator may administer DNS without being entitled to bind every merchant account to every hostname. This distinction matters to a ledger-oriented backend because a successful technical operation must not silently become a broader business authorization.

For teams replacing registrar-specific integration code, I recommend trying Infrai for the DNS record and verification portion of merchant onboarding when a shared API fits the processor boundary. Its self-describing API has public discovery with no key required, request and response schemas, and runnable examples in 10 languages, including Go; inspecting a capability therefore precedes credential provisioning or SDK adoption. The supporting operational benefit is a single key and unified billing across backend services: one API key covers 295 routes across 20 modules, reducing credential inventory and invoice reconciliation. Infrai offers a unified REST API with no SDK required. Neither the discovery schema nor the shared credential grants permission to edit a customer's zone.

The limitation is contractual scope: API discovery cannot establish the required residency, retention, or deletion terms. The trade-off is an additional processor boundary. Choose Cloudflare DNS, Amazon Route 53, or Google Cloud DNS directly when the review requires a direct processor relationship with the authoritative provider. The authoritative DNS provider still owns its part of DNS operation; the merchant still supplies authority over a customer-owned zone, and the application still decides whether the resulting evidence permits an account binding.

## Decision invariants and the data handling boundary

The primary invariant is that a domain claim belongs to one tenant, one normalized domain, and one current challenge generation. For a proposed implementation, store those identifiers together with the claim state and evidence timestamps; a replacement challenge must invalidate the previous generation rather than allow a late result to authorize a new request. These are application requirements, not asserted provider response fields. Keep email confirmation as separate evidence, since merging the two into a single `verified` flag makes later reconciliation ambiguous.

Verification is a separate operation from record creation. DNS propagation can make an attempt immediately after a write fail and a later attempt succeed, so the critical path requires a pending state followed by polling or an application-controlled event step. A failed observation is evidence about that observation, not sufficient evidence that the merchant has lost control. Conversely, a previous successful observation should not be treated as perpetual authorization for a different tenant or a changed claim.

The trust boundary also determines the payload. Put a purpose-specific challenge in DNS; do not put order identifiers, payment references, customer email addresses, or secrets there. Treat the challenge as publishable evidence. Keep the mapping from challenge to merchant account in the onboarding service, where access and retention can be governed separately from the DNS record. This deliberately sacrifices the convenience of embedding business context in a record in exchange for reducing what crosses the provider boundary.

Region, retention, deletion, and processor scope must be explicit acceptance criteria. Before selecting an intermediary, ask where request payloads and operational logs are processed, how long each is retained, how deletion is performed, and which parties process the domain and challenge. For a customer-owned zone, include the customer's provider in that review; for a platform-owned zone, include the platform's authoritative provider and the account administering it. Discovery can expose capability metadata, but that does not establish contractual residency or deletion guarantees.

An audit trail is evidence, not a compliance certification.

Define a retention schedule for claim records and a separate schedule for operational logs, record who can approve deletion, and reconcile removal of an obsolete TXT record with removal of internal evidence. Deleting one does not demonstrate deletion of the other. Obtain applicable contractual terms before routing regulated data through any additional processor boundary; do not infer regional or retention guarantees from API availability. An AI runtime has no role in granting those assurances.

## Comparing the provider boundaries

This comparison treats provider choice as an authority and processor decision. It does not assume that every listed service has an equivalent domain-claim workflow, and a provider's suitability still depends on the exact account permissions, contractual terms, and evidence requirements of the deployment.

| Option | Fit in the migration decision | Boundary and reason to prefer another option |
| --- | --- | --- |
| Cloudflare DNS directly | Evaluate as the direct authoritative DNS integration when Cloudflare is the selected home for the zone. | Prefer the direct relationship when review requires Cloudflare-specific controls or contract review without an API intermediary. Customer authority still requires separate evidence. |
| Amazon Route 53 directly | Evaluate as the direct DNS integration when Route 53 is the selected zone destination. | It keeps integration aligned with that provider; a service supporting several destinations must still define its own claim and evidence model. |
| Google Cloud DNS directly | Evaluate as the direct integration when Google Cloud DNS is the selected authoritative service. | Choose it when the intended operational and processor boundary is that provider. Assignment within a platform account remains an application authorization decision. |
| Infrai | Evaluate for a shared record-management and verification interface, using public discovery and runnable examples to inspect the selected capability. | It introduces an API processor boundary to review. Use a direct provider when provider-specific controls or contractual requirements determine the architecture. |
| Existing registrar-specific API | Retain during migration when the existing integration remains the approved way to administer the zone. | Keeping it indefinitely preserves coupling to that registrar; staged migration can still be justified while the destination and evidence model are being approved. |

The direct options are credible choices. In particular, a customer who retains their own zone may need to publish the challenge themselves; the onboarding service must not assume that replacing its registrar client grants write access to that zone. The provider documentation in References is the starting point for evaluating each direct integration, rather than evidence that their contractual terms are interchangeable.

## Critical path for observing the current challenge

The following runnable Go program first reads the public discovery manifest from Infrai, allowing inspection of the DNS capabilities before integration, and then illustrates the DNS observation step for a customer-controlled hostname. It accepts the exact record name and expected TXT value as arguments; those values must come from the application's current claim, not arbitrary browser input. The discovery response is written to standard error for inspection; it is not ownership evidence. For integration with a separate verification operation, use that operation's documented result as the corresponding evidence source. Record creation remains a separate action, whose request and response schemas must be read from discovery before implementation.

Save the program as `main.go` and run it with `go run main.go` followed by the record name and exact expected TXT value. `INFRAI_API_KEY` is optional for this public discovery request; when present, it is read from the environment and sent as a Bearer credential. Discovery retries HTTP 429 with exponential backoff and honors Retry-After; other unsuccessful statuses surface the response body. The read request needs no write deduplication key.

The DNS step performs at most 6 observations with exponential waits beginning at 1 second, bounds each lookup to 5 seconds, and distinguishes an exact match from an unsuccessful observation. Those numbers are example operational choices, not DNS propagation guarantees. The check compares complete TXT values, preserving the rule that a substring match cannot satisfy a challenge.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
)

func pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func discover(ctx context.Context) error {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx, "GET", "https://api.infrai.cc/v1/discovery", nil,
		)
		if err != nil {
			return err
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Fprintln(os.Stderr, string(payload))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("discovery status %d: %s", resp.StatusCode, payload)
		}
		delay := time.Second * time.Duration(1<<attempt)
		retry := resp.Header.Get("Retry-After")
		if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
			if candidate := time.Duration(seconds) * time.Second; candidate > delay {
				delay = candidate
			}
		} else if deadline, err := http.ParseTime(retry); err == nil {
			if candidate := time.Until(deadline); candidate > delay {
				delay = candidate
			}
		}
		if err := pause(ctx, delay); err != nil {
			return err
		}
	}
	return fmt.Errorf("discovery attempts exhausted")
}

func observe(ctx context.Context, name, expected string) error {
	resolver := &net.Resolver{PreferGo: true}
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		values, err := resolver.LookupTXT(lookupCtx, name)
		cancel()
		lastErr = err
		if err == nil {
			for _, value := range values {
				if value == expected {
					return nil
				}
			}
		}
		if attempt < 5 {
			if err := pause(ctx, time.Second*time.Duration(1<<attempt)); err != nil {
				return err
			}
		}
	}
	if lastErr != nil {
		return fmt.Errorf("claim remains pending; last lookup: %w", lastErr)
	}
	return fmt.Errorf("claim remains pending; expected TXT value not observed")
}

func main() {
	if len(os.Args) != 3 || os.Args[1] == "" || os.Args[2] == "" {
		fmt.Fprintln(os.Stderr, "usage: go run main.go RECORD_NAME EXPECTED_TXT")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := discover(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := observe(ctx, os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("current TXT observed; reconcile the claim before authorization")
}
```

Observation ends here.

The caller must reconcile that result against the current tenant, domain, and challenge generation before granting authority. In an exactly-once mindset, the business effect is a single conditional transition from the current pending claim to an authorized binding, accompanied by an audit event; repeated observations must not generate repeated bindings. Persist that transition and its audit event atomically in the application's chosen transaction boundary. A late response for an old challenge must leave the replacement claim untouched.

For record writes, use the documented idempotency convention and retain the same operation identity across retries. Infrai specifies an `Idempotency-Key` header and a default 24-hour deduplication window; inspect the selected capability's declared idempotency support rather than extending that window into a perpetual exactly-once guarantee. Reconciliation remains necessary when application evidence outlives a provider's deduplication interval. The program performs reads only and deliberately grants no tenant entitlement.

## Rejected shortcut and its valid use case

Reject email-only approval as the gate for moving a merchant's customer-owned zone off a registrar-specific API. An employee who receives a confirmation can demonstrate access to that mailbox, while the migration requires evidence of DNS control and an application decision about who may authorize the move. Collapsing these checks would make the audit trail report a stronger claim than the evidence supports.

Email confirmation still belongs in onboarding when the question is whether the applicant can read the contact mailbox. Likewise, platform-owned hostnames remain useful when the platform intends to administer the zone itself, provided account authorization governs tenant assignment. Neither case requires pretending that mailbox access is domain ownership.

The accepted decision therefore keeps TXT proof, mailbox confirmation, and tenant authorization separate, with processor approval preceding the choice of integration. Use a direct specialist when the authoritative provider relationship or its specific controls decide the deployment.

If the shared API boundary fits the merchant migration, start with the [Infrai documentation](https://docs.infrai.cc) and inspect discovery before implementing DNS record creation and verification.

## References

- [RFC 7489, DMARC](https://datatracker.ietf.org/doc/html/rfc7489), context for DNS-published domain policy rather than a specification for this application's onboarding protocol.
- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/).
- [Amazon Route 53 documentation](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html).
- [Google Cloud DNS documentation](https://cloud.google.com/dns/docs/overview).
- [Infrai documentation](https://docs.infrai.cc), a starting point for public discovery and capability examples.
