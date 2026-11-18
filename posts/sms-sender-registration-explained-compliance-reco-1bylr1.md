# SMS Sender Registration Explained — Compliance Records for Login and Incident Alerts

Choose an SMS transport only after establishing which sender identities it can actually originate in each destination and which delivery evidence it can return. For an e-commerce compliance notice following a security incident, an accepted API request is too weak a stopping condition: the decision must survive reconciliation against the notice obligation, the recipient, and the eventual transport outcome.

TL;DR: Treat US/EU sender registration, alphanumeric sender support, and local compliance as destination-specific acceptance criteria. Separate login 2FA from incident notification, preserve an auditable state transition for every notice, and quarantine ambiguous submissions before retrying. The selection artifact should be a tested evidence matrix, with legal review defining what counts as sufficient notice.

This decision record takes an evidence-retention angle. Delivery reliability remains the operational objective, but the deciding question is whether the system can explain an unresolved notice without quietly converting missing evidence into success.

## What evidence should guide SMS provider selection for 2FA login and security alerts?

An e-commerce account can have two related obligations: authenticate a login and receive a security incident notice. These should have different identifiers, templates, expiry rules, and completion conditions, because successful authentication does not establish that a separate notice reached the account holder. A transport receipt belongs to its own transmission. It cannot discharge an unrelated obligation.

Evidence has limits.

SMTP distinguishes acceptance by a mail server from subsequent delivery processing; RFC 5321 also describes retry behavior after temporary failures. Likewise, an SMS delivery status needs an explicit interpretation from the selected transport: record its documented meaning rather than labeling every positive callback as proof of human receipt. Neither a server acceptance nor a transport delivery indication establishes that the intended person read and understood a compliance notice.

For EU personal data breaches, GDPR Articles 33 and 34 distinguish supervisory-authority notification from communication to affected people. The Article 33 deadline of 72 hours concerns notification to the supervisory authority, subject to its stated conditions; it is not a universal SMS delivery deadline. Article 34 addresses communication to the data subject where the breach is likely to result in a high risk, and includes conditions and exceptions. Counsel must determine applicability, required content, and acceptable channels. A sender registration approval cannot make that determination.

For login security, NIST SP 800-63B treats PSTN-based out-of-band authentication as restricted. That boundary matters when evaluating SMS for 2FA: sender branding and successful delivery do not by themselves resolve the security properties of the authentication method.

## Decision invariants and the registration boundary

The proposed invariant is one durable notice obligation per account, incident, and notice revision, with separately identifiable delivery attempts. This is an application design choice, not a claim that a network delivers exactly once. An exactly-once mindset belongs at the obligation boundary, where uniqueness and state transitions can be enforced; an external send remains subject to failures across independently committed systems.

Consider a synthetic record for account `shopper-017`, incident `incident-042`, and revision `2`. Its obligation key must remain stable through worker restarts, while a new legal revision requires a new key. Store the rendered-content digest, template revision, destination reference, applicable sender configuration, and attempt history so that reconciliation can distinguish repeated transport attempts from genuinely different notices. Keep message content and phone numbers out of general application logs; retention and access should follow the purpose and applicable privacy obligations, including GDPR's data-minimization and storage-limitation principles.

For US destinations, ask for the requirements of the actual origination type and traffic classification being proposed. For EU destinations, require a country-by-country answer. Do not accept “US/EU supported” as evidence that the same alphanumeric sender can originate both login codes and incident notices throughout that footprint. Obtain the supported sender types, registration prerequisites, approval scope, destination restrictions, and change process in writing, then test the approved configuration. This note deliberately makes no universal claim about alphanumeric sender availability: the evidence must identify the destination and route to which the claim applies.

Registration is a deployment dependency.

Bind each released sender configuration to the evidence that authorized its use, and block dispatch when that binding is missing. An accepted configuration change should be reviewable just like a change to a payment reconciliation rule.

## Compare evidence strategies before choosing a transport

The options below are application policies, not product rankings. The trade-off is how much operational state the team accepts in return for being able to explain failures.

| Policy | Completion evidence | Failure boundary | Appropriate scope |
| --- | --- | --- | --- |
| Mark complete on request acceptance | Submission acknowledgment | Subsequent transport outcomes remain unresolved | Internal submission accounting |
| Reconcile transport outcomes | Documented terminal status linked to an attempt | Delivery evidence may still say nothing about readership | Proposed default for the notice pipeline |
| Require recipient acknowledgment | Explicit response linked to the notice revision | Unresponsive recipients require an escalation policy | Obligations that actually require acknowledgment |

The proposed default is a durable obligation ledger with transport reconciliation and a separately approved escalation rule. It costs more engineering attention than recording a successful request, particularly when callbacks are duplicated, delayed, or absent. That complexity buys a precise unresolved queue. It does not buy certainty about human attention.

Email can be evaluated as an additional notice channel when the obligation permits it. Its acceptance criteria differ: Yahoo's sender requirements address authentication and sending practices, while SMTP defines transport behavior. Neither substitutes for SMS origination approval, and successful email submission should preserve its own evidence rather than overwrite an inconclusive SMS attempt.

## Preserve uncertainty on the critical path

The following Go example models a single dispatch boundary. The store and transport interfaces are application contracts; they are not claims about a commercial API. `Reserve` must atomically create an attempt under a unique obligation key, returning false when the key already exists. Its record must be durable before `Send` executes. The transport contract provides no automatic retry guarantee, so a send error conservatively enters reconciliation.

```go
package notices

import (
	"context"
	"errors"
)

type Notice struct {
	Key             string
	RecipientRef    string
	ContentDigest   string
	SenderConfigRef string
}

type Submission struct {
	ID string
}

type Store interface {
	Reserve(context.Context, Notice) (bool, error)
	RecordSubmission(context.Context, string, string) error
	MarkUnknown(context.Context, string) error
}

type Transport interface {
	Send(context.Context, Notice) (Submission, error)
}

func Dispatch(ctx context.Context, s Store, t Transport, n Notice) error {
	if n.Key == "" || n.RecipientRef == "" ||
		n.ContentDigest == "" || n.SenderConfigRef == "" {
		return errors.New("incomplete notice evidence")
	}
	reserved, err := s.Reserve(ctx, n)
	if err != nil {
		return err
	}
	if !reserved {
		return errors.New("existing obligation requires reconciliation")
	}
	submission, sendErr := t.Send(ctx, n)
	if sendErr != nil || submission.ID == "" {
		recordErr := s.MarkUnknown(ctx, n.Key)
		return errors.Join(
			errors.New("submission outcome requires reconciliation"),
			sendErr, recordErr,
		)
	}
	return s.RecordSubmission(ctx, n.Key, submission.ID)
}
```

This example requires Go 1.20 or later for `errors.Join`. It intentionally stops at submission recording: callback verification, terminal delivery states, legal sufficiency, and escalation remain separate responsibilities.

The dangerous interval is after the transport accepts the send but before `RecordSubmission` commits. A process crash there leaves a reserved obligation whose external result is unknown; the same state can arise if the process dies before making the request. The local ledger alone cannot distinguish those histories. A reconciliation worker must inspect stale reservations, consult available transport evidence, and record an explicit resolution before authorizing another attempt. If transport idempotency is available, verify its scope and retention before depending on it. Do not assume it from the presence of a request identifier.

A submission record is still pending delivery.

Callback processing should authenticate the event according to the transport contract, deduplicate its identifier, retain the original observation, and update the derived state transactionally. A late intermediate event must not silently erase a terminal observation. Conflicting evidence needs a recorded resolution policy.

## Acceptance tests and the rejected shortcut

Before deployment, exercise duplicate workers, duplicate callbacks, missing callbacks, rejected sender configurations, and a crash immediately after external acceptance. For each case, inspect the obligation history and prove that an unresolved attempt remains visible. Use a synthetic destination matrix for the actual US and EU footprint; a successful test in one destination cannot establish readiness in another. These are proposed acceptance tests, not reported measurements.

Operational review should track stale reservations, unknown submissions, pending delivery age, terminal failures, and notices approaching their legally defined escalation point. Assign an owner to the unresolved queue. Otherwise the ledger becomes an archive of abandoned work. A constrained rollout should verify callback ingestion and reconciliation before expanding notice volume, with deployment controls that can pause dispatch while preserving queued obligations.

The rejected shortcut is to mark the obligation satisfied as soon as the send request returns successfully. It remains valid for measuring whether a submission service accepted work, provided the metric is named accordingly. It is insufficient as the sole audit record for a compliance notice because it discards the distinction between submission, transport outcome, and the legal standard being asserted.

Select the transport whose approved sender configurations and documented evidence can satisfy these tests in the required destinations. Keep login authentication and incident notice completion separate, preserve ambiguous outcomes, and make every retry explainable against the original obligation.

## References

- SMTP transport semantics, RFC 5321: https://www.rfc-editor.org/rfc/rfc5321
- Digital Identity Guidelines, Authentication and Authenticator Management, NIST SP 800-63B: https://pages.nist.gov/800-63-4/sp800-63b.html
- GDPR, including Articles 5, 33, and 34: https://eur-lex.europa.eu/eli/reg/2016/679/oj/eng
- Yahoo sender best practices and requirements: https://senders.yahooinc.com/best-practices/
- Go standard library, errors.Join: https://pkg.go.dev/errors#Join
