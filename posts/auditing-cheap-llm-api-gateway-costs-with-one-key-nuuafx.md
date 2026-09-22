# Auditing Cheap LLM API Gateway Costs with One Key (For Moderation Portability)

Short answer: choose a gateway only after proving that the same moderation report can be replayed through a second provider adapter without changing the durable decision contract. A cheap request is irrelevant if a retry creates two review tasks, a cached answer crosses a policy boundary, or a regional restriction cannot be demonstrated from the audit record. For marketplace report classification, I would put an idempotent decision ledger in front of every provider, estimate usage before dispatch, and treat routing, caching, and batching as policy-controlled optimizations behind that ledger.

This is the decision rule: **portability is the ability to reproduce and reconcile an outcome, not merely the ability to rename a model field.** One credential and one request shape are convenient, but they do not establish equivalent classification behavior, regional processing, token accounting, or retry semantics. The durable system owns those invariants. The gateway remains replaceable.

Cheap is not enough.

## What must remain true when a provider changes?

A moderation report arrives with a marketplace-scoped identifier, policy version, evidence digest, and an allowed processing region. The classifier may suggest a queue and severity, but it does not perform the enforcement action. Human review remains a separately authorized transition. That boundary limits the damage from malformed output and makes a replay observable rather than destructive.

Five invariants belong in the architecture decision record. First, `(marketplace, report_id, policy_version)` identifies one classification attempt from the business perspective. Second, the stored request uses a provider-neutral schema and immutable evidence references. Third, every accepted result records the adapter version, route policy version, input estimate, provider-reported usage when available, cache disposition, and a digest of the raw response. Fourth, a retry may call a provider more than once, but it may commit only one current result for an idempotency key. Fifth, regional eligibility is checked before network dispatch; it is not inferred afterward from a hostname or invoice.

Exactly once is an outcome property here.

No general network call becomes exactly once because a gateway accepts an idempotency header. A connection can disappear after the remote service accepts work and before the caller receives a response. The safe design permits at-least-once execution around an idempotent commit, preserves late responses as superseded audit events, and makes reconciliation a normal operation. The pattern is familiar from ledger backends: identify intent, record attempts, commit once, and retain enough evidence to explain the state later.

The failure boundary is narrow. Provider timeouts, invalid structured output, exhausted route capacity, and region-policy rejection stop before a review task is published. A classifier result that passes schema validation is still only a proposal. If publication fails, an outbox row remains pending beside the committed decision, so replay does not classify the report again merely to recover message delivery.

## How should one cheap LLM API gateway key control moderation costs?

A price table ages quickly and obscures the engineering question. The useful comparison asks where each control lives and what evidence survives a dispute. Token estimates still matter, but as admission-control inputs and reconciliation signals rather than promises of a final bill. Different accounting rules make an estimate unsuitable as a universal debit amount. Store the estimate, store returned usage separately, and investigate the delta by route and request class.

| Option | Portable contract | Cache boundary | Batch failure unit | Regional evidence | Operational trade-off |
|---|---|---|---|---|---|
| Direct provider adapters | Application-owned | Application-defined | Application-defined | Policy plus provider contract | More adapter work; least intermediary coupling |
| Hosted multi-provider gateway | Common surface with escape hatches | Gateway and application policies | Defined by its contract | Documented route plus retained evidence | Faster integration; another policy and failure domain |
| Self-operated routing layer | Team-owned | Application-defined | Application-defined | Inspectable deployment and egress controls | Highest operational load; maximum record control |

The table does not produce a universal winner. It produces verification work. For a hosted route, ask whether cache keys include tenant, policy version, normalized prompt, capability class, and region policy. For a self-operated route, ask who patches adapters, rotates credentials, and investigates accounting drift. For direct adapters, ask whether every integration emits the same audit envelope or provider-specific fields have leaked into downstream consumers.

A team may intend to route work among OpenAI, Claude, and Gemini, then later add or remove a provider. Those names describe the migration set; they do not change the invariant. Test 2 independent adapters before approval, and keep provider-specific options inside each adapter rather than promoting them into the moderation contract. The trade-off is explicit: a lowest-common-denominator schema eases movement but can suppress useful capabilities, while escape hatches preserve capabilities but create coupling. I would keep the durable result narrow and let an attempt record retain adapter-specific evidence, because auditability matters after the routing choice has changed.

Caching deserves special suspicion in moderation. Two reports with identical text may belong to different marketplaces, policy versions, or evidence contexts. A cache hit is valid only if every input that can change the classification is represented in the key and the retention policy permits reuse. Cache the validated classification envelope, not the side effect of creating a human-review task.

Batching changes the unit of recovery. A large provider batch may improve throughput, but the application still needs an item-level idempotency key and status. Otherwise one malformed item or ambiguous response turns a bounded retry into a bulk replay. Admit reports individually, assemble only region- and policy-homogeneous batches, and reconcile each returned item against the ledger before publication. The batch identifier is correlation data, never the business key.

That is the trap.

## The critical path commits evidence before effects

This Go sketch omits transport-specific fields on purpose. Adapters translate the stable envelope, while the repository and outbox enforce the business outcome. `InsertAttempt` precedes the remote call; `CommitDecisionAndEvent` is one database transaction with a uniqueness constraint on the idempotency key.

```go
type ClassifyRequest struct {
    MarketplaceID string
    ReportID      string
    PolicyVersion string
    EvidenceHash  string
    AllowedRegion string
}

type Classification struct {
    Queue      string
    Severity   string
    Confidence float64
}

type Adapter interface {
    Estimate(context.Context, ClassifyRequest) (Usage, error)
    Classify(context.Context, ClassifyRequest) (Classification, Usage, []byte, error)
}

func (s *Service) Classify(ctx context.Context, req ClassifyRequest) (Classification, error) {
    key := sha256.Sum256([]byte(strings.Join([]string{
        req.MarketplaceID, req.ReportID, req.PolicyVersion, req.EvidenceHash,
    }, "\x00")))
    if prior, ok := s.repo.Current(ctx, key); ok {
        return prior, nil
    }

    adapter, route, err := s.routes.Select(req.AllowedRegion)
    if err != nil {
        return Classification{}, err
    }
    estimate, err := adapter.Estimate(ctx, req)
    if err != nil {
        return Classification{}, err
    }
    attemptID, err := s.repo.InsertAttempt(ctx, key, route, estimate)
    if err != nil {
        return Classification{}, err
    }

    result, actual, raw, err := adapter.Classify(ctx, req)
    if err != nil {
        s.repo.RecordFailure(ctx, attemptID, err)
        return Classification{}, err
    }
    if err := validate(result); err != nil {
        s.repo.RecordFailure(ctx, attemptID, err)
        return Classification{}, err
    }

    digest := sha256.Sum256(raw)
    committed, err := s.repo.CommitDecisionAndEvent(ctx, key, attemptID, result, actual, digest)
    if errors.Is(err, ErrAlreadyCommitted) {
        return s.repo.MustCurrent(ctx, key)
    }
    return committed, err
}
```

Production code should classify errors instead of persisting unrestricted error strings, bound raw-response retention, encrypt sensitive evidence, and propagate cancellation. Those are data-governance decisions, not adapter conveniences. The evidence digest participates in the idempotency key for a reason: if evidence changes under the same report and policy version, the system creates a new decision lineage rather than returning a classification of stale material.

Streaming does not belong on this classification critical path. Server-Sent Events provide a one-way server-to-client stream and use the `text/event-stream` media type; they can show review-console progress, but UI delivery is not proof that a decision committed. The browser may reconnect. The ledger remains authoritative, and a reconnect reads durable state before subscribing to later events.

## How should portability be tested before approval?

Run conformance fixtures through at least two adapters, but do not demand identical prose or usage counts. Assert the contract the business consumes: schema validity, allowed queue values, severity bounds, policy-version echo, region-policy acceptance, and complete audit metadata. Include reports with empty evidence, oversized evidence references, duplicated identifiers, mixed-language text, and content that attempts to override classifier instructions.

Then inject ambiguity. Time out after dispatch, deliver the same job twice, return a valid response after the route deadline, fail the outbox publisher, and replay a batch containing one invalid item. The expected result is boring: one current decision per idempotency key, every attempt visible, no duplicate review task, and no dispatch outside the allowed route set.

Run all 5 failure injections.

Provider portability also needs a shadow mode. Send a sampled, policy-approved copy to a candidate adapter without allowing it to publish effects; compare normalized classifications by policy category, route eligibility, latency class, estimated-versus-reported usage, and validation failures. The acceptance threshold must be chosen by the moderation owner before results are seen. There is no factual basis for a universal percentage.

Audit the audit trail. Pick a committed review task and reconstruct which immutable evidence was classified, which policy selected the route, what estimate admitted the request, which adapter produced the accepted response, whether caching was involved, and which transaction published the task. If any link requires transient logs, the design is not ready for a contractually restricted workflow. Logs help investigation; they are not the system of record.

## Rejected option: a universal request proxy

I would reject a thin proxy that forwards one nominally common request and returns provider-shaped JSON directly to the application. It reduces initial wiring, yet leaves the expensive coupling untouched: downstream code learns provider response fields, retries lack a durable intent record, cache scope is implicit, and regional eligibility is an operational convention rather than a checked policy. Adding a second upstream later becomes a data migration and reconciliation project, not an adapter change.

The rejected design has a valid use case. For stateless experimentation where outputs have no durable effects, data is approved for every configured route, and replay does not create business harm, a thin proxy can be proportionate. Marketplace moderation before human review does have workflow effects and audit obligations, so the stronger boundary earns its complexity.

The final selection should follow a recovery exercise, not a feature checklist: disable the preferred route after dispatch, replay the report through another adapter, reconcile both attempts, and prove that only one review task becomes current. If the system can do that while preserving region policy and usage evidence, the gateway is replaceable. If it cannot, one key has merely concentrated the dependency.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
