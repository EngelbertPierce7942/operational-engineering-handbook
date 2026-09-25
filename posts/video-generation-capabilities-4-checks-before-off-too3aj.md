# Video Generation Capabilities: 4 Checks Before Offering Users Costly Options

TL;DR: expose a promo-video option only when a server-side capability snapshot says that the renderer accepts the source image format, can produce the requested output class, and remains inside explicit storage and cache limits. In a Node.js Express application, treat that snapshot as a versioned contract rather than asking the browser to guess from a file extension. Reject unsupported combinations before a render job exists, record the decision inputs, and make retries return the same decision.

For a gaming storefront, this keeps a 15-second character trailer from becoming an expensive experiment in duplicated originals, intermediate frames, and cache variants. The governing rule is simple: no capability evidence, no option.

## 1. How should Node.js check video generation capabilities before offering options?

The interface should promise only a validated output class, never an implementation detail. A choice such as `store-preview` can stand for a fixed aspect ratio, duration ceiling, and accepted input family in an internal policy document; the client does not need to know which encoder or worker will fulfill it. The capability response should also carry a revision and an expiry time, because a decision without its policy version cannot be reconstructed during reconciliation.

Image format deserves an explicit gate. MDN's image-format guide distinguishes raster formats such as JPEG, PNG, WebP, and AVIF and documents that their feature sets differ. That is enough reason to inspect the actual media type and decoded metadata at ingestion instead of treating `.png` or `.jpg` as proof. For game art, alpha support may affect compositing, while animation in a source asset may require a deliberate flatten-or-reject policy. Those are product rules built on observable properties, not guesses made by the route handler.

The public Express route can return a small, stable description: an opaque snapshot ID, the allowed option IDs, and denial reasons intended for display. Keep raw worker flags private. Otherwise, every worker deployment becomes an accidental front-end API change.

That boundary matters.

## 2. Four gates define the capability snapshot

1. **Input gate.** Decode enough of the uploaded or registered image to establish its media type, dimensions, alpha requirement, and animation status. Compare those properties with a versioned allowlist. A filename is advisory data only.

2. **Render gate.** Resolve the requested trailer class against a policy revision and the currently eligible worker class. This is where duration, aspect ratio, and output container constraints belong. The result must be a named option or a precise denial, not a loose collection of booleans that the client has to interpret.

3. **Cost gate.** Estimate retained bytes by lifecycle: original, normalized image, temporary render material, final video, and cache derivatives. Use conservative upper bounds, then deny or narrow the available choices when the account or title budget would be exceeded. Price is deliberately absent; storage and cache pressure can be governed in bytes even when providers and contracts change.

4. **Commit gate.** Bind the accepted option, asset digest, policy revision, and capability snapshot to an idempotency key before enqueueing work. Repeating the same request must resolve to the same job identity or a conflict when its payload differs. The audit record should preserve both approvals and denials, since a missing job is otherwise indistinguishable from a request that was never made.

These gates are sequential. A cost estimate based on an image that cannot be decoded is false precision, and a render job created before the idempotency record exists leaves a duplicate-work window. Exactly-once execution is not a realistic assumption across a queue and a worker; an exactly-once business effect is the useful target, enforced through stable keys, conditional state transitions, and reconciliation.

Consider the concrete failure sequence. A player uploads one animated source image, opens two browser tabs, and selects the same 15-second trailer option in both; each tab times out after submission and retries. If capability checking, job creation, and queue publication are separate unguarded steps, the system can retain two normalized images, create four job rows, and publish more than once without being able to explain which visible choice authorized which work. The numbers here describe the request sequence, not a performance benchmark. A snapshot tied to the asset digest makes the capability decision repeatable, while a payload-bound idempotency key reduces the four submissions to one business operation; an outbox and guarded worker transition then make repeated delivery harmless. The trade-off is extra state and a reconciliation loop, accepted because duplicate media work directly expands storage and cache occupancy.

## 3. Compare the decision boundaries once

| Boundary | Evidence used | Failure behavior | Audit field |
|---|---|---|---|
| Browser-only filtering | File name and client-declared type | Hide the choice, but never authorize work | None; advisory only |
| Request-time inspection | Decoded image metadata and current policy | Return no eligible option before enqueue | Asset digest and policy revision |
| Worker-time discovery | Worker observation after dequeue | Mark the attempt rejected and retain no derivatives | Job ID, attempt ID, and reason |
| Precomputed capability snapshot | Inspected asset plus versioned render and cost policy | Offer only choices valid for the snapshot lifetime | Snapshot ID, expiry, and inputs |

The selected design combines request-time inspection with a precomputed snapshot. Browser filtering remains useful for responsiveness, but it cannot be the authority because requests can bypass the interface. Worker-time discovery remains a defensive check, not the primary path; finding an ordinary incompatibility after enqueue spends queue capacity and complicates the user's mental model.

The snapshot is immutable. When policy changes, issue a new one rather than mutating the meaning of an old ID. A render submission referencing an expired snapshot should obtain a fresh decision before it can create work. This adds one read to the path, but it gives reconciliation a crisp question: did the committed job match the evidence that produced the visible option?

There is a limitation: the snapshot can become stale between inspection and submission, so it narrows uncertainty rather than eliminating it.

## 4. Put the critical path behind one transaction

The following Go sketch shows the service boundary behind the Express route. Express can authenticate, parse the request, and map this result to its response; the authority remains in a service that performs one conditional commit. Interfaces keep the example independent of a database, queue, or media library.

```go
package render

import (
    "context"
    "errors"
    "time"
)

type SubmitRequest struct {
    AccountID      string
    AssetDigest    string
    OptionID       string
    SnapshotID     string
    IdempotencyKey string
}

type Snapshot struct {
    ID             string
    AccountID      string
    AssetDigest    string
    PolicyRevision string
    AllowedOptions map[string]bool
    ExpiresAt      time.Time
}

type Job struct {
    ID             string
    SnapshotID     string
    PolicyRevision string
}

type Store interface {
    LoadSnapshot(context.Context, string) (Snapshot, error)
    CommitOnce(context.Context, SubmitRequest, Snapshot) (Job, error)
}

var (
    ErrStaleSnapshot = errors.New("capability snapshot is stale")
    ErrUnsupported   = errors.New("render option is unsupported")
)

func Submit(ctx context.Context, now time.Time, db Store, req SubmitRequest) (Job, error) {
    snap, err := db.LoadSnapshot(ctx, req.SnapshotID)
    if err != nil {
        return Job{}, err
    }
    if snap.AccountID != req.AccountID || snap.AssetDigest != req.AssetDigest || !now.Before(snap.ExpiresAt) {
        return Job{}, ErrStaleSnapshot
    }
    if !snap.AllowedOptions[req.OptionID] {
        return Job{}, ErrUnsupported
    }

    // CommitOnce atomically records the key, payload identity, audit fields, and outbox event.
    return db.CommitOnce(ctx, req, snap)
}
```

`CommitOnce` is the hard boundary. Its unique key should be scoped to the caller and operation, while its stored payload identity prevents the same key from silently authorizing a different asset or option. The durable transaction records a pending job and an outbox event together; a dispatcher may publish more than once, so the worker also applies a conditional transition before rendering. These are design invariants, not assurances supplied by a transport.

Observe decisions as a ledger. Count denials by stable reason, track snapshot age at submission, compare estimated retained bytes with measured retained bytes after completion, and alert on jobs that remain between committed and terminal states. Do not place file names, user text, or signed asset locations in metric labels. The audit record can retain controlled identifiers while operational metrics stay bounded.

The reconciliation loop should scan nonterminal jobs, verify their outbox and worker state, and repair only through the same guarded transitions used by the live path. It must never manufacture a second job merely because a response was lost. Short request timeouts are normal; ambiguous commits are the case the idempotency lookup exists to resolve.

## 5. Record the rejected shortcut and its valid use

The rejected design is a static option list embedded in the Express application. It is appealing because it removes a read and makes the interface deterministic, but it couples user-visible promises to assumptions about assets, workers, and retention policy. A deployment between option display and submission can then produce a choice that no longer has supporting evidence, while multiple application instances may disagree during rollout.

The snapshot design is also not suitable for every system. It costs a durable write or cache entry per decision, requires expiry semantics, and adds operational work for reconciliation. For a synchronous, disposable preview that retains no input or output and exposes no external choice, direct validation in the render call is the smaller design.

There is a valid use case: a closed internal tool with one controlled input profile, one renderer configuration, and no user-triggered persistent jobs can use a static list as a convenience filter. Even there, the worker should validate its input. Once the system accepts arbitrary game artwork, retains generated trailers, or exposes choices to external users, the versioned snapshot earns its complexity because it makes authorization, cost control, and audit reconstruction the same decision rather than three loosely synchronized checks.

The final rule remains narrow: inspect, snapshot, commit once, and reconcile. This approach does not guarantee that rendering will succeed; machines and inputs can still fail after admission. It does guarantee that the application offered a choice for stated reasons, under a known policy, with a bounded storage decision and a durable record that can be explained later.

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
