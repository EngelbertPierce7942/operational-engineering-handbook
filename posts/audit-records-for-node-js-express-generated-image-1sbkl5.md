# Audit Records for Node.js Express Generated Images and Per User Lifecycle Delete

Short answer: store each AI-generated e-commerce image under an opaque tenant-scoped object key, keep ownership and deletion state in a transactional metadata record, issue short-lived download links only after authorization, and use a bucket lifecycle rule as a backstop rather than the primary deletion mechanism.

The deciding constraint is deletion correctness. An expiring URL limits how long one capability can be used; it doesn't prove that the image was removed, nor does a folder-like prefix establish ownership. For private user uploads, the durable design has two clocks: access expiry measured by the signed link and retention expiry recorded against the object. They must never be treated as the same event.

## How should Node.js and Express store AI-generated images for each user?

Treat an Express endpoint as an adapter around a small storage contract, even though the critical-path example below is in Go so that the state transitions remain explicit. Authenticate the caller, derive the tenant and user identifiers from server-side identity, generate an opaque image identifier, and construct a key such as `tenant/<tenant-id>/user/<user-id>/image/<image-id>`. The client supplies image bytes and content type, but it must not supply an authoritative object key, owner, retention deadline, or bucket name. This prevents one user's path choices from becoming another user's authorization boundary.

The database row is the ledger: image ID, tenant ID, user ID, object key, content type, creation time, retention deadline, deletion state, and an idempotency key. The object store holds bytes. A download endpoint looks up that row under the authenticated tenant and user, rejects a deleted or expired record, and only then signs a short-lived read operation. Don't persist the signed URL as identity; persist the object key and generate capabilities when needed.

This distinction is small on a diagram and decisive during reconciliation.

## Invariants and failure boundaries

The first invariant is ownership: an object can be read or deleted only through a metadata row belonging to the authenticated tenant and user. The second is monotonic deletion: once the row enters `delete_pending`, no new download capability is issued, and retries can only move it toward `deleted`. The third is auditability: every accepted upload and delete request has a stable operation ID, while timestamps come from the service rather than the caller. The fourth is bounded retention: `retention_until` is fixed from policy at acceptance time unless a separately authorized policy transition changes it.

Exactly-once delivery is not a reasonable assumption across an HTTP service, a database, and object storage. The useful exactly-once mindset is narrower: repeated commands must converge on one externally visible outcome. Give upload and delete commands idempotency keys, record intent before asynchronous work, make deletion of an already absent object a successful terminal result at the application boundary, and reconcile metadata against storage on a schedule. If upload succeeds but metadata commit does not, reconciliation should quarantine or remove the unclaimed object; if delete intent commits before byte removal, readers already see the record as unavailable while a worker retries the physical deletion.

No ambiguity there.

Audit records should identify the actor, operation, image, policy, and result without copying image contents, signed URLs, or credentials. Secrets belong in a managed secret lifecycle with restricted access and rotation; the OWASP Secrets Management Cheat Sheet is the relevant baseline. I'm not sure a single retention period will satisfy every catalog, dispute, and privacy obligation in a real commerce system, because jurisdiction and data classification resolve that question, so the policy should be explicit data reviewed by counsel rather than a duration buried in route code.

## Decision record and rejected alternatives

The choice is a metadata ledger plus private object storage, explicit deletion, and lifecycle expiry as defense in depth. The comparison is about failure behavior, not brand features.

| Option | Authorization and audit behavior | Deletion behavior | Decision |
|---|---|---|---|
| User-shaped folder keys only | Prefixes are easy to inspect but cannot replace an ownership record | Bulk prefix deletion can include the wrong generation or miss renamed keys | Reject for private multi-tenant data |
| Database bytes | One transaction can bind metadata and bytes | Row deletion is direct, but large payloads compete with transactional workloads and backups | Valid for small, tightly bounded artifacts |
| Metadata ledger plus object storage | Ownership, idempotency, and policy stay queryable | Explicit deletes handle user requests; lifecycle rules bound forgotten objects | Adopt for generated commerce images |

The catch is operational weight: the adopted option adds a worker, reconciliation, and two systems whose state can diverge temporarily. A team with tiny images, low volume, and a strict need for single-transaction semantics may reasonably keep binary data in its database. Conversely, a lifecycle-only design is suitable for disposable derived previews whose deletion deadline may be approximate and for which there is no user-triggered erasure promise; it is not suitable when the API must acknowledge a specific deletion and later prove what happened.

Multipart upload deserves a separate decision based on object size and retry requirements. Amazon S3's multipart overview describes parts being uploaded independently and the need to complete or stop the multipart upload; adopting that pattern therefore also means tracking unfinished upload state and cleaning it under policy. Do not add multipart complexity merely because the storage interface supports it.

## Critical path in code

The following service boundary is intentionally provider-neutral. A Node.js and Express implementation should preserve the same order and idempotency semantics: authenticated identity enters the service, the server derives the key, storage receives bytes, and metadata records the accepted object. Production code also needs size limits, media validation, request cancellation, rate controls, and a reconciliation worker; those are policy-specific and are not hidden inside this focused example.

```go
package images

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"time"
)

type Store interface {
	Put(ctx context.Context, key, contentType string, body io.Reader) error
	Delete(ctx context.Context, key string) error
	SignGet(ctx context.Context, key string, expiresAt time.Time) (string, error)
}

type Repository interface {
	FindByIdempotencyKey(ctx context.Context, tenantID, key string) (Image, bool, error)
	Insert(ctx context.Context, image Image) error
	MarkDeletePending(ctx context.Context, tenantID, userID, imageID string) (Image, error)
	MarkDeleted(ctx context.Context, imageID string, deletedAt time.Time) error
}

type Image struct {
	ID             string
	TenantID       string
	UserID         string
	ObjectKey      string
	ContentType    string
	IdempotencyKey string
	CreatedAt      time.Time
	RetentionUntil time.Time
}

type Service struct {
	store     Store
	repo      Repository
	now       func() time.Time
	retention time.Duration
}

func (s *Service) Upload(ctx context.Context, tenantID, userID, idempotencyKey, contentType string, body io.Reader) (Image, error) {
	if prior, found, err := s.repo.FindByIdempotencyKey(ctx, tenantID, idempotencyKey); err != nil || found {
		return prior, err
	}

	id, err := randomID()
	if err != nil {
		return Image{}, err
	}
	now := s.now().UTC()
	image := Image{
		ID: id, TenantID: tenantID, UserID: userID,
		ObjectKey: fmt.Sprintf("tenant/%s/user/%s/image/%s", tenantID, userID, id),
		ContentType: contentType, IdempotencyKey: idempotencyKey,
		CreatedAt: now, RetentionUntil: now.Add(s.retention),
	}

	if err := s.store.Put(ctx, image.ObjectKey, contentType, body); err != nil {
		return Image{}, err
	}
	if err := s.repo.Insert(ctx, image); err != nil {
		return Image{}, err
	}
	return image, nil
}

func (s *Service) Delete(ctx context.Context, tenantID, userID, imageID string) error {
	image, err := s.repo.MarkDeletePending(ctx, tenantID, userID, imageID)
	if err != nil {
		return err
	}
	if err := s.store.Delete(ctx, image.ObjectKey); err != nil {
		return err
	}
	return s.repo.MarkDeleted(ctx, image.ID, s.now().UTC())
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
```

There is one deliberate failure boundary in `Upload`: object creation precedes metadata insertion, so an insertion failure can leave an unclaimed object. The alternative order can leave metadata pointing at bytes that never arrived. The former is safer for reads because no row means no download authorization, provided a reconciler lists objects under controlled prefixes and removes unclaimed keys after a conservative grace period. It should emit counts for accepted uploads, idempotent replays, pending deletions, deletion age, unclaimed objects, and reconciliation outcomes; alerts should key on the age of unresolved state, not merely worker process health.

Deployment should stage lifecycle rules against a non-production prefix and test boundary timestamps, repeated requests, cancellation, wrong-tenant access, and reconciliation after each injected failure point. Keep the retention deadline in metadata even when the bucket rule uses an equivalent age, because the database answers policy and audit questions while the storage rule performs eventual cleanup. A daily sample that joins expected live keys to inventory is more useful than assuming that a successful API response proves long-term agreement.

## Operational acceptance criteria

Before release, demonstrate that two identical upload commands return the same logical image, a replayed delete reaches the same terminal state, a user cannot influence another user's key, and no signed download is created after deletion intent or retention expiry. Then verify that abandoned multipart work, unclaimed objects, and overdue `delete_pending` rows are visible to reconciliation, with credentials absent from logs and audit records.

Retention needs a written rule for generated originals, derived thumbnails, and audit metadata because those classes may have different obligations. Legal hold, if applicable, must override automated physical deletion through an authorized and audited state transition; otherwise the lifecycle backstop can erase evidence the business was required to preserve. The system is accepted only when deletion and preservation are both testable outcomes, not aspirations attached to a bucket configuration.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
