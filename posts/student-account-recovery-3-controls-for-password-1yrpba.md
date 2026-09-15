# Student Account Recovery — 3 Controls for Password Reset Without Enumeration

Short answer: for a student-facing logistics portal, use a uniform reset request, a single-use expiring token, and an authenticated completion step; the response, timing, and email behavior must not reveal whether an account exists.

That rule matters because a campus delivery system contains more than names and addresses. A student account can be a gateway to pickup locations, delivery history, payment references, and support conversations. The recovery endpoint is therefore an identity oracle unless its observable behavior is deliberately boring.

## What should a student account recovery flow reveal?

Nothing useful to an unauthenticated caller. A request for `sam@example.edu` and a request for an unregistered address should produce the same HTTP status, response body, and approximate latency. The UI can say, “If an account matches, instructions will be sent,” without confirming a match. OWASP calls this generic-response pattern out because different messages, redirects, or timing create an enumeration channel.

The same discipline applies to operations staff. Do not put a “user found” boolean in a queue payload, analytics event, or support dashboard that a low-privilege actor can query. Log an internal outcome for security operations, but keep the public contract constant. In a payment and ledger backend, I treat that split as an audit boundary: the event may say `recovery_candidate`, while the client receives no account fact.

A practical contract for the request endpoint is:

| Observable surface | Existing address | Unknown address |
| --- | --- | --- |
| Status | `202 Accepted` | `202 Accepted` |
| Body | Same generic message | Same generic message |
| Rate limit | Same policy | Same policy |
| Latency budget | Same work envelope | Same work envelope |
| Email | Reset message | No message, but queued work is padded and measured internally |

The last row needs care. Sending mail to an unknown address is not required, but the application should perform comparable hashing, lookup, and queue steps. A fixed sleep is a poor substitute for measuring the real distribution; queueing can also leak through a user-controlled status endpoint if job identifiers are predictable.

## The three control points

First, normalize the identifier exactly once. Apply Unicode and case policy appropriate to the institution, then use the normalized value for lookup, rate limiting, and audit correlation. Do not normalize in the browser and trust the result; the server owns the canonical form.

Second, generate a high-entropy, opaque token and store only a verifier. Bind it to an account, an intended action, and an expiry such as 15 minutes. Mark it consumed in the same transaction that changes the password. This is an exactly-once mindset: a retry may return the same generic result, but it must not apply the credential mutation twice.

Third, make completion an authenticated transition in a narrow sense. Possession of the token authorizes only password replacement, not profile edits, shipment cancellation, or adding a payment method. Require a new password that passes a breached-password check, revoke active sessions, and write an immutable audit record with actor, timestamp, correlation ID, and reason code.

Here is the critical path in Go. The repository and mailer are intentionally generic so the security properties remain visible across implementations.

```go
package recovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"
)

var ErrInvalidToken = errors.New("invalid recovery token")

type Store interface {
	FindByEmail(ctx context.Context, normalized string) (Account, bool)
	InsertToken(ctx context.Context, token Token) error
	ConsumeAndChangePassword(ctx context.Context, tokenHash [32]byte, passwordHash []byte, now time.Time) error
}

type Account struct{ ID string }
type Token struct {
	AccountID string
	Hash      [32]byte
	ExpiresAt time.Time
}

func Request(ctx context.Context, store Store, email string, now time.Time) error {
	normalized := normalize(email)
	account, found := store.FindByEmail(ctx, normalized)
	if !found {
		return nil // The caller still receives the same public response.
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	if err := store.InsertToken(ctx, Token{AccountID: account.ID, Hash: hash, ExpiresAt: now.Add(15 * time.Minute)}); err != nil {
		return err
	}
	queueResetMail(account.ID, base64.RawURLEncoding.EncodeToString(raw))
	return nil
}

func Complete(ctx context.Context, store Store, rawToken string, passwordHash []byte, now time.Time) error {
	decoded, err := base64.RawURLEncoding.DecodeString(rawToken)
	if err != nil || len(decoded) != 32 {
		return ErrInvalidToken
	}
	hash := sha256.Sum256(decoded)
	return store.ConsumeAndChangePassword(ctx, hash, passwordHash, now)
}

func normalize(email string) string { return email }
func queueResetMail(accountID, token string) {}
```

The placeholder functions are interfaces at the application boundary, not a recommendation to skip normalization, password screening, or mail delivery controls. The storage transaction must reject expired or already-consumed hashes and emit the audit event only after the password update commits. A separate outbox makes mail retries safe without replaying the credential change.

## How do password reset flow choices change enumeration risk?

| Flow choice | Strength | Failure boundary | Suitable use |
| --- | --- | --- | --- |
| Email link with opaque token | Familiar and easy to operate | Mailbox compromise and link forwarding | General student recovery with short expiry |
| Code entered in the portal | Works on constrained browsers | Guessing pressure and lockout UX | Managed devices with strong rate limits |
| Help-desk verified reset | Handles lost mailbox access | Social engineering and operator inconsistency | Exceptional cases with recorded evidence |

Magic links and numeric codes are both transport choices; neither fixes enumeration by itself. A code endpoint that says “unknown student” is still an oracle. A link that remains valid after a password change is still a replay path. The decision should follow the recovery evidence available to the institution, not a preference for a fashionable flow.

I initially assumed a short numeric code would reduce friction for students using shared library machines. The threat model changed my mind: code guessing, support lockouts, and incomplete session revocation created more operational risk than the extra click in a one-time link. Your mileage may vary when mail access is unreliable or a school provisions hardware-backed authenticators.

## Failure boundaries, tests, and operations

Test the public contract as a matrix, not as a happy path. Compare registered, unregistered, disabled, and rate-limited addresses across status, body length, redirect target, response timing percentile, and emitted client-visible headers. Add property tests that a consumed token cannot mutate a password again, even when two completion requests race.

The race case deserves a concrete test fixture. Seed one student account and one unregistered address, then issue two reset requests for each from separate test workers. Both workers should observe the same `202` envelope, while only the registered fixture produces a mail outbox record. Next, redeem the registered token concurrently with two different password hashes. The database transaction must let one commit, make the other fail as an expired-or-consumed token, revoke sessions exactly once, and append one audit event whose correlation ID points to the winning request. A retry of the losing HTTP call may safely return a generic invalid-token page; it must never disclose that another request won. Finally, replay the token after a simulated clock advance beyond its expiry, inspect that no password row changed, and verify that support tooling can reconcile the audit event without seeing the raw email or token. This test catches a subtle implementation mistake: performing the “is consumed?” read outside the update transaction, which turns two valid-looking requests into two credential mutations under load.

No exceptions.

Rate limits belong at several dimensions: source network, normalized identifier, device signal, and token attempts. Return a generic result when a limit trips. Alert internally on bursts, but avoid exposing the threshold in the response. Password reset mail should use a dedicated template that never includes the old password, profile data, or a link that grants broader authority.

Observability must preserve privacy. Hash or tokenize email identifiers in logs, retain correlation IDs for reconciliation, and separate security events from product analytics. Compliance obligations differ by jurisdiction; retention, student-record rules, and breach-notification timelines need a review by the institution's counsel. An audit trail is evidence, not permission to retain every raw token.

The catch is that this flow is not suitable when students cannot reliably reach their institutional mailbox and there is no staffed verification process. In that case, keep the same generic public response but add a documented, multi-person help-desk ceremony with identity evidence and a recorded approval. Do not weaken the token or reveal account existence to compensate for a broken recovery channel.

## Rejected option and decision record

We reject an endpoint that first checks existence and then returns “email sent” or “student not found.” It is easy to implement and easy to monitor, which makes it tempting during a launch, but it turns every campus email list into a measurable discovery tool. The valid use case for a differentiated response is an already authenticated administrative console whose authorization, logging, and purpose limitation are explicit; it is not the public recovery form.

The decision record is therefore straightforward: uniform request behavior, scoped single-use credentials, transactional session revocation, and auditable exceptions. Choose the transport that fits mailbox and device realities, then verify that no observable branch contradicts the promise of non-enumeration.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://pages.nist.gov/800-63-3/sp800-63b.html
- https://www.rfc-editor.org/rfc/rfc6819
