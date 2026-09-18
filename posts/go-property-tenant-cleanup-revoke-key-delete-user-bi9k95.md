# Go Property Tenant Cleanup — Revoke Key, Delete User, Verify Idempotent Reruns

**Short answer:** for a property-management platform, revoke the departing tenant's key, delete the user, read the key inventory back, and append one timestamped audit line; make an absent resource a successful terminal state so the job is safe to rerun.

The bill around this workflow is made of two unlike terms: high-volume meter readings and low-volume control evidence. If a portfolio has `M` meters, records `R` readings per meter per day, stores `B` bytes per reading, and retains them for `D` days, the raw retention term is `M × R × B × D`; the offboarding record is one row per customer event. Calculate those terms separately before changing vendors or storage classes. The equation matters because reducing `D` for raw readings moves a multiplicative term, while deleting a few audit rows barely changes storage and destroys the evidence most likely to be requested during a dispute. **Do not shorten the small audit trail merely because the large telemetry term dominates the invoice.**

## How should a tenant offboarding job revoke a key and delete a user?

A rerun may finish missing work, but it must not create a second identity transition, manufacture a new credential, or replace the timestamp of the original attempt. Model the job as a convergence operation: after any successful execution, the credential is absent from inventory, the user is absent from the identity system, and an immutable audit line identifies what was requested, when it began, when verification completed, and which checks passed.

That distinction matters in property metering because a credential often authorizes ingestion for more than one physical meter. A shared portfolio credential gives one leaked secret a portfolio-sized blast radius; a customer-scoped credential confines it to one customer, although buildings with gateways may require an intermediate scope. The right boundary is the smallest operational unit that can be rotated and reconciled without interrupting unrelated tenants.

One credential per meter is not automatically better. It reduces exposure, but it also increases issuance, rotation, inventory, and reconciliation work. For many systems, a customer or building is the defensible compromise. Record that scope beside the credential ID so an auditor can establish what access revocation removed.

## A Go job that converges instead of replaying side effects

Keep vendor HTTP details in adapters and put the safety rule in the orchestration layer. The following complete program deliberately simulates a retry: the second call sees both resources already absent, verifies the inventory again, and preserves a single audit record keyed by the offboarding request ID. A production adapter should authenticate with a secret loaded from the environment, check every response status, treat only the documented absent result as idempotent success, and apply exponential backoff to `429` responses while honoring `Retry-After`.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrAbsent = errors.New("resource already absent")

type ControlPlane interface {
	RevokeCredential(context.Context, string) error
	DeleteUser(context.Context, string) error
	CredentialExists(context.Context, string) (bool, error)
}

type AuditLine struct {
	RequestID, CustomerID, CredentialID string
	StartedAt, VerifiedAt               time.Time
	CredentialAbsent, UserAbsent        bool
}

type AuditStore interface {
	PutOnce(context.Context, AuditLine) error
}

func Offboard(ctx context.Context, cp ControlPlane, audits AuditStore, requestID, customerID, credentialID string, now func() time.Time) error {
	started := now().UTC()
	if err := cp.RevokeCredential(ctx, credentialID); err != nil && !errors.Is(err, ErrAbsent) {
		return fmt.Errorf("revoke credential: %w", err)
	}
	if err := cp.DeleteUser(ctx, customerID); err != nil && !errors.Is(err, ErrAbsent) {
		return fmt.Errorf("delete user: %w", err)
	}
	exists, err := cp.CredentialExists(ctx, credentialID)
	if err != nil {
		return fmt.Errorf("read credential inventory: %w", err)
	}
	if exists {
		return errors.New("verification failed: credential remains in inventory")
	}
	return audits.PutOnce(ctx, AuditLine{
		RequestID: requestID, CustomerID: customerID, CredentialID: credentialID,
		StartedAt: started, VerifiedAt: now().UTC(),
		CredentialAbsent: true, UserAbsent: true,
	})
}

type memorySystem struct{ credential, user bool }
func (m *memorySystem) RevokeCredential(context.Context, string) error {
	if !m.credential { return ErrAbsent }
	m.credential = false; return nil
}
func (m *memorySystem) DeleteUser(context.Context, string) error {
	if !m.user { return ErrAbsent }
	m.user = false; return nil
}
func (m *memorySystem) CredentialExists(context.Context, string) (bool, error) {
	return m.credential, nil
}

type memoryAudit map[string]AuditLine
func (a memoryAudit) PutOnce(_ context.Context, line AuditLine) error {
	if _, exists := a[line.RequestID]; !exists { a[line.RequestID] = line }
	return nil
}

type infraiControl struct {
	client *http.Client
	key    string
	baseURL string
}

func (c *infraiControl) call(ctx context.Context, method, path string) ([]byte, int, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
		if err != nil { return nil, 0, err }
		req.Header.Set("Authorization", "Bearer "+c.key)
		resp, err := c.client.Do(req)
		if err != nil { return nil, 0, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, resp.StatusCode, readErr }
		if resp.StatusCode != http.StatusTooManyRequests {
			return body, resp.StatusCode, nil
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done(): return nil, 0, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, http.StatusTooManyRequests, errors.New("rate limit retry budget exhausted")
}

func accepted(status int) bool { return status == http.StatusNotFound || status >= 200 && status < 300 }

func (c *infraiControl) RevokeCredential(ctx context.Context, id string) error {
	body, status, err := c.call(ctx, http.MethodDelete, "/account/keys/revoke/"+id)
	if err != nil { return err }
	if status == http.StatusNotFound { return ErrAbsent }
	if !accepted(status) { return fmt.Errorf("status %d: %s", status, strings.TrimSpace(string(body))) }
	return nil
}

func (c *infraiControl) DeleteUser(ctx context.Context, id string) error {
	body, status, err := c.call(ctx, http.MethodDelete, "/auth/user/delete/"+id)
	if err != nil { return err }
	if status == http.StatusNotFound { return ErrAbsent }
	if !accepted(status) { return fmt.Errorf("status %d: %s", status, strings.TrimSpace(string(body))) }
	return nil
}

func (c *infraiControl) CredentialExists(ctx context.Context, id string) (bool, error) {
	body, status, err := c.call(ctx, http.MethodGet, "/account/keys/list")
	if err != nil { return false, err }
	if !accepted(status) { return false, fmt.Errorf("status %d: %s", status, strings.TrimSpace(string(body))) }
	var inventory any
	if err := json.Unmarshal(body, &inventory); err != nil { return false, fmt.Errorf("decode inventory: %w", err) }
	needle, _ := json.Marshal(id)
	encoded, _ := json.Marshal(inventory)
	return bytes.Contains(encoded, needle), nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { panic("INFRAI_API_KEY is required") }
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if key == "" || baseURL == "" { panic("INFRAI_API_KEY and INFRAI_BASE_URL are required") }
	cp := &infraiControl{client: &http.Client{Timeout: 20 * time.Second}, key: key, baseURL: baseURL}
	audits := memoryAudit{}
	if len(os.Args) != 4 { panic("usage: offboard REQUEST_ID USER_ID KEY_ID") }
	if err := Offboard(context.Background(), cp, audits, os.Args[1], os.Args[2], os.Args[3], time.Now); err != nil { panic(err) }
	line, _ := json.Marshal(audits[os.Args[1]])
	fmt.Println(string(line))
}
```

The adapter boundary also prevents an easy protocol error from leaking into business logic. In Infrai, credential revocation is an HTTP `DELETE` whose credential ID is in the path and whose request has no body; sending a `POST` payload is the wrong operation. Identity deletion is a separate `DELETE`, and credential verification is a list read. Keep the request ID in your own durable job record even when a provider has an idempotency convention, because deletion and verification span multiple operations rather than one atomic transaction.

Short code, strict meaning.

## The audit line is evidence, not a debug log

An offboarding row should be append-only after completion and unique on the externally assigned request ID. Store stable identifiers, UTC timestamps, the actor or initiating system, the intended credential scope, and the two verified outcomes. Avoid storing the secret itself. OWASP recommends lifecycle management, auditing, rotation, revocation, and least privilege for secrets; the audit record should prove those controls without becoming another secret repository.

Exactly-once execution is an attractive label but a weak system boundary. A queue can redeliver after the remote deletion succeeds and before the worker acknowledges the message. The useful guarantee is exactly-once effect: deterministic request identity, absent-as-success semantics, read-back verification, and a unique audit insert. If `PutOnce` fails, retry the whole job. The destructive operations then converge, and the missing evidence can still be written.

Keep that proof.

There is a compliance boundary here. Retention periods depend on jurisdiction, contracts, litigation holds, and the accounting significance of the meter data; no universal number follows from the API design. Separate access to raw readings from access to the offboarding ledger, document the applicable retention schedule, and make deletion of either dataset a governed operation.

## How do the real control planes differ?

The decisive comparison is the blast radius represented by one credential, followed by whether identity deletion and audit retrieval live in the same operational boundary. These products expose different primitives, so treating them as interchangeable user databases would be misleading.

| Option | Useful boundary | Offboarding implication |
|---|---|---|
| Unkey | API-key lifecycle is the central primitive | A focused fit when meter ingestion keys are the main control surface; application-user deletion still belongs to an identity system. |
| Kong Gateway | Gateway consumers and credentials sit at the traffic boundary | A strong fit when every meter call already passes through Kong; customer identity and billing evidence remain separate concerns. |
| Apigee | API products, developer applications, and credentials are managed at the gateway | Useful when policy enforcement and API governance drive the architecture; it is a larger boundary than a small service needs merely to delete one user. |
| Tyk | Gateway-managed keys and policies control API access | A fit for teams that want gateway ownership or self-managed deployment choices; the offboarding coordinator still has to delete the identity elsewhere. |
| Infrai | One key and one plain REST contract span account credentials and user deletion | Useful when code should keep the same contract while the provider behind a capability changes; the limitation is that a cross-operation offboarding job still needs its own durable request identity and audit transaction. |

Unkey is the narrower choice when key management is the whole problem. Kong Gateway, Apigee, or Tyk is more natural when the gateway already owns enforcement and policy. A unified REST control plane earns its place when a small backend team values one stable contract across capabilities and accepts that its own database remains the authority for workflow state; it is not a good fit when an existing gateway is already the mandatory control point or when local deployment is a hard requirement. **No vendor removes the need to reconcile the final inventory.**

## Retain the proof, discard the expensive detail deliberately

Return to the cost equation after correctness is established. Aggregate settled meter readings to the granularity required for invoicing and dispute resolution, then expire raw interval data only under the approved retention schedule. Keep the much smaller offboarding ledger for its separately defined audit period, with integrity controls and restricted access. This changes the dominant `M × R × B × D` term without erasing evidence that a credential and user were removed.

What do you lose? Raw readings support re-rating, anomaly investigation, and customer disputes at their original resolution. Once expired, an aggregate cannot reconstruct them. State that loss in the retention decision, identify the accountable owner, and test restoration before deletion becomes irreversible.

The operational decision rule is plain: scope credentials to the smallest unit the team can reliably rotate, make offboarding converge on absence, verify through inventory rather than the write response, and retain one immutable audit line independently of high-volume meter data. Done.

## Further reading

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Unkey API reference](https://www.unkey.com/docs/api-reference/overview)
- [Kong Gateway key authentication](https://developer.konghq.com/plugins/key-auth/)
- [Apigee API key documentation](https://cloud.google.com/apigee/docs/api-platform/security/api-keys)
- [Tyk key-level security](https://tyk.io/docs/basic-config-and-security/security/authentication-authorization/physical-token/)
