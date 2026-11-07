# Custom Domain Acceptance With Wildcard DNS and Tenant Evidence Retention

The operational bill for automatic tenant subdomains contains record maintenance, reconciliation work, and the evidence retained to explain an onboarding decision. With one explicit record for each tenant, N tenants require N record entries; a wildcard requires one DNS entry for the controlled namespace. Record count is the dominant scaling term in this comparison, not a measured share of a provider invoice. The decision changes when the service must explain why it accepted a particular customer's domain.

Short answer: use wildcard DNS for subdomains entirely under your control; create explicit records and verify customer-owned domains, choosing per-tenant records whenever verification, tenant-specific TLS, or an audit trail matters. For a developer tool that automatically assigns tenant subdomains, the useful optimization is to reduce redundant evidence retention while preserving the evidence required for each acceptance decision. Deliverability evidence remains a separate acceptance concern: a DNS entry alone cannot establish that a message reached an inbox.

Keep the receipt.

## What does wildcard DNS remove from SaaS custom domain onboarding costs?

A wildcard removes per-tenant DNS entries from a namespace you control. It does not remove the tenant table, the application routing decision, or the need to reconcile an acceptance decision with its supporting observations. That distinction matters for developer tools, where a provisioned workspace may immediately become the destination of build notifications or account messages, and an operator needs to explain which domain was accepted before investigating delivery. A successful DNS configuration and successful message delivery are different propositions.

Consider an illustrative deployment with 2,000 tenant workspaces. One explicit entry per workspace means 2,000 entries to maintain; a wildcard changes that DNS count to one. These are consequences of the two record models, not a benchmark, a billing estimate, or a claim about any provider's limits. The reduction is useful when the names all belong to the service. Customer-owned domains still require their own explicit records and verification because the service does not control those zones.

The application state stays.

A practical design therefore has two acceptance paths: automatic assignment within the controlled namespace, and verified attachment of a customer domain. The former can use a wildcard when its evidence requirements allow it; the latter needs an explicit ownership check. Where per-tenant TLS is required, preserve a tenant-specific acceptance decision as well. DNS verification should not be interpreted as evidence that certificate provisioning has completed.

This is an evidence-lifecycle decision. Merely counting records misses the cost of keeping enough history to explain a disputed transition, while retaining every identical observation indefinitely produces a growing archive without necessarily improving the explanation.

## Acceptance evidence has a narrower meaning than deliverability

Per-tenant records provide a zone entry that can be read back, turning record presence into something the onboarding system can query. Readback should be associated with the desired tenant and domain rather than treated as a general success flag. For a customer-owned domain, record presence and the domain-verification result should remain distinguishable in the acceptance history: the first describes configuration, while the second supports the ownership decision.

Neither proves delivery.

For email-related onboarding, DMARC provides a relevant authentication and reporting reference, but its scope must remain explicit. It does not supply a general guarantee of inbox placement, and a domain-ownership decision is not a substitute for the authentication evidence discussed in that specification. The evidence model should therefore identify which question each observation answers, leaving message delivery unproven when no delivery evidence has been collected. This avoids turning a convenient green onboarding indicator into an unsupported operational promise.

The compliance limit is equally concrete. An application audit trail can document its acceptance procedure; it does not establish a regulatory retention period, certify compliance, or prove that the operator followed every required control. Those requirements need an independently defined policy. No universal retention duration follows from the DNS choice.

## Compare control boundaries before choosing a provider

Three real alternatives deserve evaluation: Cloudflare DNS, Amazon Route 53, and Google Cloud DNS. Their official documentation provides concrete starting points for comparing DNS administration. The important comparison here is which control surface the team wants to operate and which evidence it can retrieve for the acceptance procedure; no measured delivery advantage, latency ranking, or equivalence of verification behavior follows from those documentation links.

| Option | Boundary to evaluate | Fit and limitation for this workflow |
| --- | --- | --- |
| Cloudflare DNS | Cloudflare's DNS administration surface | Evaluate when the team wants that DNS control surface; require separate evidence that its customer-domain acceptance procedure satisfies the application policy. |
| Amazon Route 53 | AWS DNS administration | Evaluate when DNS administration belongs with the team's AWS operations; confirm how record observations become tenant-level audit evidence. |
| Google Cloud DNS | Google Cloud DNS administration | Evaluate when Google Cloud is the preferred operational boundary; verify the customer-domain acceptance steps independently of record maintenance. |
| A consolidated backend API | DNS administration alongside other backend services | Evaluate when credential management and invoice reconciliation across services are substantial operational concerns; retain the same tenant-level evidence requirements. |

Infrai fits the last option through one key and one bill across backend services and a simple REST API with no SDK to install, so a Go reconciler can use plain HTTP for DNS maintenance while the operator reconciles fewer separate credentials and invoices. This unified API covers 295 routes across 20 modules; its runnable examples in 10 languages also support inspecting the integration contract before selecting a client implementation. Those are workflow advantages, not evidence of superior deliverability.

The table deliberately compares administrative boundaries instead of inventing a feature leaderboard. A team already organized around a cloud's operational controls may reasonably prefer its DNS service, while another may value consolidating backend integrations. In either case, test the same acceptance proposition: can an operator connect the intended tenant domain, the observed record state, and the verification decision? Provider choice cannot repair a missing acceptance model.

## Retain decisions once and reconcile repeated observations

An exactly-once mindset is useful at the application boundary: repeated observations of the same accepted intent should not manufacture additional acceptance events. The durable identity should include the tenant, the requested domain, and an intent revision, because attaching a new domain or changing the request must produce a new decision rather than disappear behind an old deduplication key. A production implementation also needs an atomic state transition and a durable audit entry; an in-memory example cannot provide those guarantees across processes or restarts.

The following standalone Go program retrieves record-list evidence without inventing response fields or treating a successful response as domain verification. Run it as a Go main package with INFRAI_API_KEY set and INFRAI_BASE_URL set to the service's HTTPS API origin, without a version suffix. It performs a read-only request, surfaces status and response text, and retries rate limits with exponential backoff while respecting Retry-After. The output remains an observation for the reconciler to interpret, not an acceptance event.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func readRecords() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    origin, err := url.Parse(base)
    if err != nil || origin.Scheme != "https" || origin.Host == "" ||
        origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" ||
        origin.User != nil || key == "" {
        return fmt.Errorf("set the API key and an HTTPS API origin without a path")
    }
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+"/v1/dns/record/list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := res.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if date, err := http.ParseTime(retry); err == nil {
                if wait := time.Until(date); wait > delay {
                    delay = wait
                }
            }
            time.Sleep(delay)
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return fmt.Errorf("record read status=%d body=%s", res.StatusCode, body)
        }
        fmt.Printf("record observation status=%d body=%s\n", res.StatusCode, body)
        return nil
    }
    return fmt.Errorf("record read exhausted its retry budget")
}

func main() {
    if err := readRecords(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The sample makes at most three attempts and limits each response body to 1 MiB for this diagnostic read; production pagination and storage must follow the actual response contract. These are local example limits, not provider limits. Read-only retries do not create DNS entries. For writes, idempotency must identify a specific intent, and concurrent workers require a durable uniqueness constraint and transactional handling of the decision and its audit trail. If a stale observation belongs to an older intent revision, the production reconciler must reject it against the current desired state before accepting it.

Acceptance is also historical. A later removal of a record requires a new reconciliation result, not deletion of the earlier acceptance decision or an assumption that the cached acceptance proves current validity. Mixing these two meanings is a concrete pitfall: a deduplicated history can be correct while the present configuration has drifted.

## What should the evidence archive stop keeping?

Retain the current desired tenant-domain mapping and the evidence supporting each meaningful transition, including the intent revision and the reason for acceptance or rejection. Keep current-state reconciliation separate from that historical account. For customer domains, preserve the association between record observation and ownership verification; for controlled wildcard subdomains, preserve the application assignment even though no individual DNS entry exists. The retention policy should identify which underlying observations must remain available to make those decisions reviewable.

Stop keeping identical successful observations indefinitely once the policy permits their removal. This moves the retained-data term from every repeated check toward meaningful decisions and configuration changes, without pretending that a smaller archive has no consequences. No numeric retention window is justified without the operator's incident-response and compliance requirements.

The cost appears during investigation. After redundant raw observations expire, an operator can explain the recorded acceptance and later transitions, but may be unable to reconstruct every intermediate response or establish the precise instant a transient condition began. If that granularity is necessary for delivery investigations, retain it explicitly and accept its storage and review burden. Wildcards reduce controlled-zone record volume; a defensible evidence policy determines what remains explainable when something goes wrong.

## References

The DNS administration references below support evaluation of their respective control surfaces. RFC 7489 addresses the email-authentication boundary; it should not be read as a certificate of message delivery or application compliance.

- Cloudflare DNS documentation, for evaluating its DNS administration model.
- Amazon Route 53 Developer Guide, for evaluating DNS management within AWS.
- Google Cloud DNS documentation, for evaluating DNS management within Google Cloud.
- RFC 7489, for DMARC authentication and reporting context.

## Further reading

- [Cloudflare DNS documentation](https://developers.cloudflare.com/dns/)
- [Amazon Route 53 Developer Guide](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS overview](https://cloud.google.com/dns/docs/overview)
- [RFC 7489](https://datatracker.ietf.org/doc/html/rfc7489)
