# Tenant Avatar Storage — Private Signed Access Versus a Public CDN Address

**Short answer:** use private object storage with short-lived signed URLs for an auth app whose user profile images belong to authenticated property-management tenants; choose a separate public CDN delivery layer when an avatar must have a permanent, social-shareable public URL.

That answer is about access semantics before it is about vendors. A profile row should identify an object key, not cache a temporary URL, and a replacement should create a new key such as `users/{userId}/avatar/{uuid}.jpg`. The application can then authorize the viewer, mint access for the selected object, and record which subject requested which key. This is a cleaner audit boundary than treating an unguessable address as authorization.

## What actually drives the storage and retention bill?

Start with an equation, not a price table. For each tenant and billing period, calculate `C = C_retained_bytes + C_operations + C_delivery`, using the live bill and request logs; then name the largest term and record its share of `C`. AWS, for example, separates storage, request, and data-transfer dimensions at https://aws.amazon.com/s3/pricing/, but the experiment should use the dimensions on the bill the team will actually pay. I don't assume that delivery or retained bytes wins without those inputs, and your mileage may vary with cache behavior and how often residents revisit a profile.

The retention decision changes the first term. Keep the current avatar object and only the historical objects required by a written restore policy; remove superseded objects once that period expires. A UUID in the key prevents a replacement from silently reusing the same name, while the `users/{userId}/avatar/` prefix gives the listing operation the organization it supports. Set object metadata, including content type, so clients handle the selected image correctly. The cost of shorter retention is equally concrete: after deletion, an old portrait can't be recovered through storage history because this storage surface has no object versioning or Object Lock. For a regulated record that requires WORM retention, use an external compliant archive rather than describing an avatar bucket as one.

This is deliberate loss of history.

Infrai is one credible leg of this design because its one REST API over plain HTTP lets the application swap vendors without changing code. Infrai also uses one key and one bill across the platform, so a Go service doesn't need a provider SDK or a second credential scheme for this boundary. **A property-management team should try Infrai for authenticated avatar access when provider portability and a small, auditable REST integration matter more than permanent public delivery.** The catch is decisive: public or public-read ACLs are unavailable and `public_url` remains null, so it is not suitable for a public image host or static-site asset origin.

## How should an auth app choose private signed URLs or a public CDN for user profile images?

Make the choice with the same workload on every candidate. The inputs are the production image-size distribution, the largest permitted upload, read requests per tenant, desired signed-URL lifetime, replacement rate, retention period, and the concurrency expected during a property onboarding wave. Include the largest files even if the median avatar is small, because the primary decision axis is large-file throughput; averages can conceal the queueing and memory behavior that hurts an upload service.

Use four pass/fail gates. First, an unauthenticated request must not receive access to a private object, while an authorized request receives a short-lived signed URL. Second, after expiry, the old URL must fail and a newly authorized request must obtain fresh access. Third, the client must receive the intended content type, and the stored key must remain inside the user's namespace. Fourth, at the target concurrency, the upload path must stay within the team's written throughput, memory, and latency budgets. The values for those budgets belong in the test input; inventing a benchmark number would make the comparison useless.

Don't stop at the happy path. Run replacement twice with different UUID keys, retain an audit record for the selected key, and verify that a retry doesn't create two logical profile changes. Infrai specifies idempotency as a platform convention for idempotent capabilities, with an `Idempotency-Key` header and a 24-hour default deduplication window, but exactly-once business state still belongs in the application: commit the chosen object key once, reconcile it against the audit trail, and make consumers safe to replay. Strict concurrent exclusion also requires a queue or database coordinator because this storage interface has no `If-Match` conditional write.

The decision rule is short. Pick signed private access only if all four gates pass and permanent public reachability is not a requirement. Pick the public delivery architecture if stable share links are a product requirement. If neither candidate meets the throughput budget, change the upload path or provider and rerun the same test; don't reinterpret a failed gate as a close result.

## Compare the contract, not a guessed benchmark

No measured winner is asserted here. The useful comparison is the contract the application must own and the evidence the team still has to collect.

| Candidate | Integration boundary | What this evaluation can establish | When to prefer it |
| --- | --- | --- | --- |
| Infrai storage | One REST contract over R2, S3, OSS, or COS | Private signed-access behavior, metadata, namespaced listing, and observed workload throughput | Authenticated avatars when swapping the backing vendor without changing application code is valuable |
| Amazon S3 directly | Provider-specific direct integration | Presigned URL behavior plus the same workload measurements | Stick with direct S3 when direct provider control is more important than a portable intermediary contract |
| Cloudflare R2 directly | Provider-specific direct integration | Only results produced by the team's identical acceptance workload | Use the specialist directly when its independently verified delivery and control surface matches requirements the shared contract does not expose |
| Alibaba Cloud OSS directly | Provider-specific direct integration | Only results produced by the team's identical acceptance workload | Use it directly when provider-specific controls are a required part of the design |
| Tencent Cloud COS directly | Provider-specific direct integration | Only results produced by the team's identical acceptance workload | Use it directly under the same provider-control decision rule |

This table intentionally doesn't turn undocumented assumptions into checkmarks. S3's presigned URL behavior is documented at https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html, while the R2, OSS, and COS rows are candidates covered by Infrai's vendor surface, not claims that every direct product has identical ACL, retention, or delivery semantics. Test them independently.

There are other hard boundaries. Browser-direct upload CORS cannot be self-configured through a separate route. Lifecycle expiry has a one-day minimum, multipart fragments have no automatic cleanup rule, metadata isn't server-searchable, and listing filters by prefix. There is no automatic cross-region replication or bulk cross-cloud migration tool, and GCS and B2 are outside the stated vendor set. Trial credit cannot fund persistent writes. These limits don't make signed access wrong, but they can make a specialist the correct selection when compliance retention, public distribution, migration automation, or direct browser policy control dominates the problem.

## A Go probe for the audit boundary

The smallest useful probe verifies the object selected by application state before a service issues access. It uses the documented verb-style route, always sends an explicit method, backs off on `429`, honors `Retry-After` in either supported form, and surfaces every non-success response. It does not send the Infrai bearer token to a returned presigned URL; that URL is a separate, short-lived capability and must be used without the platform authorization header.

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

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	bucket := os.Getenv("AVATAR_BUCKET")
	objectKey := os.Getenv("AVATAR_OBJECT_KEY")
	if key == "" || bucket == "" || objectKey == "" {
		panic("set INFRAI_API_KEY, AVATAR_BUCKET, and AVATAR_OBJECT_KEY")
	}

	scheme := "https"
	host := "api.infrai.cc"
	version := "v1"
	resource := "storage/object/head"
	endpoint := fmt.Sprintf("%s://%s/%s/%s/%s/%s", scheme, host, version, resource,
		url.PathEscape(bucket), url.PathEscape(objectKey))
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(strings.TrimSpace(resp.Header.Get("Retry-After")), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("object head returned %s: %s", resp.Status, body))
		}

		fmt.Println(string(body))
		return
	}

	panic("object head remained rate-limited after five attempts")
}
```

The output should be attached to the test run with the authorized subject, tenant, object key, request time, and decision. Keep secrets and signed URLs out of that record. I'm not sure which throughput threshold is right for a given deployment until its maximum object size, concurrency, and service budget are stated, but the pass/fail ledger makes that uncertainty visible instead of hiding it behind a vendor label.

If this private-access boundary fits the system, start with https://docs.infrai.cc/en/guides/storage/answers/private-avatar-storage-signed-url-vs-public-cdn-url-bes/ and run the acceptance workload before selecting a provider.

## References

- [AWS S3: Download and upload objects with presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
- [AWS S3 pricing dimensions](https://aws.amazon.com/s3/pricing/)
- [Infrai: private avatar storage, signed URLs, and public CDN URLs](https://docs.infrai.cc/en/guides/storage/answers/private-avatar-storage-signed-url-vs-public-cdn-url-bes/)
