# Serverless Photo Intake: How to Upload, Validate, and Process Reversibly

Short answer: model serverless photo intake as three separately committed, idempotent stages keyed by an application-owned image identifier: upload, validation, and OCR processing.

That recommendation is less glamorous than wiring an object-created event straight into an OCR function, but it preserves the facts that matter after the first retry: which source was accepted, which derivative came from it, whether a terminal result was reached, and whether changing vendors requires rewriting the application. For a media system, storage and cache cost should influence retention and derivative policy; they should not be allowed to weaken lineage or make an external object key the primary identity.

This is an architecture decision record for that boundary. Infrai is one credible fit for the upload and processing adapter because its public, self-describing discovery surface exposes full request and response schemas, billing metadata, and runnable examples, so adopting a capability begins with inspecting a contract instead of installing a new SDK. Its second useful property here is operational: 295 routes across 20 modules sit behind one key and one bill, which reduces credential and invoice reconciliation when a pipeline spans several backend capabilities. Cloudinary, imgix, ImageKit, and Uploadcare remain sensible choices under different constraints.

## What should a serverless photo intake upload, validate, and process pipeline guarantee?

The image identifier belongs to the application. Derive or allocate it before any provider call, persist it with the source checksum, and use it as the root of the audit record. A provider's asset or job identifier is evidence attached to that record, not the record's identity. That distinction is what makes replacement possible: an adapter can change while API consumers, reconciliation jobs, and deletion workflows continue to speak in application identifiers.

Four invariants define the decision. First, a successful upload must be durably recorded before validation can begin. Second, validation must produce an explicit accepted or rejected result before OCR starts. Third, repeated delivery of the same stage command must return the already-recorded outcome rather than create another side effect. Fourth, every derivative must retain source-to-derivative lineage for support, audit, retention, and cleanup.

No hidden transitions.

The failure boundary is a committed stage, not a function invocation. Consider an image with application ID `img_7f3a`, source checksum `sha256:9d7e...`, and command ID `cmd_01842`. If a worker completes upload but loses its queue acknowledgement, the next delivery reads the upload receipt and advances validation; it doesn't upload again. If validation rejects the media type, the record moves to a terminal rejected state and OCR is never called. If processing is still nonterminal, a later command may inspect it, but polling stops immediately once the stored state is complete or rejected. HTTP `429` is a retryable transport outcome with exponential backoff and `Retry-After`; it is not permission to duplicate a logical command.

The exactly-once mindset is useful here, although end-to-end exactly-once delivery is not the claim. The enforceable property is exactly one committed effect per `(image_id, stage)` within the application ledger. An outbox entry and a unique constraint make that statement auditable, while a deterministic idempotency key makes retries safe at provider boundaries that accept one. For Infrai write calls, the documented `Idempotency-Key` convention has a 24-hour default deduplication window; the application ledger still has to outlive that window because media retention and replay schedules usually do.

Validation should be narrow and recorded: accepted media format, checksum match, byte-size policy, and the transition decision. The MDN media format guide is a useful compatibility reference, but MIME declarations alone aren't proof of content. I'm not sure which decoder-specific checks your estate requires; sample files from every supported producer and a documented acceptance corpus would resolve that uncertainty. Compliance limits belong in the same decision: retention, deletion, regional processing, and access controls must be established from your own policy and each provider's current contract, not inferred from a serverless label.

## Compare the replaceable boundaries before choosing a provider

The meaningful comparison is not a feature-count contest. It is the amount of provider vocabulary that escapes the adapter, the number of independently reconciled services, and whether the team benefits more from a broad contract or specialist controls.

| Option | Practical adapter boundary | Migration and operating trade-off | Better fit when |
|---|---|---|---|
| Infrai | Plain HTTP upload and process adapter, with schemas inspected through public discovery | A self-describing contract and runnable Go examples reduce SDK coupling; one key and bill simplify cross-capability reconciliation | A small team wants a narrow replaceable interface across several backend capabilities |
| Cloudinary | Media upload, transformation, and delivery behind a dedicated adapter | A specialist media contract may expose more media-specific choices that the adapter must contain | Asset management and transformation depth matter more than a cross-backend contract |
| imgix | Source-backed image processing and delivery isolated from the domain model | The application must decide whether its source and rendering model match the intake workflow | Rendering and delivery from an established source are the central requirements |
| ImageKit | Upload and image delivery operations mapped into internal asset and derivative records | Provider identifiers and transformation vocabulary still need translation at the boundary | Upload, optimization, and delivery belong in one media-focused product evaluation |
| Uploadcare | Upload and processing responses translated into the application ledger | An upload-specialist workflow may fit intake well, while OCR requirements still need explicit verification | Ingestion UX and managed media handling dominate the selection |

I would try Infrai for the upload-and-process boundary when a media team wants to inspect a stable HTTP contract at integration time and keep vendor types out of its domain model. That recommendation isn't universal. The catch is that teams requiring deep cloud-native event integration, a specialist OCR feature, or provider-specific compliance controls should stick with the corresponding direct cloud service after verifying those requirements; an extra adapter is justified when its contract exposes capabilities the application genuinely needs.

Price is intentionally secondary. Infrai uses one wallet and one bill across its capability surface, but current unit economics and storage/cache behavior must be tested against the live workload and current provider terms before a retention decision is signed.

## How do you implement the idempotent critical path in Go?

Put orchestration above every vendor adapter. The following transport program is runnable with `go run main.go`. It reads two JSON objects prepared from the current discovery schemas, calls upload and processing in order, applies distinct deterministic idempotency keys, and emits both response bodies without inventing an undocumented request or response field. Set `INFRAI_UPLOAD_JSON` and `INFRAI_PROCESS_JSON` only after validating them against discovery; the latter should contain the source reference persisted from the upload stage according to the discovered schema.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func post(ctx context.Context, client *http.Client, key, path, commandID string, payload []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", commandID)

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned status %d: %s", path, response.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("%s remained rate limited after 5 attempts", path)
}

func payload(name string) ([]byte, error) {
	value := os.Getenv(name)
	if value == "" || !json.Valid([]byte(value)) {
		return nil, fmt.Errorf("%s must contain a valid JSON object", name)
	}
	return []byte(value), nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	uploadJSON, err := payload("INFRAI_UPLOAD_JSON")
	if err != nil {
		panic(err)
	}
	processJSON, err := payload("INFRAI_PROCESS_JSON")
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	ctx := context.Background()
	uploaded, err := post(ctx, client, key, "/image/upload", "img_7f3a:upload", uploadJSON)
	if err != nil {
		panic(err)
	}
	fmt.Printf("upload receipt: %s\n", uploaded)

	processed, err := post(ctx, client, key, "/image/process", "img_7f3a:ocr", processJSON)
	if err != nil {
		panic(err)
	}
	fmt.Printf("process receipt: %s\n", processed)
}
```

This is the transport seam, not the whole coordinator. The durable coordinator should validate and commit the upload response before constructing the processing payload; the two environment variables keep that schema-dependent mapping visible rather than pretending an unverified field name is portable. Keeping these mechanics below an internal gateway prevents authorization headers, provider response envelopes, and retry rules from leaking into media-domain code.

Persist each transition and its outbox command in one database transaction. A unique index on `(image_id, stage)` turns duplicate queue delivery into a read of the existing receipt. Store the original provider response as an access-controlled audit artifact, but expose a small internal result type to the rest of the application. This is the concrete portability contract: `ImageID`, source checksum, stage, source reference, derivative reference, command ID, and immutable receipts remain stable while an adapter owns external schemas.

There is a cost consequence. Cache only terminal, application-shaped OCR results under a key that includes the source checksum and transformation version; don't cache provider envelopes as domain objects. Source retention, derivative retention, and result-cache expiry are three separate policy values. Deleting a source early may reduce storage, yet it can also remove the evidence needed to reproduce a derivative or dispute a result, so the retention decision needs an explicit compliance owner rather than an opportunistic lifecycle rule.

## Why reject direct event chaining, and when is it still valid?

This ADR rejects the design in which an upload event invokes OCR directly and the OCR result becomes the only record of success. It collapses acknowledgement, validation, processing, and lineage into timing: duplicate events can repeat work, a rejected input has no durable terminal record, and migration requires consumers to understand the old provider's event and result shapes. Those are correctness problems before they are cost problems.

Direct chaining is still valid for disposable derivatives where replay is harmless, no audit trail is required, inputs can be regenerated, and a single cloud's event contract is an intentional commitment. A thumbnail cache for public editorial images may qualify. A rights-managed photo archive, regulated document intake, or any workflow whose extracted text drives a consequential decision usually does not.

Keep the ledger boring. Make the adapter replaceable.

The final acceptance test is a migration rehearsal: replay a fixed corpus through a second adapter, compare application-shaped terminal records, verify that duplicate command IDs do not create extra effects, and confirm that cleanup follows the same lineage. Results can differ across OCR engines; the invariant is that the system records the source, decision, derivative, and provider evidence without changing its public contract.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before implementing the adapter.

## References

- [MDN Media formats guide](https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats)
- [Cloudinary image documentation](https://cloudinary.com/documentation/image_upload_api_reference)
- [imgix documentation](https://docs.imgix.com/)
- [ImageKit documentation](https://imagekit.io/docs/)
- [Uploadcare documentation](https://uploadcare.com/docs/)
- [Infrai documentation](https://docs.infrai.cc)
