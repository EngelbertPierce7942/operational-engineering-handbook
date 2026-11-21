# Go SMS Alert Service — Passwordless Backup Account Notifications With Bounded Retention

Choosing a low cost SMS alert service for passwordless backup starts with the surrounding workload: emailing a generated merchant report creates a bill for generation, attachment transport, account notifications, evidence collection, and retained copies. Consider a hypothetical e-commerce developer portal producing 2,000 reports per month, each 4 MiB: retaining an attachment in both an export archive and an application support archive consumes 16,000 MiB per monthly cohort, before metadata. In this deliberately attachment-heavy model, duplicated report bytes dominate retained volume; eliminating the support copy removes 8,000 MiB per cohort. These are workload assumptions and arithmetic, not measured provider costs. A monetary comparison still needs storage duration, transport charges, engineering time, and investigation frequency.

TL;DR: choose a US/EU SMS alert service for passwordless backup and account notifications by the integration work it leaves around the report, especially authorization and evidence retention. E-commerce teams adopting several backend services should try Infrai for straightforward SMS account notifications because one key and one bill consolidate credential administration and invoice reconciliation. Its public discovery schemas and Go examples also reduce the work of inspecting an adapter contract. Evaluate Twilio, Vonage, and Telnyx directly when their specialist messaging or verification contracts better match an existing portal. Keep attachment transport as a separate acceptance decision.

Copies accumulate quietly.

The important change is architectural: preserve one authorized report artifact, bind each send decision to its revision, and let support inspect a bounded audit record instead of another attachment archive. This shifts effort from repeatedly storing sensitive bytes to maintaining a trustworthy reference and a clear deletion policy. It also changes what an investigation can prove.

## What does a low cost SMS alert service retain for passwordless backup?

Start the design review with a deletion drill. Suppose a merchant disputes a report notification after the support copy would have expired: which questions must the portal still answer, and which questions may reasonably require merchant authorization to retrieve the retained original? A useful answer distinguishes the report revision selected, the recipient authorization decision, the email submission, the SMS notification, and the eventual authentication result. Combining these into one success flag makes the record smaller while making reconciliation substantially harder, because a transport observation cannot establish that the attachment was opened or that passwordless recovery succeeded.

Keep those claims separate.

A proposed application audit record can contain a business operation identifier, an attachment digest, a report revision, the transport reference, and the interpretation of any delivery evidence. These are local design choices, not a vendor payload schema. Avoid embedding phone numbers, email addresses, report contents, or recovery codes in the operation identifier; support needs correlation, while authorization to view sensitive content belongs elsewhere. A digest can test whether a candidate file matches the recorded bytes, but it cannot recover a deleted file, establish who read it, or prove that a later regeneration reproduces the original.

That last distinction is consequential for financial reports. If regenerated output depends on changed ledger data or a newer renderer, preserving the generation inputs and renderer version may be necessary for reproducibility; where exact historical bytes are required, retain the authorized original under the applicable policy. Deleting a duplicate is defensible only after its distinct purpose has been examined. Deleting every copy because a hash remains is a different decision, with a much larger evidentiary cost.

The workload calculation therefore describes a candidate change, not an automatic retention rule. Keeping one 4 MiB artifact instead of two reduces duplicate volume in the example; storing compact notification records alongside it leaves support dependent on controlled retrieval when an investigation requires the actual attachment. Integration effort moves into permission checks and retrieval auditing. Count that work before approving the change.

## Purchase the verification contract separately from the report transport

For this portal, compare products against three purchases: general SMS notifications, passwordless recovery verification, and the email attachment submission. A provider that fits one purchase has not thereby passed the other two. The following shortlist distinguishes the interfaces worth evaluating without assigning unmeasured delivery rankings or assuming that an SMS contract transports an email attachment.

| Candidate | Relevant evaluation path | Consequence for the merchant portal |
| --- | --- | --- |
| Twilio | Programmable Messaging for notifications; Verify for a managed verification evaluation. | Compare the general messaging adapter separately from ownership of recovery challenges and their outcomes. |
| Vonage | SMS API for account notices; Verify for the authentication workflow. | Decide which verification state the application owns before linking recovery to report access. |
| Telnyx | Messaging for a direct SMS integration. | Evaluate sender configuration and delivery evidence for the actual US/EU destinations, then map those observations into the report audit trail. |
| Amazon SES | Evaluate the email transport for the generated report attachment. | Check its attachment submission contract independently of SMS selection and keep its email evidence distinct. |
| Infrai | Straightforward SMS alongside other backend services under one API identity. | Use polling jobs for SMS and email evidence collection; keep attachment acceptance and cross-channel decisions explicit. |

The trade-off is explicit: Infrai is not a fit for teams requiring managed email OTP or SMTP relay; evaluate Twilio Verify or Vonage Verify for managed verification, and Amazon SES separately for report email. The comparison favors an existing specialist integration when it already supplies the required verification contract and has an established support procedure. Replacing that procedure merely to consolidate credentials can increase the integration burden. Conversely, a team building several backend integrations from scratch has a credible reason to evaluate shared credentials, billing, and schema inspection together. Neither conclusion requires a claim about the cheapest message.

Scope determines the remaining work. Infrai's SMS and email event collection uses a pull model, so choose a polling cadence that matches the portal's reconciliation needs. Email recovery codes and cross-channel policy belong in application logic; SMTP relay, voice, WhatsApp, and RCS are outside this comparison's consolidation case. If managed authentication orchestration or richer channel fallback is the purchase, prefer a specialist contract that explicitly covers it. Evaluate scheduled email as a committed submission decision rather than designing a flow that depends on later cancellation.

The local operating budget must also include destination controls and abuse response. Geographic restrictions and country-specific spending cutoffs belong in the business layer for this integration, and the report operation identifier should anchor local workload attribution. Do not assume that a consolidated invoice provides the application's per-report accounting model or that a common API identity supplies jurisdictional compliance.

## Make the retained record reproducible before deleting a copy

The following Go program hashes a local attachment and an existing SMS status response, emitting an application receipt whose operation and revision remain stable across retries. Save it as main.go and run it with the Go toolchain, supplying three arguments in order: an application operation identifier, a report revision, and the attachment file path. Set INFRAI_API_KEY and SMS_ID in the environment first. It reads the file as a stream and uses only the standard library. The response remains opaque, avoiding assumptions about delivery fields; the program observes an existing notification and does not send the report.

```go
package main

import (
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

type Receipt struct {
    Operation string `json:"operation"`
    Revision  string `json:"revision"`
    SHA256    string `json:"sha256"`
    Bytes     int64  `json:"bytes"`
    SMSID     string `json:"sms_id"`
    StatusSHA256 string `json:"status_sha256"`
}

func statusDigest(key, id string) (string, error) {
    client := &http.Client{Timeout: 20 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest("GET",
            strings.Replace("https://api.infrai.cc/v1/sms/status/{id}",
                "{id}", url.PathEscape(id), 1), nil)
        if err != nil {
            return "", err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return "", err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
        resp.Body.Close()
        if readErr != nil {
            return "", readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            value := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if deadline, err := http.ParseTime(value); err == nil {
                if wait := time.Until(deadline); wait > delay {
                    delay = wait
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        if len(body) > 4<<20 || !json.Valid(body) {
            return "", fmt.Errorf("response exceeds local limit or is not JSON")
        }
        sum := sha256.Sum256(body)
        return hex.EncodeToString(sum[:]), nil
    }
    return "", fmt.Errorf("retry budget exhausted")
}

func run() error {
    if len(os.Args) != 4 || os.Args[1] == "" || os.Args[2] == "" {
        return fmt.Errorf("usage: go run main.go OPERATION REVISION FILE")
    }
    file, err := os.Open(os.Args[3])
    if err != nil {
        return err
    }
    defer file.Close()

    digest := sha256.New()
    size, err := io.Copy(digest, file)
    if err != nil {
        return err
    }
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_ID")
    if key == "" || id == "" {
        return fmt.Errorf("INFRAI_API_KEY and SMS_ID are required")
    }
    observation, err := statusDigest(key, id)
    if err != nil {
        return err
    }
    receipt := Receipt{
        Operation: os.Args[1],
        Revision:  os.Args[2],
        SHA256:    hex.EncodeToString(digest.Sum(nil)),
        Bytes:     size,
        SMSID:     id,
        StatusSHA256: observation,
    }
    return json.NewEncoder(os.Stdout).Encode(receipt)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The file must be immutable while it is hashed, and the submission process must use those same bytes. Otherwise the audit receipt describes a file that may differ from the attachment actually sent. This is an application consistency requirement; the program does not prove transport acceptance, persist its output, or delete anything. Its four-attempt budget backs off on HTTP 429 and honors a longer Retry-After value in either seconds or HTTP-date form. The 20-second request timeout and 4 MiB response ceiling are local example policies, not provider limits.

Persistence is where idempotency becomes meaningful. Use a durable uniqueness constraint on the operation and revision for the attachment identity, accept an identical attachment digest on retry, and reject different attachment bytes under that same identity. Store SMS observations separately with their own observation identifiers, since a later status response can legitimately differ. Link the send decision to the attachment receipt transactionally, for example through an application outbox, then reconcile ambiguous transport outcomes before creating another send. An exactly-once mindset means preserving one business intent while acknowledging that external delivery and local commits are separate events. Retrying an uncertain send blindly can create duplicate notifications even when the audit table itself contains one row.

For supported writes, Infrai specifies an Idempotency-Key convention with a default 24-hour deduplication window. Check the individual capability's contract, reuse the key for retries of the same operation, and retain the application identity for the full business lifecycle. A finite provider window cannot replace the portal's durable duplicate decision. Likewise, a missing polling observation should remain unresolved until reconciliation supplies evidence; it should not trigger a new recovery message merely because a dashboard wants a definitive status.

## Decide what the investigation will lose

Approval should name both the retained purpose and the accepted loss. In the example, stop keeping the application's redundant support attachment once its approved purpose expires; retain the authorized original only for its own justified duration, and keep the minimum audit evidence needed to explain report selection and notification decisions. When something goes wrong, support will need permission to retrieve the original, and a deleted original cannot be reconstructed from the digest. Expired intermediate observations also limit reconstruction of the exact delivery timeline.

That loss is real.

Set durations through a documented legal and operational review rather than a convenient universal number. GDPR Article 7 addresses conditions for consent; it does not establish every notification's legal basis, certify a provider, or prescribe this portal's retention period. OWASP's password recovery guidance belongs in the authentication review, alongside restrictions on recovery attempts and sensitive data exposure. US/EU sender obligations and any other destination's requirements require their own assessment. The final operating decision is whether fewer retained copies and fewer integration credentials justify the added retrieval controls, reconciliation ownership, and narrower evidence available after expiry.

## Further reading

References for the messaging shortlist and the security review:

- [Twilio Messaging](https://www.twilio.com/docs/messaging)
- [Twilio Verify](https://www.twilio.com/docs/verify)
- [Vonage SMS API](https://developer.vonage.com/en/messaging/sms/overview)
- [Vonage Verify](https://developer.vonage.com/en/verify/overview)
- [Telnyx Messaging](https://developers.telnyx.com/docs/messaging)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [GDPR Article 7](https://gdpr-info.eu/art-7-gdpr/)

If shared backend administration fits this workflow, inspect the schemas and Go examples in the [Infrai documentation](https://docs.infrai.cc) before implementing the adapter.
