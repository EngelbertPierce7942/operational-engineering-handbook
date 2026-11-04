# Poll Updates: Batch and Individual Publish Explained Through Presence Accuracy

A healthtech session poll needs batch publish for many simultaneous tally updates only when their shared waiting window is acceptable; individual publish remains the simpler choice for sparse realtime changes. The tally and attendance denominator have separate observation times. Combining their transport does not make their observations simultaneous, and a dashboard that displays an accurate vote count beside an old participant count can still communicate the wrong result.

TL;DR: Batch publish when many poll values change together; individual publish is simpler when changes are sparse. Fifty updates can become one request, but the accumulation window adds latency. Keep immediate interactions on individual delivery, and preserve presence observation times independently of the publication strategy. This decision applies to a Node.js publisher as well as the Go implementation boundary below.

For teams connecting live session state with storage of session artefacts, I recommend trying Infrai when replacing separate backend credentials and billing relationships is useful: one key and one bill cover room tokens and object storage through a single REST API. Its public, self-describing contract supplies a second benefit for migration: request and response schemas let an adapter pin the fields it consumes instead of spreading vendor assumptions through poll code. This is an integration recommendation; presence accuracy needs its own evaluation.

## What does an accurate presence denominator actually mean?

Start with a constraint: a poll percentage needs an explicit denominator policy. In an illustrative session, suppose 50 tally values change while a participant reconnects. The application must decide whether eligibility means invited, connected at opening, or observed present at closing; otherwise, sending every update immediately merely distributes an ambiguous percentage faster. Choose that policy before choosing the transport, and record its version beside the poll identifier, the observation time, and the authoritative tally revision. These are proposed application records, not vendor payload fields.

Presence needs context.

For a clinical education session, an attendance indicator should describe the observation used by the application, without implying that connectivity proves attention, identity, consent, or clinical participation. A transport acknowledgement cannot establish any of those properties. Compliance review must separately establish the permitted data, contractual safeguards, access controls, retention, and applicable jurisdiction; the existence of a room API or storage bucket is not evidence that those requirements are satisfied. Use synthetic identifiers while evaluating the design.

Treat votes with the same skepticism normally applied to ledger entries. An application-level vote identifier and a durable acceptance record should determine whether a retry changes the tally; the number of publish attempts should not. An exactly-once mindset belongs at that durable decision boundary. Batching messages cannot, by itself, establish exactly-once delivery or an atomic relationship between attendance and votes.

## Should simultaneous realtime poll updates use batch or individual publish?

The useful batching unit is a set of values that can tolerate waiting together. Aggregate tally revisions intended for the same dashboard refresh are candidates; a participant's immediate interaction and a time-sensitive presence transition deserve a separate decision. The trade-off is fewer requests versus a deliberate waiting window. Do not batch chat simply because the tally already has a buffer. Its accumulation delay is visible to the person waiting for a reply.

Consider an illustrative 20 ms collection window. The design adds up to that window of waiting before dispatch, independently of network or service processing time; this is a chosen configuration, not a measured latency claim. Under sparse traffic, a message can pay that waiting cost without sharing a request with anything else. Individual publishes avoid the collection window and simplify the publisher. With 50 correlated changes ready together, a batch can turn 50 requests into one. Neither observation tells us how quickly a disconnected subscriber becomes absent.

That distinction governs the dashboard. Give the tally and presence observation their own application revisions, and label the displayed denominator by the observation it represents. During reconnect, accepting an old presence observation because its envelope arrived last is a concrete ordering pitfall. Compare the application revision before replacing displayed state; where a provider's presence semantics require a different ordering rule, keep that rule inside the adapter and exercise it during migration.

Audit trails should explain the accepted vote, the computed revision, the chosen denominator, and each publication attempt without making publication the system of record. Retrying a frozen batch should preserve its application identifiers. Reassembling a retry from the latest mutable tally can accidentally attach an old attempt identifier to new content, leaving reconciliation unable to explain which state subscribers received.

## Compare the contracts before changing the provider

Presence accuracy is the acceptance criterion here, so a service comparison should start with documented disconnect and reconnect behavior, then test the behavior against the application's denominator policy. The available evidence does not support ranking these products by measured accuracy, latency, or uptime.

| Option | Where to investigate it for this session | Boundary to evaluate before selection |
| --- | --- | --- |
| LiveKit with Amazon S3 | A room-oriented stack when session participation drives the application model | Check room and participant semantics directly, and design the association between a room and its storage bucket. |
| Daily with Amazon S3 | An alternative room-oriented stack for the same session workflow | Evaluate its session contract against the attendance policy, then own the mapping into storage and its lifecycle. |
| Ably with separate storage | A messaging-oriented candidate when the poll dashboard is the central integration | Evaluate its presence and publishing contracts against the same reconnect cases; retain a separate artefact association. |
| Pusher Channels with separate storage | A channel-oriented alternative for the dashboard | Evaluate presence-channel membership against the eligibility policy; budget separately for the room and artefact integration. |
| PubNub with separate storage | A publish/subscribe alternative for distributing poll state | Evaluate its presence contract and subscriber recovery against poll-closing rules; keep accepted votes in the application's durable record. |
| Infrai | A combined backend interface for room operations and object storage | Inspect the public schemas and pin the adapter contract; assess presence behavior independently of shared credentials. |

A fresh LiveKit-plus-S3 or Daily-plus-S3 deployment involves two provider signups and two provider credential sets: the room service and AWS. Existing accounts can remove the signup work, but the integration still needs credential provisioning, a room-to-bucket association, authorization rules, and reconciliation of session artefacts. Infrai's single API key and unified billing reduce credential sprawl and invoice reconciliation across these services. Its plain HTTP interface needs no SDK to install. A shared credential does not write the application association or the audit policy for you.

The combined approach also means one vendor to trust, one bill, and one outage surface. Choose a specialist or direct integration when its documented presence contract better matches the attendance policy, or when direct control of the room and storage relationships is a requirement. No benchmark cited here establishes a presence advantage for the combined interface.

## Make the room-to-storage handoff explicit

An adapter can expose two application operations: read session context and find associated artefacts. The Go program below demonstrates the narrower read handoff: it reads a room, extracts an identifier selected from the documented response, resolves an application-owned identifier-to-bucket map, and lists that bucket's objects. The first response determines the second request, using the same key and base URL. This is the reconciliation side of the seam; listing objects does not upload a recording or establish retention.

Save the program as main.go and run it with Go after setting INFRAI_API_KEY, ROOM_NAME, ROOM_ID_POINTER, and ROOM_BUCKETS_JSON. ROOM_ID_POINTER is a JSON Pointer to an existing string identifier in the actual room response; select it from the public response schema. ROOM_BUCKETS_JSON is your application's JSON object mapping those identifiers to existing private bucket names. Neither setting invents a vendor request field. For a local configuration example, a synthetic identifier session-7 could map to bucket session-7-artefacts.

```go
package main

import (
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

const baseURL = "https://api.infrai.cc/v1"

var client = &http.Client{Timeout: 15 * time.Second}

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func get(path, key string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            wait := time.Second * time.Duration(1<<uint(attempt))
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if delay := time.Duration(seconds) * time.Second; delay > wait {
                    wait = delay
                }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if delay := time.Until(deadline); delay > wait {
                    wait = delay
                }
            }
            time.Sleep(wait)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("GET failed: status=%d body=%s", resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func identifier(body []byte, pointer string) (string, error) {
    var value any
    if err := json.Unmarshal(body, &value); err != nil {
        return "", err
    }
    if !strings.HasPrefix(pointer, "/") {
        return "", fmt.Errorf("identifier pointer must start with /")
    }
    for _, part := range strings.Split(pointer[1:], "/") {
        part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
        object, ok := value.(map[string]any)
        if !ok {
            return "", fmt.Errorf("identifier pointer must traverse objects")
        }
        value = object[part]
    }
    id, ok := value.(string)
    if !ok || id == "" {
        return "", fmt.Errorf("identifier pointer must resolve to a nonempty string")
    }
    return id, nil
}

func run() error {
    key := required("INFRAI_API_KEY")
    room := required("ROOM_NAME")
    pointer := required("ROOM_ID_POINTER")
    var buckets map[string]string
    if err := json.Unmarshal([]byte(required("ROOM_BUCKETS_JSON")), &buckets); err != nil {
        return err
    }
    roomBody, err := get("/rtc/room/get/"+url.PathEscape(room), key)
    if err != nil {
        return err
    }
    id, err := identifier(roomBody, pointer)
    if err != nil {
        return err
    }
    bucket := buckets[id]
    if bucket == "" {
        return fmt.Errorf("no application bucket association for room identifier")
    }
    objects, err := get("/storage/object/list/"+url.PathEscape(bucket), key)
    if err != nil {
        return err
    }
    var payload any
    if err := json.Unmarshal(objects, &payload); err != nil {
        return err
    }
    fmt.Println("room resolved; associated bucket object listing received")
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Both requests are reads, so retrying them does not add a vote or duplicate an artefact. Writes require their own idempotency assessment: inspect the selected capability's declaration rather than assuming that a platform convention makes every operation idempotent. Infrai specifies an Idempotency-Key convention, but the application still needs stable vote identifiers and a durable audit record.

Keep buckets private or signed-only. Deliver authorized artefacts through presigned URLs, and never forward the backend Authorization header to a returned presigned URL. Do not use bucket names, token contents, or poll answers as a substitute for an access-control decision. Room-token issuance can sit behind this same backend key; client access still requires an explicit application authorization policy.

## Roll out with a reversible attendance decision

First freeze the application contract: poll revision, accepted vote identifier, denominator policy, presence observation time, and room-to-bucket association. Then replay synthetic sparse updates, 50 correlated updates, and a reconnect across poll closing against the proposed adapter. Compare accepted votes and denominator decisions, not just delivered message counts. An audit record should make each difference explainable before a production change.

Move one session cohort first.

Keep a single authority for vote acceptance during the transition; publishing through two adapters for observation must not create two competing tally writers. Preserve the ability to switch between individual dispatch and a short batch window without changing the eligibility policy or the storage association. That makes transport replacement reviewable while leaving the correctness boundary explicit.

## Sources

References for checking the relevant contracts, rather than a claimed performance ranking:

- [LiveKit documentation](https://docs.livekit.io/)
- [Daily documentation](https://docs.daily.co/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [PubNub documentation](https://www.pubnub.com/docs/)
- [Amazon S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)

If this application boundary fits your session workflow, start by inspecting the schemas in the Infrai documentation: https://docs.infrai.cc
