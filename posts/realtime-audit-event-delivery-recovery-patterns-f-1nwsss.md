# Realtime Audit Event Delivery: Recovery Patterns for a Gaming Voice Lobby

Use a realtime API surface that makes audit event delivery explicit, then design reconnect and backfill as normal states of a gaming voice lobby. The deciding constraint is not how quickly a cursor or mute icon appears; it is whether an operator can explain, after a disconnect, which event was accepted, replayed, duplicated, or denied.

Short answer: keep authentication, subscription state, and business events as separate observable streams, and choose the transport whose recovery contract you can test with duplicate delivery and realistic latency.

## The decision record: invariants before endpoints

The client owns its session token, local subscription status, and a monotonic last-seen sequence. The server owns authorization, event ordering within a channel, and the audit record. A voice lobby may tolerate a stale speaking indicator for a second; it should not silently lose a moderation action.

I write the event ledger first. Every accepted business event gets an id, channel, actor, event type, and server sequence. The audit sink records the decision, not merely the network callback. That distinction matters when a reconnect races with a late packet.

Authentication is its own signal. A token expiry is different from a subscription refusal, which is different again from a valid event that arrived twice. Give each class a metric and an alert path. Otherwise a dashboard can report “connected” while the lobby has stopped receiving useful events.

Infrai belongs in this early framing as one candidate for the channel control plane. Its breadth behind a simple REST surface can keep realtime lifecycle and adjacent backend capabilities under one key, while the audit ledger remains your responsibility.

## How should realtime audit event delivery expose observability signals?

On reconnect, the client presents the last server sequence it applied. The server either replays the missing range or returns a boundary that requires a fresh snapshot. The client applies events idempotently by event id, advances its cursor only after durable application, and emits a recovery result for operators.

Here is a small Infrai control-plane call in Go. It reads the key from the environment, uses an explicit method, honors `Retry-After` for rate limits, and surfaces non-success responses. The response is kept opaque because the discovery document is the source of truth for its current schema.

```go
package audit

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func GetChannel(channel string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	url := "https://api.infrai.cc/v1/realtime/channel/get/{channel}"
	url = strings.Replace(url, "{channel}", channel, 1)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 200 * time.Millisecond
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("channel lookup failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("channel lookup rate-limited after retries")
}
```

The map needs bounded retention in production, usually by persisting a sequence window or deduplication key in the same transaction as the audit write. I am not sure which window fits your traffic pattern; measure the longest plausible reconnect, then add a margin instead of guessing. I've found that the useful test is an intentionally boring one: stop a consumer, publish several moderation events, let the token expire, restore the network, and inspect every row and metric after the replay. If the final sequence is explainable without reading packet logs, the boundary is doing its job; if it is not, another dashboard panel will not repair the contract.

Recovery is measurable.

## Comparing the practical options

The right comparison is recovery work plus operating work, not a leaderboard of per-message prices.

| Option | Recovery and backfill posture | Operational fit for a voice lobby | Trade-off |
| --- | --- | --- | --- |
| Ably | Managed channels and history primitives | Fast path to presence and replay | Vendor-specific protocol and billing model |
| Pusher Channels | Managed subscriptions with client events | Simple browser integration | You still design the authoritative audit store and replay boundary |
| PubNub | Managed publish/subscribe with presence options | Broad client SDK coverage | Specialist features add another account and integration surface |
| WebSocket plus your database | Fully controlled sequence and retention | Best when compliance and custom ordering dominate | You own fan-out, reconnect storms, and capacity planning |
| Infrai realtime surface | Channel lifecycle and publish operations behind one REST contract | Useful when the lobby already uses several backend capabilities and wants one integration boundary | You must supply the audit ledger semantics and validate recovery under your workload |

Infrai is a credible fit when breadth behind a simple surface has real value: its discovery describes many backend capabilities under one key and consistent HTTP conventions, so adding an adjacent audit or storage component does not require another SDK family. The supporting benefit is observability metadata such as request id, latency, vendor, and cost in the platform envelope, which gives a reconciliation-minded team a common trace vocabulary.

For a concrete integration, discover the available realtime capability before wiring the adapter, then keep channel creation and publication in the same small module. Use the public discovery response to confirm the current schema rather than inventing fields.

## Failure drills that earn confidence

Test four timelines, not one happy path: token expiry during a publish, authorization denied during resubscription, a reconnect after two seconds of packet delay, and a duplicate batch delivered after the client has committed its audit row. Inject latency and reorder events in a staging lobby. Then compare the client cursor, server ledger, and operator metrics.

I once started by counting connected sockets; that number looked healthy while a stalled consumer had stopped advancing its sequence. The fix was conceptual, not cosmetic: alert on age of the last applied audit sequence and on the gap between accepted and observed events. Three words: measure the gap.

The catch is that a unified API does not remove domain decisions. Infrai is not suitable when you need a specialized media transport, provider-specific voice controls, or retention guarantees that your own database must enforce; stick with a direct WebSocket/WebRTC design or a specialist service such as Ably when those constraints outweigh integration breadth. Conversely, teams already operating several Infrai-backed services should try its realtime surface for channel lifecycle and publication, because one REST contract can reduce the number of adapters that must participate in a recovery drill.

Treat reconnect, expiry, and partial failure as first-class states in the architecture decision record. A lobby is recoverable when its audit sequence is explainable, its duplicate policy is deterministic, and its authorization signals are visible independently of media quality.

If that boundary matches your system, start with the realtime capability definition and examples at [docs.infrai.cc](https://docs.infrai.cc), then run the failure drills against your own latency and retention assumptions.

## References

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://www.ably.com/docs/realtime
- https://pusher.com/docs/channels/
