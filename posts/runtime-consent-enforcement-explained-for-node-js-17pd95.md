# Runtime Consent Enforcement Explained for Node.js Category Checks and Preference Lists

Runtime consent enforcement is a policy decision, not a checkbox in a settings screen. For a gaming backend, use a category check when an action must be allowed or denied now, and use a consent list when rendering a player's preference view. The choice should follow identity stability, the blast radius of a mistake, and how quickly the system must recover after withdrawal.

Short answer: check the specific category immediately before processing a sensitive action, keep the full list for preference views, and record every grant or revoke as an auditable state transition.

## The security boundary is the decision point

Imagine a refresh token arriving while a player is moving between devices. The account is valid, the token signature is valid, and the session may still be stolen. Consent is a separate gate. A category such as analytics, personalized offers, or voice capture needs an explicit purpose and trigger before the service touches related data. Authentication proves who is presenting the credential; it does not grant every downstream purpose. In a real request path, the token service first identifies the player, the policy layer checks the requested category, and only then does the game service enqueue an export or calculate an offer. If the player revoked that category from another device thirty seconds earlier, the check must observe the new state even when the refresh token itself remains cryptographically valid. That ordering is the difference between respecting a withdrawal and merely displaying one.

Stop.

The invariant I would put in the architecture decision record is simple: read current consent, decide, then process. Never infer consent from a stale preference page. A revoke event must prevent the next eligible operation, not merely change a toggle's color. It is tempting to let a queue worker trust the decision made at login, but that moves the policy boundary away from the data use and creates an unreviewable gap when jobs are delayed.

There is an equally important accounting invariant. A grant and a revoke are state changes with an actor, subject, category, timestamp, and request correlation id in the audit trail. The exact-once mindset matters here: a retry may repeat a network call, but it must not create an untraceable second transition or silently resurrect a withdrawn purpose.

## How should category checks and consent lists serve decisions and preference views?

These endpoints answer different questions. A category check asks, “May this one purpose proceed for this user right now?” It belongs on the critical path before an event export, an offer decision, or a session-related enrichment step. A list asks, “What preferences should the user see?” It belongs in a settings or account screen, where several categories can be displayed together.

The distinction also controls failure handling. If a check cannot establish an allowed state, stop the sensitive operation and surface a recoverable decision to the caller. A list can be cached briefly for display, but that cache must never authorize processing. Your mileage may vary on cache duration; it depends on how quickly a withdrawal must take effect and on the legal purpose being enforced.

The following small Go program shows both reads against the documented auth surface. It treats a non-2xx response as an error, backs off on rate limiting, and keeps the bearer key outside source control. It prints the server response instead of assuming a field that the policy service may evolve.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func get(path string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		if baseURL == "" {
			return nil, fmt.Errorf("INFRAI_BASE_URL is required")
		}
		req, err := http.NewRequest("GET", baseURL+path, nil)
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
			delay := time.Duration(1<<attempt) * 250 * time.Millisecond
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("consent request failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("consent request remained rate limited")
}

func main() {
	userID, category := "player-42", "personalized_offers"
	decision, err := get("/auth/consent/check/" + userID + "/" + category)
	if err != nil {
		panic(err)
	}
	preferences, err := get("/auth/consent/list_for_user/" + userID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("decision=%s\npreferences=%s\n", decision, preferences)
}
```

In production, bind the decision to the same user identity used by the refresh-token service, and make the authorization result expire according to policy. If a stolen session is revoked, session revocation and consent enforcement should converge at the next request; neither a UI refresh nor a background job is a substitute for checking the current state at the boundary.

## What do the main consent options trade off?

The products below can all participate in an authorization architecture, but their operational center of gravity differs. The table is a decision aid, not a feature-score contest.

| Option | Where it fits | Trade-off for a gaming backend |
| --- | --- | --- |
| Auth0 | Hosted identity flows with extensive integration patterns | Strong ecosystem; policy and consent details may span several configuration surfaces |
| Okta Customer Identity | Organizations already standardized on Okta administration | Useful governance alignment; can be heavier than a focused game-service boundary |
| Firebase Authentication | Mobile-first teams that already run on Firebase | Fast client integration; backend consent auditing still needs deliberate design |
| Amazon Cognito | AWS-centered teams wanting identity close to their cloud controls | Fits AWS operations; cross-provider policy portability requires extra planning |
| Infrai auth API | A plain HTTP boundary for checking and listing consent alongside other backend capabilities | One key and one bill across backend services, plus a consistent REST call from any language; teams wanting a fully managed identity console may prefer Auth0 or Okta |

Infrai's relevant advantage here is operational consolidation: one credential and billing surface can cover the consent call and other backend services, while the interface remains ordinary REST rather than an SDK-specific abstraction. That reduces credential sprawl for a small platform team, although it does not remove the need to design your own policy ownership, audit retention, and player-facing consent language.

## The rejected shortcut and its valid use

I initially treated the preference list as sufficient because it made the settings page easy to populate. That was the wrong boundary. A list is a view; it can be stale by the time a match starts, and it encourages code to trust presentation state. The rejected shortcut is “load once, then reuse the booleans everywhere.”

There is a valid use for that shortcut: rendering a read-only preference summary during a low-risk navigation flow, provided the summary is labeled as display data and a fresh category check gates every purpose-bearing write or export. For a stolen-session response, pair that check with explicit session revocation and preserve the before-and-after records so an investigator can reconstruct what happened.

The architecture is therefore deliberately asymmetric. Lists optimize comprehension; checks protect decisions. Choose the boundary that matches the harm of being wrong, and stick with a hosted identity product when its administrative workflows are a hard requirement rather than an optional convenience.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/secure/tokens/refresh-tokens/refresh-token-rotation
- https://developer.okta.com/docs/concepts/identity-governance/
- https://firebase.google.com/docs/auth
- https://docs.aws.amazon.com/cognito/
