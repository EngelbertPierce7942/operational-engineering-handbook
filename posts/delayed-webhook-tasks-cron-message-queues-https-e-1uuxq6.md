# Delayed Webhook Tasks: Cron, Message Queues, HTTPS Endpoints, and Long Jobs

Short answer: for an edtech payment reconciliation, use a recurring cron trigger to start a bounded nightly scan, and use a message queue for each delayed webhook obligation; keep public HTTPS handling short and move long-running work to restartable workers.

The useful distinction is not “cron versus queue” as a matter of fashion. It is whether the system is recovering a recurring interval or recovering an individual obligation. A reconciliation run must be able to answer which provider records were examined, which were ambiguous, and which batch can be resumed. A delayed webhook task must retain its event identity, due time, attempt history, and final disposition. Those are different recovery units.

## What evidence does a nightly edtech reconciliation keep for cron and message queues?

Cron is a clock. It can call a public HTTP endpoint on a fixed cadence, but it does not make the work complete and it does not create a durable record for every event. For a nightly payment-provider reconciliation, the endpoint should calculate a deterministic window from durable checkpoints, create or find a reconciliation batch, and enqueue bounded partitions. If the trigger is late or absent, the next invocation should derive the missing interval from stored state; it should not assume that a sequence of clock ticks is the ledger of what happened.

A queue is an obligation ledger only in the loose operational sense. A message can identify one delayed webhook task and tell a worker which durable record to load when the task is due. The application database remains authoritative for the payload, payment state, and audit evidence. This matters because a message queue commonly provides at-least-once delivery: a worker may see the same event again after it commits business state but before it acknowledges the message.

The decision rule is therefore narrow. Use cron to discover work from time and durable state; use a queue to distribute independently retryable work; use neither as the financial system of record.

Exactly once is an application invariant, not a transport setting.

For each webhook event, persist an idempotency key with a uniqueness constraint, and commit that claim in the same database transaction as the terminal business transition and its audit append. A duplicate delivery can then produce another attempt record without producing another ledger effect. The transactional outbox pattern extends the same discipline to publication: the state change and the fact that a message should be published are recorded together, after which a relay can retry publication.

## What public HTTPS endpoint limits matter before scheduling long-running jobs?

The public endpoint is an ingress boundary, not a worker pool. A push subscription needs a publicly reachable HTTPS target, while a cron target is also an HTTP endpoint; either way, authentication, certificate rotation, request size, rate limiting, and sensitive logging belong in the threat model. A private consumer cannot receive a push request directly, so the design must either expose a narrowly scoped ingress or have a pull worker read from the queue inside the private network.

The handler should acknowledge the trigger after it has recorded the batch and handed off bounded work. It should not wait for provider pagination, reconciliation decisions, or webhook delivery. A 900-second execution ceiling makes this more than a performance preference: a long-running job needs a coordinator that can return early, plus workers whose attempts are independently visible and retryable.

Keep transport messages small. A delayed message can be limited to seven days, a message body to 256 KB, and retention to 30 days under the queue semantics used here; these limits are reasons to store a compact batch or event reference rather than copying a payment payload into every message. Work with a due date beyond the delay horizon belongs in durable application state and can be swept into the queue when it enters that horizon. The sweep is a recovery mechanism, not one cron schedule per event.

The compliance boundary deserves equal care. I am not sure a generic queue retention period can satisfy a particular education-payment or card-program obligation without the applicable control framework and retention schedule. Preserve the evidence required by policy in the application domain, and treat queue deletion after acknowledgment as transport cleanup rather than records disposal.

## The Go implementation starts with a recoverable batch

Begin with a batch row containing a stable batch ID, the provider time window, a cursor or partition marker, and a state such as `open`, `reconciling`, or `complete`. Each partition gets an idempotent task key. A worker loads the provider record and the local payment record, writes a comparison result, and advances the partition only in the transaction that makes that result durable. If the process stops, recovery starts from the last committed partition rather than from an inexact wall-clock guess.

The audit trail should distinguish facts from decisions: “provider record observed,” “local record missing,” “amount differs,” and “manual review closed” are not interchangeable. For webhook delivery, record the event ID, destination, attempt number, response class, and next action without storing more sensitive content than the investigation requires. A retry is then an observable state transition, not an invisible loop.

Here is a deliberately small Go example of the claim that prevents a duplicate business effect. The map stands in for a database uniqueness constraint; it is not a production persistence layer.

```go
package main

import "fmt"

type Task struct {
	EventID string
	Amount  int64
}

type Reconciler struct {
	claimed map[string]struct{}
	balance int64
}

func (r *Reconciler) Apply(task Task) string {
	key := "webhook:" + task.EventID
	if _, exists := r.claimed[key]; exists {
		return "duplicate attempt recorded"
	}

	r.claimed[key] = struct{}{}
	r.balance += task.Amount
	return "business effect applied"
}

func main() {
	r := &Reconciler{claimed: make(map[string]struct{})}
	task := Task{EventID: "evt_104729", Amount: 1250}

	fmt.Println(r.Apply(task))
	fmt.Println(r.Apply(task))
	fmt.Println("balance:", r.balance)
}
```

In production, `Apply` must be part of one database transaction, and the worker must acknowledge only after that transaction commits. A crash before commit should permit retry; a lost acknowledgment after commit should create a harmless duplicate attempt. A process-local mutex or a separate cache lock cannot prove that the ledger mutation and the idempotency claim committed together.

## What are the trade-offs for cron, queues, and public webhook delivery?

The comparison should follow the recovery unit and the failure model.

| Mechanism | Good fit | Recovery concern |
| --- | --- | --- |
| Recurring cron trigger | Start a nightly reconciliation or periodic sweep | Missed invocations may not backfill themselves; checkpoints must define the interval to revisit |
| Delayed message queue | Hold one retryable webhook or reconciliation partition until due | At-least-once delivery requires idempotent consumers and an audit record outside transport |
| Public HTTPS ingress | Receive a push request and enqueue bounded work | It expands the untrusted network boundary and must remain a short, authenticated coordinator |
| Pull worker | Consume private work without exposing the consumer directly | The team owns polling, visibility, retry, and capacity controls |
| Workflow engine | Coordinate dependent, long-lived steps and human decisions | More operational machinery than an independent delayed callback needs |

The catch is that a queue is not a replayable ledger, and cron is not per-event state. Choose a queue when each event needs an independent retry and disposition; choose a recurring sweep when durable state can derive the obligations and recovery should cover a time window; choose a workflow abstraction when the reconciliation itself has durable dependent stages or human review. A public endpoint is unsuitable when exposing any ingress is unacceptable under the network or compliance model; use a private pull path and make the trigger internal instead.

Pricing should not decide this architecture. Delivery semantics, evidence retention, recovery time, and the cost of operating another worker path will dominate a reconciliation incident. Your mileage may vary on the right partition size because provider pagination, rate limits, and the local database shape determine how much work fits comfortably inside one attempt.

## Roll out by proving recovery

Start with one payment-provider window and one webhook event type. Before deployment, write the invariant in a test: duplicate delivery may create multiple attempt records, but one event ID may produce only one terminal ledger transition. Test a crash before the transaction commits, a crash after commit but before acknowledgment, a missing cron invocation, a provider timeout, and a partition that exceeds its worker deadline.

Then verify operational recovery with identifiers rather than dashboards alone. An operator should be able to join the reconciliation batch, outbox row, queue task, idempotency claim, provider record, webhook receipt, and manual-review decision. Exercise the seven-day delay boundary and the 256 KB message boundary with representative references, not full payment payloads. Confirm that a job approaching 900 seconds is split and resumed, while the public HTTPS handler returns after durable handoff.

That is the decision that survives an incident: the scheduler supplies a clock, the queue supplies delivery attempts, and the application supplies truth.

## References

- AWS SQS FIFO queues documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- Transactional Outbox pattern: https://microservices.io/patterns/data/transactional-outbox.html
