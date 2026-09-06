# Backfill probe retention repair

Local runtime restart: 2026-09-05 19:31:46 UTC. Economic cohort, checkpoint
schedule, evidence policy and trading admission were not reset or modified.
This is an operational implementation boundary, not a new acceptance result.

The pre-change trace scanned 1,170 blocks in ten 125-block attempts. It spent
68.719 seconds yielding to live work and had a successful-chunk p95 of 5.672
seconds. No provider deferral was reported. Yield time is deliberate host/SQLite
isolation; it must not be interpreted as available capacity to reclaim blindly.

The adaptive controller could earn a larger probe after five successful chunks,
but the drain loop kept using its initial size throughout the run. Subsequent
small-chunk successes overwrote the pending probe. Short scheduler windows also
mistakenly taught the provider controller that the smaller size was its new stable
size.

The coordinator now consumes the controller's next size within the same run.
Successful scheduler-limited attempts leave the provider model unchanged, retaining
the probe for a suitable later window or worker. Failures still use the existing
rate-limit/timeout/preemption handling. Requested limits, the 250-block SQLite
commit cap, 15-second large-chunk window rule, lane deadline, completion reserve
and independent evidence service remain unchanged.

New telemetry distinguishes successful work seconds and blocks per successful
work second from cooperative waiting. A larger probe is an opportunity, not proof
of backlog convergence; validate actual blocks recovered versus arrivals over
several completed workers, alongside live and evidence health.

Regression coverage includes same-cycle probe consumption, retention across short
windows, and a probe remaining durable for the next worker. Existing lane tests
cover reserve, preemption, provider failure, caps and commit behavior.

## Cursor commit recovery

The historical scan is replay-safe, but its final queue-cursor transaction shares
the SQLite writer with live decisions. A transient `database is locked` at that
boundary previously converted a useful scan into a failed worker and forced the
range to be replayed. Cursor commits now retry the complete transaction with a
bounded exponential backoff that is clipped by the lane deadline. Deadline
interruptions and persistent locks still fail closed, and each worker reports
`cursor_lock_retries` so contention is measurable rather than hidden.

The first post-change worker (2026-09-06 06:41 UTC) completed normally with no
provider errors or cursor-lock retries; it scanned 822 blocks and reduced the
pending cursor by 184 blocks while preserving the live guard.

## Guard-at-commit prefetch

Production then showed the remaining constraint clearly: across 30 workers,
successful work used 1,067 seconds while whole-chunk live-priority waiting used
2,032 seconds. The isolated secondary RPC was waiting together with SQLite even
though only the primary database write can collide with the live lane.

Supervised isolated backfill now uses a one-chunk, two-phase pipeline. It fetches
and parses historical logs before admission, then waits at the primary-write
boundary. SQLite apply and durable queue-cursor settlement share a child deadline
clipped to the next supervisor-published safe window. Shared-provider and
unsupervised operation retain the previous whole-chunk gate. The durable cursor
still advances only after apply succeeds; process loss before that point causes an
idempotent refetch, not skipped history.

Per-worker telemetry now separates `head_rpc`, `log_rpc`, `parse`,
`commit_window_wait`, `sqlite_apply`, `activations`, `cursor_file`, and
`cursor_commit`. This evidence will determine whether a later 125/250-block sizing
change or a durable staging spool is justified. Live cadence, admission bounds,
paper-only behavior, database commit cap, and current evidence history are unchanged.

## First production check

Completed 2026-09-05 19:34:03 UTC: 855 blocks in 109.984 seconds, with
attempt sizes 125/125/125/125/250/125/125. The next 250-block size was retained.
Successful work took 29.377 seconds; cooperative waiting took 75.672 seconds.
There was one cooperative preemption, no provider deferral and no supervisor
failure/timeout in this window. This verifies probe execution and safety, NOT a
throughput uplift or convergence: the earlier 1,170-block sample recovered more.
Ninety-six focused tests passed. Learning remains enabled with its existing cohort.

## Prefetch cost-accounting and zero-activation repair

The first guard-at-commit traces revealed two local costs that were not provider
limits. The coordinator included `commit_window_wait` in its learned chunk cost,
so a safe cooperative pause made the next chunk appear too expensive and stopped
recovery early. Historical chunks also materialised the entire pending V4
activation book before slicing it to zero, despite backfill never promoting
candidates.

Prefetch scheduling now learns from active work while reporting total elapsed time
separately. The actual parent deadline and the commit-window child deadline remain
authoritative, so excluding deliberate wait from the projection cannot extend a
run or permit a write outside an admitted window. A zero activation limit now uses
a count-only query; nonzero and unlimited live activation behavior is unchanged.

The first completed production worker after both repairs recovered 1,240 blocks in
10 durable commits. It used 41.219 seconds of active work and waited cooperatively
for 59.624 seconds, with zero preemptions and zero cursor-lock retries. During that
worker the backlog fell by 596 blocks. This is 45% more recovery than the recent
30-worker mean of 856 blocks, but longer-run convergence still depends on repeated
workers sustaining recovery above contemporaneous arrivals.

The immediately following worker recovered 1,164 blocks in 11 commits, used
37.999 seconds of active work, waited 56.986 seconds, and reduced backlog by
another 566 blocks. It had one replay-safe cooperative preemption, no lane
failure, no provider deferral, and no cursor-lock retry. Across the two workers,
2,404 blocks were recovered while the local pending total fell by 1,162 blocks.

## Safe-window sizing correction

Later workers regressed to eight consecutive 125-block attempts even though the
durable controller continued to report a proven 250-block size. The coordinator
was sizing from the window visible *before* prefetch. When live was active that
window necessarily reported no headroom, so every prepared chunk was reduced even
though its write phase would wait for a new safe window.

Isolated guard-at-commit prefetch now starts with the controller's proven size and
lets the commit gate judge the actual write window. If a large prepared chunk is
really preempted at that boundary, the same durable cursor gets one local retry at
the 125-block minimum. That fallback is scheduler/density evidence and therefore
does not shrink the provider-capability model. Shared-provider and non-prefetch
workers retain the conservative pre-fetch window cap, and every write and cursor
settlement remains bounded by the existing child deadline.

Two production workers after reload validated the behavior. The first recovered
1,084 blocks in five commits and reduced its local backlog by 566 blocks; its
attempt sequence was 250/125/250/250/250/250, demonstrating one density fallback
followed by recovery at the proven size. The second recovered 1,237 blocks in six
commits and reduced backlog by 626 blocks using 250 blocks throughout, with no
preemption. Combined recovery was 2,321 blocks and the two local backlog deltas
totalled -1,192. Both workers completed without provider deferral, cursor-lock
retry, or lane failure; live execution remained disabled.

## Resident recovery duty cycle

The next 39-worker trace showed that chunk throughput was no longer the main
limit. Backfill workers spent about 109 seconds on average between completion and
the next launch. Most long gaps contained healthy live/marks work, but the
five-minute supervisor also refused to start a fully bounded 120-second child in
its final 123 seconds. Repeating that tail every five minutes left historical
recovery active only about half the elapsed time.

The scheduled runner now supervises for 19 minutes, below its existing 20-minute
hard task limit. The isolated backfill child has a four-minute budget, allowing it
to remain resident across several live reservations and resume at every admitted
commit window. Its existing cooperative gate, child write deadlines, 10-second
completion reserve, single-writer cursor, 24-chunk ceiling, and hard-kill grace
remain unchanged. Evidence and certificate selection still outrank pressured
backfill between child exits, so persistence does not become an uninterruptible
daemon or remove their fairness slots. The five-minute Scheduled Task uses
`IgnoreNew`; triggers during the longer supervisor are intentionally coalesced.

The first resident production worker stayed active across seven successful live
passes, proving cooperative non-interference, but recovered only 1,125 blocks. It
oscillated 250 -> 125 -> 250 three times; the failed large attempts consumed
roughly 55 unreported seconds before their replay-safe rollback. Scheduler
fallback now requires three successful 125-block commits before the next
250-block probe, without changing provider capability state. Historical apply
also batches 500 events per interruptible transaction instead of 100, amortizing
repeated pool/cache setup. The SQLite progress handler, child deadline, and live
ingestion transaction size are unchanged.

Because the backfill budget and historical transaction quantum are acceptance
policy inputs, future operational cohorts are labelled
`robinhood-operational-v25`. Existing cohort rows are preserved and are not
relabelled or reset; no cohort was collecting when this boundary changed.

The first v25 production worker ran 224.204 seconds across seven live cycles,
recovered 1,532 blocks in eight durable commits, and reduced its local backlog by
558 blocks. It used one 250-block preemption followed by three successful
125-block fallback commits, then returned to 250; no provider deferral, cursor
lock retry, or lane failure occurred. After it exited, the still-running
19-minute supervisor admitted the due evidence lane, which completed, and then
launched a second resident backfill worker without waiting for another Scheduled
Task boundary. This validates both duty-cycle continuity and evidence fairness;
rolling convergence remains a multi-worker acceptance measurement.

## Zero-arrival pressure semantics (v26)

The clean v25 production window recovered faster than the chain created new
debt and reduced the measured backlog substantially. Once inferred arrivals
reached zero, however, the pressure calculation represented the undefined
`recovered / 0` ratio as `1.0`. Since the frozen target is `1.10`, this marked a
shrinking backlog as pressured and kept recovery priority unnecessarily active.

Operational policy v26 freezes pressure-model version 2. A zero-arrival,
decreasing-backlog window now publishes a null numeric ratio plus
`target_met: true` and the explicit assessment
`no_inferred_arrivals_backlog_decreasing`. Zero arrivals with a flat, non-empty
backlog remains pressured. Positive-arrival windows retain the original ratio
comparison unchanged.
