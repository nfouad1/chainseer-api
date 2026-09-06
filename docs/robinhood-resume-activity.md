# Local resume recovery and activity observability

The completed operational cohort is a historical acceptance sample, not proof
that the learner is currently making progress. No cohort, economic path, policy,
admission threshold, or paper position is reset by this change.

## Recovery

`manage_chainseer_robinhood_learning_task.ps1 configure-recovery` adds or updates
the named `ChainseerResume` event trigger on the existing learning task. It watches
the System log for Microsoft-Windows-Power-Troubleshooter event 1 and delays the
launch by 60 seconds, allowing the pre-sleep supervisor to finish cleanup.
The same task's five-minute repetition remains the fallback. `IgnoreNew` is
required, so an existing supervisor is not duplicated. Existing actions, principal,
task enabled state and power settings are retained. It does not wake the computer,
enable a paused task, or enable live execution. Installation also adds the trigger.

Windows supports event subscriptions and delayed event triggers:
[Microsoft event trigger schema](https://learn.microsoft.com/en-us/windows/win32/taskschd/taskschedulerschema-eventtriggertype-complextype).

Registration and XML preservation can be tested without suspending the laptop.
End-to-end resume timing still needs a real sleep/wake observation; configuration
alone is not proof of recovery. It also cannot collect while the laptop is asleep,
logged out of the interactive account, or disconnected. Reconnect retries retain
the existing worker deadlines and periodic launcher, not a new infinite retry loop.

## Dashboard

The activity banner reads small bounded telemetry files on each API response,
independently of expensive cached research snapshots. Reads have no DB/RPC work,
no task-control side effects, and no Timechain writes.

- Active: supervisor heartbeat within 60 seconds and completed live work within 90.
- Recovering: a new launcher/supervisor has started but recent live work is pending.
- Waiting: a completed supervisor window within the configured interval plus 60 seconds.
- Stalled/degraded: supervisor is responsive but live work is overdue or failed.
- Deferred: a recent live attempt yielded within its safety budget; not a success or a stall.
- Paused: the management script's local schedule records disabled learning.
- Stale/unknown/error: missing progress, invalid timestamps, or launcher failure.

Schedule intent is the local management file, not a per-request Windows task query;
out-of-band Task Scheduler changes can leave that intent stale, but will not create
fresh heartbeat evidence. These are recent-activity indicators, not process leases.

Evidence is separate: it shows actual quotes in the latest completed batch, its
age and stop reason. It becomes stale after two configured launch intervals plus
90 seconds. `no_eligible_work` requires an explicit drained, positive-budget worker
check with zero selected rows. Wall-clock nominal due counts do not prove a block
checkpoint was eligible at that worker's measured head.

Validation: unit tests cover pauses, sleep gaps, clock anomalies, launcher failure,
missing work, independent evidence age, malformed telemetry, startup completion
semantics and idempotent XML updates preserving unrelated settings.
