# TODO

## Robinhood Chain

- [x] Harden concurrent live scans and scheduled learning against shared RPC/API contention.
  - Add bounded retry with exponential backoff and jitter around observer RPC calls and external price-provider requests.
  - Advance discovery cursors only after the complete block window succeeds.
  - Preserve the full Python traceback in scheduler status and log files instead of only the first line.
  - Add a regression test that runs a live Robinhood token scan alongside a scheduled learning cycle and simulates throttling/timeouts.
  - Verify both lanes recover without losing candidates, duplicating promotions, or leaving a run marked `running`.

## Production scanner

- [x] Make live scan polling resilient to temporary API and Fly health interruptions.
  - Preserve the accepted job ID when an individual status poll times out or returns a transient `5xx` response.
  - Display a reconnecting state and retry with bounded exponential backoff instead of terminating a scan near completion.
  - Recover and display a report that completed while the webpage was disconnected.
  - Reserve “No result was published” for a confirmed terminal backend failure; use an accurate temporary-connection message for polling interruptions.
  - Isolate full Timechain audits and other maintenance work from request-serving CPU, memory, and event-loop capacity.
  - Add an end-to-end regression test that interrupts polling around 90%, completes the backend job, and proves the report is recovered without starting a duplicate analysis.
  - Add production telemetry for polling timeouts, Fly health-check failures, memory pressure, reconnect success, and abandoned-but-completed jobs.
