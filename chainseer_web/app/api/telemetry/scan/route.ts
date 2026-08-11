import { NextRequest, NextResponse } from "next/server";

const JOB_RE = /^[a-f0-9]{32}$/;
const EVENTS = new Set([
  "poll_interruption",
  "reconnect_success",
  "recovered_completed",
  "recovery_deadline_exceeded",
]);

export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => null);
  const event = typeof body?.event === "string" ? body.event : "";
  const jobId = typeof body?.job_id === "string" ? body.job_id : "";
  if (!EVENTS.has(event) || !JOB_RE.test(jobId)) {
    return NextResponse.json({ ok: false }, { status: 422 });
  }
  console.info(JSON.stringify({
    event: `scan_${event}`,
    job_id: jobId,
    reconnect_attempts:
      typeof body?.reconnect_attempts === "number"
        ? Math.max(0, Math.floor(body.reconnect_attempts))
        : 0,
    observed_at: new Date().toISOString(),
  }));
  return NextResponse.json(
    { ok: true },
    { headers: { "Cache-Control": "no-store" } },
  );
}
