import { NextResponse } from "next/server";

function configuration() {
  return {
    baseUrl: process.env.CHAINSEER_API_URL?.replace(/\/+$/, ""),
    token: process.env.CHAINSEER_API_TOKEN,
  };
}

/**
 * Public-site read model for the paper ledger.  It is deliberately GET-only:
 * the public web application never receives an operator capability, private
 * key, or a route that could submit an order.  Manual paper exits remain in
 * the local operator dashboard, where the append is explicitly confirmed.
 */
export async function GET() {
  const { baseUrl, token } = configuration();
  if (!baseUrl || !token) {
    return NextResponse.json(
      { error: { code: "paper_status_unavailable", message: "Paper telemetry is not connected on this deployment." } },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
  const requestId = crypto.randomUUID();
  try {
    const response = await fetch(`${baseUrl}/v1/paper/status`, {
      cache: "no-store",
      signal: AbortSignal.timeout(10_000),
      headers: { Authorization: `Bearer ${token}`, "X-Request-ID": requestId },
    });
    const body = await response.json().catch(() => ({
      detail: "The paper telemetry service returned an unreadable response.",
    }));
    return NextResponse.json(body, {
      status: response.status,
      headers: {
        "Cache-Control": "no-store",
        "X-Request-ID": response.headers.get("x-request-id") || requestId,
      },
    });
  } catch {
    return NextResponse.json(
      { error: { code: "paper_service_unreachable", message: "Paper telemetry is temporarily unreachable." } },
      { status: 503, headers: { "Cache-Control": "no-store", "X-Request-ID": requestId } },
    );
  }
}
