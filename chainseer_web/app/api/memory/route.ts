import { NextResponse } from "next/server";

function configuration() {
  return {
    baseUrl: process.env.CHAINSEER_API_URL?.replace(/\/+$/, ""),
    token: process.env.CHAINSEER_API_TOKEN,
  };
}

export async function GET() {
  const { baseUrl, token } = configuration();
  if (!baseUrl || !token) {
    return NextResponse.json(
      {
        error: {
          code: "memory_status_unavailable",
          message: "Memory Core status is not connected on this deployment.",
        },
      },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }
  const requestId = crypto.randomUUID();
  try {
    const response = await fetch(`${baseUrl}/v1/memory/status`, {
      cache: "no-store",
      signal: AbortSignal.timeout(15_000),
      headers: {
        Authorization: `Bearer ${token}`,
        "X-Request-ID": requestId,
      },
    });
    const body = await response.json().catch(() => ({
      detail: "The Memory Core returned an unreadable response.",
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
      {
        error: {
          code: "memory_service_unreachable",
          message: "Memory Core integrity status is temporarily unreachable.",
        },
      },
      {
        status: 503,
        headers: { "Cache-Control": "no-store", "X-Request-ID": requestId },
      },
    );
  }
}
