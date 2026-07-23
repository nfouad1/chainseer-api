import { NextRequest, NextResponse } from "next/server";

const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
const JOB_RE = /^[a-f0-9]{32}$/;
const MAX_REQUEST_BYTES = 2_048;

function configuration() {
  const baseUrl = process.env.CHAINSEER_API_URL?.replace(/\/+$/, "");
  const token = process.env.CHAINSEER_API_TOKEN;
  return { baseUrl, token };
}

function unavailable() {
  return NextResponse.json(
    {
      error: {
        code: "scanner_unavailable",
        message:
          "Live analysis is not connected on this deployment yet. No address was stored.",
      },
    },
    { status: 503, headers: { "Retry-After": "60" } },
  );
}

async function clientIdentity(request: NextRequest, token: string) {
  const source =
    request.headers.get("cf-connecting-ip") ||
    request.headers.get("x-real-ip") ||
    "unknown";
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(token),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(source),
  );
  return Array.from(new Uint8Array(signature))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function proxy(
  path: string,
  init?: RequestInit,
  identity?: string,
) {
  const { baseUrl, token } = configuration();
  if (!baseUrl || !token) return unavailable();

  const requestId = crypto.randomUUID();
  try {
    const response = await fetch(`${baseUrl}${path}`, {
      ...init,
      cache: "no-store",
      signal: AbortSignal.timeout(12_000),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        "X-Request-ID": requestId,
        ...(identity ? { "X-Chainseer-Client": identity } : {}),
        ...(init?.headers || {}),
      },
    });
    const body = await response.json().catch(() => ({
      detail: "The analysis service returned an unreadable response.",
    }));
    const retryAfter = response.headers.get("retry-after");
    return NextResponse.json(body, {
      status: response.status,
      headers: {
        "Cache-Control": "no-store",
        "X-Request-ID": response.headers.get("x-request-id") || requestId,
        ...(retryAfter ? { "Retry-After": retryAfter } : {}),
      },
    });
  } catch {
    return NextResponse.json(
      {
        error: {
          code: "analysis_service_unreachable",
          message:
            "The analysis service is temporarily unreachable. No result was published.",
        },
      },
      {
        status: 503,
        headers: { "Retry-After": "30", "X-Request-ID": requestId },
      },
    );
  }
}

export async function POST(request: NextRequest) {
  const contentLength = Number(request.headers.get("content-length") || "0");
  if (
    !Number.isFinite(contentLength) ||
    contentLength < 0 ||
    contentLength > MAX_REQUEST_BYTES
  ) {
    return NextResponse.json(
      {
        error: {
          code: "request_too_large",
          message: "Request body is too large.",
        },
      },
      { status: 413, headers: { "Cache-Control": "no-store" } },
    );
  }

  let payload: unknown;
  try {
    payload = await request.json();
  } catch {
    return NextResponse.json(
      { error: { code: "invalid_json", message: "Request body must be JSON." } },
      { status: 400 },
    );
  }

  const address =
    typeof payload === "object" &&
    payload !== null &&
    "address" in payload &&
    typeof payload.address === "string"
      ? payload.address.trim()
      : "";

  if (!ADDRESS_RE.test(address)) {
    return NextResponse.json(
      {
        error: {
          code: "invalid_address",
          message: "Enter a valid 42-character EVM contract address.",
        },
      },
      { status: 422 },
    );
  }

  const { token } = configuration();
  const identity = token ? await clientIdentity(request, token) : undefined;
  return proxy("/v1/analyses", {
    method: "POST",
    body: JSON.stringify({ address }),
  }, identity);
}

export async function GET(request: NextRequest) {
  const job = request.nextUrl.searchParams.get("job") || "";
  if (!JOB_RE.test(job)) {
    return NextResponse.json(
      {
        error: {
          code: "invalid_job",
          message: "The analysis job identifier is invalid.",
        },
      },
      { status: 404 },
    );
  }
  return proxy(`/v1/analyses/${job}`);
}
