import { NextRequest, NextResponse } from "next/server";

const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
const SOLANA_ADDRESS_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
const BASE58_ALPHABET =
  "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

function validSolanaMint(value: string) {
  if (!SOLANA_ADDRESS_RE.test(value)) return false;
  let number = 0n;
  for (const character of value) {
    const index = BASE58_ALPHABET.indexOf(character);
    if (index < 0) return false;
    number = number * 58n + BigInt(index);
  }
  let byteLength = 0;
  for (let remaining = number; remaining > 0n; remaining >>= 8n) {
    byteLength += 1;
  }
  const leadingZeroes = value.length - value.replace(/^1+/, "").length;
  return leadingZeroes + byteLength === 32;
}

function configuration() {
  return {
    baseUrl: process.env.CHAINSEER_API_URL?.replace(/\/+$/, ""),
    token: process.env.CHAINSEER_API_TOKEN,
  };
}

async function clientIdentity(request: NextRequest, token: string) {
  const existing = request.cookies.get("chainseer_monitor_device")?.value;
  const source =
    existing && /^[a-f0-9-]{36}$/.test(existing)
      ? existing
      : crypto.randomUUID();
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
  const identity = Array.from(new Uint8Array(signature))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return { identity, issuedDevice: existing ? null : source };
}

function validate(network: string, address: string) {
  if (network !== "robinhood" && network !== "solana") return false;
  return network === "solana"
    ? validSolanaMint(address)
    : ADDRESS_RE.test(address);
}

async function proxy(
  request: NextRequest,
  path: string,
  init?: RequestInit,
) {
  const { baseUrl, token } = configuration();
  if (!baseUrl || !token) {
    return NextResponse.json(
      {
        error: {
          code: "monitor_unavailable",
          message: "Critical monitoring is not connected on this deployment.",
        },
      },
      { status: 503, headers: { "Retry-After": "60" } },
    );
  }
  const requestId = crypto.randomUUID();
  const { identity, issuedDevice } = await clientIdentity(request, token);
  try {
    const response = await fetch(`${baseUrl}${path}`, {
      ...init,
      cache: "no-store",
      signal: AbortSignal.timeout(12_000),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        "X-Request-ID": requestId,
        "X-Chainseer-Client": identity,
        ...(init?.headers || {}),
      },
    });
    const body = await response.json().catch(() => ({
      detail: "The monitoring service returned an unreadable response.",
    }));
    const result = NextResponse.json(body, {
      status: response.status,
      headers: {
        "Cache-Control": "no-store",
        "X-Request-ID": response.headers.get("x-request-id") || requestId,
      },
    });
    if (issuedDevice) {
      result.cookies.set("chainseer_monitor_device", issuedDevice, {
        httpOnly: true,
        secure: true,
        sameSite: "strict",
        path: "/",
        maxAge: 365 * 24 * 60 * 60,
      });
    }
    return result;
  } catch {
    return NextResponse.json(
      {
        error: {
          code: "monitor_service_unreachable",
          message: "Critical monitoring is temporarily unreachable.",
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
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: { code: "invalid_json", message: "Request body must be JSON." } },
      { status: 400 },
    );
  }
  const payload = body && typeof body === "object" ? body : {};
  const network =
    "network" in payload && payload.network === "solana"
      ? "solana"
      : "robinhood";
  const address =
    "address" in payload && typeof payload.address === "string"
      ? payload.address.trim()
      : "";
  if (!validate(network, address)) {
    return NextResponse.json(
      { error: { code: "invalid_address", message: "Invalid token address." } },
      { status: 422 },
    );
  }
  return proxy(request, "/v1/watch", {
    method: "POST",
    body: JSON.stringify({ network, address }),
  });
}

export async function GET(request: NextRequest) {
  const network = request.nextUrl.searchParams.get("network") || "";
  const address = request.nextUrl.searchParams.get("address")?.trim() || "";
  const after = request.nextUrl.searchParams.get("after") || "";
  if (!validate(network, address)) {
    return NextResponse.json(
      { error: { code: "invalid_address", message: "Invalid token address." } },
      { status: 422 },
    );
  }
  const query = new URLSearchParams({ network, address, limit: "50" });
  if (after) query.set("after", after);
  return proxy(request, `/v1/watch/alerts?${query.toString()}`);
}

export async function DELETE(request: NextRequest) {
  const network = request.nextUrl.searchParams.get("network") || "";
  const address = request.nextUrl.searchParams.get("address")?.trim() || "";
  if (!validate(network, address)) {
    return NextResponse.json(
      { error: { code: "invalid_address", message: "Invalid token address." } },
      { status: 422 },
    );
  }
  return proxy(
    request,
    `/v1/watch/${encodeURIComponent(address)}?network=${network}`,
    { method: "DELETE" },
  );
}
