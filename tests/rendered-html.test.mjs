import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const projectRoot = new URL("../", import.meta.url);

async function render(path = "/") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  return worker.fetch(
    new Request(`http://localhost${path}`, {
      headers: { accept: "text/html" },
    }),
    {
      ASSETS: {
        fetch: async () => new Response("Not found", { status: 404 }),
      },
    },
    {
      waitUntil() {},
      passThroughOnException() {},
    },
  );
}

test("server-renders the Chainseer product page", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);

  const html = await response.text();
  assert.match(html, /<title>Chainseer/);
  assert.match(html, /Robinhood Chain/);
  assert.match(html, /Solana/);
  assert.match(html, /Timechain-sealed/);
  assert.match(html, /Powered by Cypher Tempre/);
  assert.doesNotMatch(html, /Private beta/i);
  assert.match(html, /not financial advice/i);
  assert.doesNotMatch(html, /Your site is taking shape|react-loading-skeleton/);
  assert.doesNotMatch(html, /codex-preview/);
  assert.doesNotMatch(html, /url\([A-Za-z]:[\\/]/);
});

test("ships restrictive browser security headers", async () => {
  const response = await render();
  assert.equal(response.headers.get("x-frame-options"), "DENY");
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.match(
    response.headers.get("content-security-policy") ?? "",
    /frame-ancestors 'none'/,
  );
  assert.equal(
    response.headers.get("referrer-policy"),
    "strict-origin-when-cross-origin",
  );
});

test("publishes privacy and terms pages", async () => {
  const [privacyResponse, termsResponse] = await Promise.all([
    render("/privacy"),
    render("/terms"),
  ]);
  assert.equal(privacyResponse.status, 200);
  assert.equal(termsResponse.status, 200);
  assert.match(await privacyResponse.text(), /Timechain records/);
  assert.match(await termsResponse.text(), /No safety guarantee/);
});

test("publishes robots and sitemap metadata", async () => {
  const [robotsResponse, sitemapResponse] = await Promise.all([
    render("/robots.txt"),
    render("/sitemap.xml"),
  ]);
  assert.equal(robotsResponse.status, 200);
  assert.equal(sitemapResponse.status, 200);
  assert.match(await robotsResponse.text(), /Disallow: \/api\//);
  assert.match(await sitemapResponse.text(), /usechainseer\.com\/privacy/);
});

test("keeps secrets server-side and removes the starter preview", async () => {
  const [page, apiRoute, packageJson] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/api/analyses/route.ts", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  assert.match(page, /\/api\/analyses/);
  assert.match(page, /SAMPLE DATA · NOT LIVE/);
  assert.match(page, /nothing was sealed/);
  assert.match(page, /setShowExample\(false\)/);
  assert.doesNotMatch(page, /0x407470F8D77d12417A6cfaC5940c2f8B5F4E8a27/);
  assert.doesNotMatch(page, /ring_000751/);
  assert.match(apiRoute, /CHAINSEER_API_TOKEN/);
  assert.match(apiRoute, /validSolanaMint/);
  assert.match(apiRoute, /JSON\.stringify\(\{ address, network \}\)/);
  assert.doesNotMatch(page, /CHAINSEER_API_TOKEN|NEXT_PUBLIC_CHAINSEER/);
  assert.match(packageJson, /"name": "chainseer-web"/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);

  await assert.rejects(
    access(new URL("../app/_sites-preview/SkeletonPreview.tsx", import.meta.url)),
  );
  await assert.rejects(access(new URL("public/_sites-preview", projectRoot)));
});
