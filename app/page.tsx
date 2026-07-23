"use client";

import { FormEvent, useState } from "react";

const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;

type PublicReport = {
  schema_version: string;
  token: {
    address: string;
    name?: string;
    symbol?: string;
    chain: string;
    chain_id?: number;
    explorer_url?: string;
  };
  decision: {
    action?: string;
    risk_level?: string;
    model_risk_level?: string;
    score?: number;
    confidence?: string;
    confidence_detail?: string;
    recommendation?: string;
    hard_stops: Array<{
      code?: string;
      severity?: string;
      reason?: string;
      action?: string;
    }>;
  };
  factors: Record<string, number>;
  flags: {
    red: string[];
    yellow: string[];
    green: string[];
    unknown: Record<string, string>;
  };
  market: {
    price_usd?: number | string | null;
    market_cap_usd?: number | string | null;
    liquidity_usd?: number | string | null;
    volume_24h_usd?: number | string | null;
    age?: string | null;
  };
  evidence: {
    fact_count: number;
    block_pin?: number;
    ledger_hash?: string;
    facts: Array<{
      id?: string;
      source?: string;
      query_hash?: string;
      response_hash?: string;
      block?: number;
      timestamp?: string;
      cache_hit?: boolean;
    }>;
  };
  timechain: {
    ring?: number;
    ring_hash?: string;
    decision?: string;
    scores: Record<string, number>;
  };
  analyzed_at?: string;
  disclaimer: string;
};

type ScanState = "idle" | "submitting" | "analyzing" | "succeeded" | "failed";

const factorNames: Record<string, string> = {
  security: "Contract security",
  honeypot_safety: "Honeypot safety",
  liquidity: "Liquidity health",
  lp_lock: "LP lock",
  holder_distribution: "Holder distribution",
  volume: "Volume quality",
  maturity: "Token maturity",
  creator_risk: "Creator risk",
  wash_trading: "Wash trading",
  deployer: "Deployer history",
  sentiment: "Market sentiment",
  trend: "Trend strength",
};

function errorMessage(body: unknown, fallback: string) {
  if (!body || typeof body !== "object") return fallback;
  if (
    "error" in body &&
    body.error &&
    typeof body.error === "object" &&
    "message" in body.error &&
    typeof body.error.message === "string"
  ) {
    return body.error.message;
  }
  if ("detail" in body && typeof body.detail === "string") return body.detail;
  return fallback;
}

function delay(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function formatMoney(value: number | string | null | undefined) {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return "Unknown";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: amount >= 1000 ? "compact" : "standard",
    maximumFractionDigits: amount < 1 ? 6 : 2,
  }).format(amount);
}

function LiveReport({ report }: { report: PublicReport }) {
  const factorEntries = Object.entries(report.factors)
    .filter(([key, value]) => key !== "legitimacy" && Number.isFinite(value))
    .map(([key, value]) => ({
      key,
      label: factorNames[key] || key.replaceAll("_", " "),
      score: Math.max(0, Math.min(100, Number(value))),
    }));
  const hardStops = report.decision.hard_stops || [];
  const risk = (report.decision.risk_level || "Unknown").toLowerCase();

  return (
    <section className="live-report" id="live-report" aria-live="polite">
      <div className="live-report-head">
        <div>
          <div className="eyebrow">
            <span className="status-dot" />
            Live sealed analysis
          </div>
          <h2>
            {report.token.name || "Unknown token"}
            {report.token.symbol ? ` (${report.token.symbol})` : ""}
          </h2>
          <p className="live-address">{report.token.address}</p>
        </div>
        <div className={`live-risk risk-${risk}`}>
          <span>{report.decision.action || "REVIEW"}</span>
          <strong>{report.decision.score ?? "—"}<small>/100</small></strong>
          <em>{report.decision.risk_level || "Unknown"} risk</em>
        </div>
      </div>

      <div className="live-summary-grid">
        <article>
          <span>Confidence</span>
          <strong>{report.decision.confidence || "Limited"}</strong>
          <p>{report.decision.confidence_detail || "Data completeness varies by source."}</p>
        </article>
        <article>
          <span>Hard-stop gate</span>
          <strong>{hardStops.length ? `${hardStops.length} triggered` : "Clear"}</strong>
          <p>
            {hardStops.length
              ? hardStops.map((stop) => stop.code).filter(Boolean).join(" · ")
              : "No automatic rejection condition was detected."}
          </p>
        </article>
        <article>
          <span>Market snapshot</span>
          <strong>{formatMoney(report.market.liquidity_usd)} liquidity</strong>
          <p>
            {formatMoney(report.market.volume_24h_usd)} volume ·{" "}
            {report.market.age || "Age unknown"}
          </p>
        </article>
        <article>
          <span>Evidence</span>
          <strong>{report.evidence.fact_count} facts</strong>
          <p>Block {report.evidence.block_pin?.toLocaleString() || "unknown"}</p>
        </article>
      </div>

      <div className="live-recommendation">
        <span>Investor interpretation</span>
        <p>{report.decision.recommendation || "Review the evidence before making a decision."}</p>
      </div>

      {hardStops.length > 0 && (
        <div className="live-hard-stops">
          {hardStops.map((stop, index) => (
            <article key={`${stop.code || "stop"}-${index}`}>
              <span>{stop.severity || "Material"} hard stop</span>
              <strong>{stop.code?.replaceAll("_", " ") || "Risk condition"}</strong>
              <p>{stop.reason}</p>
            </article>
          ))}
        </div>
      )}

      <div className="live-detail-grid">
        <div className="live-factors">
          <div className="panel-head">
            <div><span className="panel-index">01</span><h3>Risk dimensions</h3></div>
            <span>Higher is safer</span>
          </div>
          <div className="factor-grid">
            {factorEntries.map((factor) => (
              <div className="factor" key={factor.key}>
                <div className="factor-label">
                  <span>{factor.label}</span><strong>{factor.score}</strong>
                </div>
                <div className="bar">
                  <span
                    className={factor.score >= 70 ? "good" : "warn"}
                    style={{ width: `${factor.score}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="live-evidence">
          <div className="panel-head">
            <div><span className="panel-index">02</span><h3>Verification</h3></div>
            <div className="verified-seal"><span>◆</span> {report.timechain.decision || "SEALED"}</div>
          </div>
          <dl>
            <div><dt>Timechain Ring</dt><dd>{report.timechain.ring ?? "—"}</dd></div>
            <div><dt>Ring hash</dt><dd>{report.timechain.ring_hash?.slice(0, 20) || "—"}…</dd></div>
            <div><dt>Ledger hash</dt><dd>{report.evidence.ledger_hash?.slice(0, 20) || "—"}…</dd></div>
            <div><dt>Block pin</dt><dd>{report.evidence.block_pin?.toLocaleString() || "—"}</dd></div>
          </dl>
          {report.token.explorer_url && (
            <a href={report.token.explorer_url} target="_blank" rel="noreferrer">
              Open contract explorer ↗
            </a>
          )}
        </div>
      </div>

      <div className="live-flags">
        <article>
          <span>Material concerns</span>
          {(report.flags.red.length || report.flags.yellow.length) ? (
            <ul>
              {[...report.flags.red, ...report.flags.yellow].slice(0, 8).map((flag) => (
                <li key={flag}>{flag}</li>
              ))}
            </ul>
          ) : <p>No material concern was returned.</p>}
        </article>
        <article>
          <span>Unknowns and limits</span>
          {Object.keys(report.flags.unknown).length ? (
            <ul>
              {Object.entries(report.flags.unknown).slice(0, 8).map(([key, value]) => (
                <li key={key}><strong>{factorNames[key] || key}:</strong> {value}</li>
              ))}
            </ul>
          ) : <p>No unresolved component was returned.</p>}
        </article>
      </div>

      <p className="live-disclaimer">{report.disclaimer}</p>
    </section>
  );
}

const factors = [
  { label: "Contract security", score: 94, tone: "good" },
  { label: "Honeypot safety", score: 100, tone: "good" },
  { label: "Liquidity health", score: 76, tone: "good" },
  { label: "LP lock", score: 82, tone: "good" },
  { label: "Holder distribution", score: 61, tone: "warn" },
  { label: "Volume quality", score: 72, tone: "good" },
  { label: "Token maturity", score: 45, tone: "warn" },
  { label: "Creator risk", score: 68, tone: "warn" },
  { label: "Wash trading", score: 87, tone: "good" },
  { label: "Deployer history", score: 74, tone: "good" },
  { label: "Market sentiment", score: 70, tone: "good" },
  { label: "Trend strength", score: 66, tone: "warn" },
];

const evidence = [
  {
    id: "DEMO-001",
    label: "Source code verified",
    source: "Blockscout",
    block: "Demo block",
    status: "confirmed",
  },
  {
    id: "DEMO-002",
    label: "No sell restriction detected",
    source: "GoPlus + RPC",
    block: "Demo block",
    status: "confirmed",
  },
  {
    id: "DEMO-003",
    label: "Top 10 holders control 31.8%",
    source: "RPC",
    block: "Demo block",
    status: "watch",
  },
];

export default function Home() {
  const [address, setAddress] = useState("");
  const [notice, setNotice] = useState("");
  const [scanState, setScanState] = useState<ScanState>("idle");
  const [liveReport, setLiveReport] = useState<PublicReport | null>(null);
  const [showExample, setShowExample] = useState(false);

  async function submitScan(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedAddress = address.trim();

    if (!ADDRESS_RE.test(normalizedAddress)) {
      setNotice("Enter a valid 42-character EVM contract address.");
      setScanState("failed");
      return;
    }

    setScanState("submitting");
    setLiveReport(null);
    setShowExample(false);
    setNotice("Submitting the contract to the serialized analysis queue…");

    try {
      const submission = await fetch("/api/analyses", {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address: normalizedAddress }),
      });
      const submissionBody = await submission.json().catch(() => null);
      if (!submission.ok) {
        throw new Error(
          errorMessage(submissionBody, "The analysis could not be submitted."),
        );
      }
      const jobId =
        submissionBody && typeof submissionBody.job_id === "string"
          ? submissionBody.job_id
          : "";
      if (!/^[a-f0-9]{32}$/.test(jobId)) {
        throw new Error("The analysis service returned an invalid job identifier.");
      }

      setScanState("analyzing");
      setNotice(
        submissionBody.cached
          ? "Loading the recent sealed result…"
          : "Analyzing contract, liquidity, holders, deployer history, and provenance…",
      );

      for (let attempt = 0; attempt < 150; attempt += 1) {
        if (attempt > 0) await delay(2_000);
        const response = await fetch(
          `/api/analyses?job=${encodeURIComponent(jobId)}`,
          { cache: "no-store" },
        );
        const job = await response.json().catch(() => null);
        if (!response.ok) {
          throw new Error(
            errorMessage(job, "The analysis status could not be retrieved."),
          );
        }
        if (job.status === "succeeded" && job.result) {
          setLiveReport(job.result as PublicReport);
          setScanState("succeeded");
          setNotice(
            `Analysis sealed to Timechain Ring ${job.result.timechain?.ring ?? "—"}.`,
          );
          window.setTimeout(() => {
            document.getElementById("live-report")?.scrollIntoView({
              behavior: "smooth",
              block: "start",
            });
          }, 50);
          return;
        }
        if (job.status === "failed") {
          throw new Error(
            errorMessage(job, "The analysis failed without publishing a result."),
          );
        }
      }
      throw new Error("The analysis is taking longer than expected. Try again shortly.");
    } catch (error) {
      setScanState("failed");
      setNotice(
        error instanceof Error
          ? error.message
          : "The analysis could not be completed.",
      );
    }
  }

  function loadExample() {
    setShowExample(true);
    setNotice("Demo report opened. Its token, evidence, score, and Timechain proof are fictional.");
    setScanState("idle");
    setLiveReport(null);
    document.getElementById("report")?.scrollIntoView({ behavior: "smooth" });
  }

  return (
    <main>
      <header className="site-header">
        <a className="brand" href="#top" aria-label="Chainseer home">
          <span className="brand-mark" aria-hidden="true">C</span>
          <span>CHAINSEER</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#report">Demo report</a>
          <a href="#method">Method</a>
          <a href="#timechain">Timechain</a>
          <a href="#evidence" onClick={() => setShowExample(true)}>Evidence</a>
        </nav>
        <div className="header-actions">
          <a
            className="timechain-badge"
            href="#timechain"
            aria-label="Learn how Chainseer analysis is sealed with Cypher Tempre Timechain"
          >
            <span className="badge-glyph" aria-hidden="true">◆</span>
            <span>
              <strong>Timechain-sealed</strong>
              <small>PoQ-verified analysis</small>
            </span>
          </a>
          <a className="header-cta" href="#scanner">Scan contract</a>
        </div>
      </header>

      <section className="hero" id="top">
        <div className="hero-glow" aria-hidden="true" />
        <div className="eyebrow">
          <span className="status-dot" />
          On-chain intelligence · Powered by Cypher Tempre Timechain
        </div>
        <h1>
          Know the contract
          <br />
          <span>before it knows your wallet.</span>
        </h1>
        <p className="hero-copy">
          Chainseer turns fragmented on-chain signals into one evidence-backed
          risk decision—before you buy, bridge, or connect.
        </p>

        <form className="scanner" id="scanner" onSubmit={submitScan}>
          <div className="network-select" aria-label="Selected network">
            <span className="network-glyph">◆</span>
            <span>
              <strong>Robinhood Chain</strong>
              <small>Base · coming soon</small>
            </span>
          </div>
          <label className="sr-only" htmlFor="contract-address">Contract address</label>
          <input
            id="contract-address"
            value={address}
            onChange={(event) => {
              setAddress(event.target.value);
              setNotice("");
            }}
            placeholder="Paste a contract address 0x..."
            autoComplete="off"
            spellCheck={false}
          />
          <button type="submit" disabled={scanState === "submitting" || scanState === "analyzing"}>
            {scanState === "submitting" || scanState === "analyzing"
              ? "Analysis running…"
              : "Run risk scan"}
          </button>
        </form>
        <div className="scanner-meta">
          <span>No wallet connection</span>
          <span>No signature</span>
          <span>Evidence pinned to block</span>
          <button type="button" onClick={loadExample}>Open demo report →</button>
        </div>
        {notice && <p className="scan-notice" role="status">{notice}</p>}
      </section>

      <section className="signal-strip" aria-label="Chainseer capabilities">
        <div><strong>12</strong><span>risk dimensions</span></div>
        <div><strong>6</strong><span>hard-stop gates</span></div>
        <div><strong>Block-pinned</strong><span>market evidence</span></div>
        <div><strong>Timechain</strong><span>tamper-evident memory</span></div>
      </section>

      {liveReport && <LiveReport report={liveReport} />}

      <section className="report-shell" id="report">
        <div className="section-heading">
          <div>
            <div className="eyebrow muted">Optional product walkthrough</div>
            <h2>See the decision, then inspect the proof.</h2>
          </div>
          <p>
            Open the fictional demo to understand the report structure. A real
            scan replaces it with fresh, sealed analysis.
          </p>
        </div>

        <details
          className="demo-report"
          open={showExample}
          onToggle={(event) => setShowExample(event.currentTarget.open)}
        >
          <summary>
            <span>
              <strong>Fictional demo report</strong>
              <small>Sentinel Demo (DEMO) · no live token or on-chain lookup</small>
            </span>
            <span className="demo-tag">SAMPLE DATA · NOT LIVE</span>
          </summary>
          <div className="demo-content">
        <div className="decision-grid">
          <article className="decision-card">
            <div className="card-kicker">
              Effective action
              <span className="demo-tag">DEMO</span>
            </div>
            <div className="decision-word">WATCH</div>
            <p>
              No critical exploit condition was detected, but concentration
              and token maturity require monitoring before entry.
            </p>
            <div className="decision-footer">
              <div><span>Risk score</span><strong>78<span>/100</span></strong></div>
              <div><span>Confidence</span><strong className="confidence">MEDIUM</strong></div>
            </div>
          </article>

          <article className="stops-card">
            <div className="card-kicker">Hard-stop gate</div>
            <div className="clear-line">
              <span className="clear-icon">✓</span>
              <div>
                <strong>No hard stop triggered</strong>
                <p>Six irreversible risk conditions checked.</p>
              </div>
            </div>
            <ul className="stop-list">
              <li><span>Honeypot behavior</span><b>Clear</b></li>
              <li><span>Sell restrictions</span><b>Clear</b></li>
              <li><span>Known scam flags</span><b>Clear</b></li>
              <li><span>Unlocked liquidity</span><b>Clear</b></li>
              <li><span>Extreme concentration</span><b>Clear</b></li>
              <li><span>Unverified proxy</span><b>Clear</b></li>
            </ul>
          </article>
        </div>

        <div className="factor-panel">
          <div className="panel-head">
            <div><span className="panel-index">01</span><h3>Risk dimensions</h3></div>
            <span>Higher is safer</span>
          </div>
          <div className="factor-grid">
            {factors.map((factor) => (
              <div className="factor" key={factor.label}>
                <div className="factor-label"><span>{factor.label}</span><strong>{factor.score}</strong></div>
                <div className="bar">
                  <span className={factor.tone} style={{ width: `${factor.score}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="evidence-panel" id="evidence">
          <div className="panel-head">
            <div><span className="panel-index">02</span><h3>Evidence ledger</h3></div>
            <div className="verified-seal"><span>◆</span> Demo format only</div>
          </div>
          <div className="evidence-table" role="table" aria-label="Fictional demonstration evidence ledger">
            <div className="evidence-row evidence-header" role="row">
              <span>Finding</span><span>Source</span><span>Block</span><span>Status</span>
            </div>
            {evidence.map((item) => (
              <div className="evidence-row" role="row" key={item.id}>
                <span><small>{item.id}</small>{item.label}</span>
                <span>{item.source}</span>
                <span className="mono">{item.block}</span>
                <span className={`evidence-status ${item.status}`}>{item.status}</span>
              </div>
            ))}
          </div>
          <div className="chain-seal">
            <div>
              <span className="seal-mark">C</span>
              <div><strong>Illustrative Timechain proof</strong><span>Format preview only · nothing was sealed</span></div>
            </div>
            <code>demo_ring_0001 · fictional_hash</code>
          </div>
        </div>
          </div>
        </details>
      </section>

      <section className="method" id="method">
        <div className="method-intro">
          <div className="eyebrow muted">The Chainseer method</div>
          <h2>One score is not enough.</h2>
          <p>
            A token can look strong on the surface while one hidden condition
            makes the entire trade invalid. Chainseer separates the verdict,
            confidence, hard stops, and evidence so you can see exactly what
            changed the decision.
          </p>
        </div>
        <div className="steps">
          <article><span>01</span><h3>Observe</h3><p>Collect contract, liquidity, holder, deployer, and market signals.</p></article>
          <article><span>02</span><h3>Challenge</h3><p>Run hard-stop gates and test conflicting evidence before scoring.</p></article>
          <article><span>03</span><h3>Explain</h3><p>Translate the result into an investor-friendly action and watchlist.</p></article>
          <article><span>04</span><h3>Remember</h3><p>Seal evidence and compare later outcomes without rewriting history.</p></article>
        </div>
      </section>

      <section className="timechain-section" id="timechain">
        <div className="timechain-orbit" aria-hidden="true">
          <span className="orbit-ring orbit-one" />
          <span className="orbit-ring orbit-two" />
          <span className="orbit-ring orbit-three" />
          <span className="orbit-core">C</span>
        </div>
        <div className="timechain-content">
          <div className="eyebrow">Powered by Cypher Tempre Timechain</div>
          <h2>Analysis that cannot quietly rewrite its past.</h2>
          <p className="timechain-lead">
            Every completed Chainseer analysis and outcome reflection is sealed
            as a cryptographically linked Timechain Ring. Each new Ring commits
            to the one before it, creating a verifiable history of what the
            agent observed, concluded, and later learned.
          </p>
          <div className="timechain-benefits">
            <article>
              <span>01 / Integrity</span>
              <h3>Tamper-evident history</h3>
              <p>
                Editing an earlier Ring breaks the hash chain. Reports remain
                auditable instead of becoming a silently changing narrative.
              </p>
            </article>
            <article>
              <span>02 / Conscience</span>
              <h3>Proof-of-Qualia gate</h3>
              <p>
                Before sealing, PoQ challenges coherence, relevance,
                consistency, depth, novelty, and alignment. Unsupported
                certainty is surfaced rather than hidden.
              </p>
            </article>
            <article>
              <span>03 / Memory</span>
              <h3>Learning with provenance</h3>
              <p>
                Later outcomes can be linked back to the original decision, so
                Chainseer learns from results without altering the forecast it
                actually made.
              </p>
            </article>
            <article>
              <span>04 / Evolution</span>
              <h3>New faculties, sealed</h3>
              <p>
                When the agent encounters a genuine reasoning gap, Cambium can
                grow a new sense or modality—and record where that capability
                came from.
              </p>
            </article>
          </div>
          <div className="timechain-trace">
            <span>RING N</span><i />
            <span>RING N+1</span><i />
            <span className="trace-active">RING N+2 · VERIFIED</span>
          </div>
        </div>
      </section>

      <section className="closing">
        <div>
          <span className="eyebrow">Built for decisions, not hype</span>
          <h2>Read the chain. Verify the claim.</h2>
        </div>
        <a href="#scanner">Scan a contract</a>
      </section>

      <footer>
        <a className="brand footer-brand" href="#top">
          <span className="brand-mark">C</span><span>CHAINSEER</span>
        </a>
        <p>
          Chainseer provides informational risk analysis, not financial advice.
          Digital assets can lose all value.
        </p>
        <div className="footer-links">
          <a href="/privacy">Privacy</a>
          <a href="/terms">Terms</a>
          <span>© 2026 Chainseer</span>
        </div>
      </footer>
    </main>
  );
}
