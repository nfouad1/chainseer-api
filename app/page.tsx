"use client";

import { FormEvent, useState } from "react";

const SAMPLE_ADDRESS = "0x407470F8D77d12417A6cfaC5940c2f8B5F4E8a27";

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
    id: "FACT-1042",
    label: "Source code verified",
    source: "Blockscout",
    block: "35,184,921",
    status: "confirmed",
  },
  {
    id: "FACT-1048",
    label: "No sell restriction detected",
    source: "GoPlus + RPC",
    block: "35,184,921",
    status: "confirmed",
  },
  {
    id: "FACT-1054",
    label: "Top 10 holders control 31.8%",
    source: "RPC",
    block: "35,184,921",
    status: "watch",
  },
];

export default function Home() {
  const [address, setAddress] = useState("");
  const [notice, setNotice] = useState("");

  function submitScan(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const isAddress = /^0x[a-fA-F0-9]{40}$/.test(address.trim());

    if (!isAddress) {
      setNotice("Enter a valid 42-character EVM contract address.");
      return;
    }

    setNotice(
      "The public analysis endpoint is not connected yet. Your address was not stored. Use the example report below to explore the interface.",
    );
  }

  function loadExample() {
    setAddress(SAMPLE_ADDRESS);
    setNotice("Example report loaded. All values below are demonstration data.");
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
          <a href="#report">Example report</a>
          <a href="#method">Method</a>
          <a href="#timechain">Timechain</a>
          <a href="#evidence">Evidence</a>
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
          On-chain intelligence · Private beta
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
          <button type="submit">Run risk scan</button>
        </form>
        <div className="scanner-meta">
          <span>No wallet connection</span>
          <span>No signature</span>
          <span>Evidence pinned to block</span>
          <button type="button" onClick={loadExample}>Load example report →</button>
        </div>
        {notice && <p className="scan-notice" role="status">{notice}</p>}
      </section>

      <section className="signal-strip" aria-label="Chainseer capabilities">
        <div><strong>12</strong><span>risk dimensions</span></div>
        <div><strong>6</strong><span>hard-stop gates</span></div>
        <div><strong>Block-pinned</strong><span>market evidence</span></div>
        <div><strong>Timechain</strong><span>tamper-evident memory</span></div>
      </section>

      <section className="report-shell" id="report">
        <div className="section-heading">
          <div>
            <div className="eyebrow muted">Demonstration report</div>
            <h2>See the decision, then inspect the proof.</h2>
          </div>
          <p>
            Example data illustrates the report structure. It is not a live
            assessment or investment recommendation.
          </p>
        </div>

        <div className="decision-grid">
          <article className="decision-card">
            <div className="card-kicker">
              Effective action
              <span className="demo-tag">EXAMPLE</span>
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
            <div className="verified-seal"><span>◆</span> Integrity verified</div>
          </div>
          <div className="evidence-table" role="table" aria-label="Evidence ledger">
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
              <div><strong>Timechain proof</strong><span>Proof-of-Qualia sealed analysis</span></div>
            </div>
            <code>ring_000751 · 1e184f4121cdff67…</code>
          </div>
        </div>
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
            <span>RING 749</span><i />
            <span>RING 750</span><i />
            <span className="trace-active">RING 751 · VERIFIED</span>
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
        <span>© 2026 Chainseer</span>
      </footer>
    </main>
  );
}
