"use client";

import { FormEvent, useEffect, useId, useState } from "react";

const ADDRESS_RE = /^0x[a-fA-F0-9]{40}$/;
const SOLANA_ADDRESS_RE = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
type Network = "robinhood" | "base" | "solana";

type EntityGraphNode = {
  id: string;
  type: string;
  address: string;
  label?: string;
  roles: string[];
  attributes: Record<string, string | number | boolean | null>;
};

type EntityGraphEdge = {
  id: string;
  source: string;
  target: string;
  relationship: string;
  evidence_status: string;
  confidence: string;
  evidence_refs: string[];
  attributes: Record<string, string | number | boolean | null>;
};

type EntityGraphSignal = {
  id: string;
  code: string;
  severity: string;
  reason: string;
  entity_ids: string[];
  evidence_refs: string[];
  confidence: string;
};

type EntityGraph = {
  schema_version: string;
  network: string;
  root_entity_id: string;
  anchor: Record<string, string | number | boolean | null>;
  summary: {
    entity_count: number;
    relationship_count: number;
    privileged_entity_count: number;
    confirmed_relationship_count: number;
    provider_attested_relationship_count: number;
    signal_count: number;
    high_or_critical_signal_count: number;
    insider_risk_level: string;
    coverage: string;
    scoring_scope: string;
    changes_legitimacy_score: boolean;
  };
  nodes: EntityGraphNode[];
  edges: EntityGraphEdge[];
  signals: EntityGraphSignal[];
  limitations: string[];
  graph_hash: string;
};

type PublicReport = {
  schema_version: string;
  token: {
    address: string;
    name?: string;
    symbol?: string;
    chain: string;
    chain_id?: number | string;
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
    market_cap_kind?: "reported_market_cap" | "fdv_proxy" | "unavailable" | string;
    market_cap_source?: string | null;
    fdv_usd?: number | string | null;
    liquidity_usd?: number | string | null;
    volume_24h_usd?: number | string | null;
    age?: string | null;
  };
  holders: {
    count?: number | null;
    count_status?: "reported" | "unavailable" | string;
    count_source?: string | null;
    sample_size?: number;
    sample_kind?: string | null;
    largest_holder_pct?: number | null;
    top10_holder_pct?: number | null;
    concentration_basis?: string | null;
    pool_and_program_vaults_excluded?: boolean | null;
    caveat?: string;
  };
  evidence: {
    fact_count: number;
    block_pin?: number;
    anchor_type?: "block_pin" | "confirmed_slot_anchor" | string;
    anchor_caveat?: string;
    infrastructure_indeterminate?: string[];
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
    cognitive_ring?: number;
    cognitive_ring_hash?: string;
    cognition?: {
      version?: string;
      status?: string;
      input_policy?: string;
      salience?: number;
      dissonance?: number;
      senses?: Array<{ id?: number; name?: string }>;
      modalities?: Array<{ id?: number; name?: string }>;
      frames?: string[];
      relevant_rings?: number[];
      retrieved_faculties?: string[];
      growth?: Array<{
        action?: string;
        faculty?: string;
        kind?: string;
        reason?: string;
      }>;
      immune?: { status?: string; covenant?: number };
    };
  };
  entity_graph?: EntityGraph;
  analyzed_at?: string;
  disclaimer: string;
};

type ScanState = "idle" | "submitting" | "analyzing" | "succeeded" | "failed";

type TokenMonitor = {
  key: string;
  network: Network;
  address: string;
  label: string;
  cursor: string;
};

type CriticalAlert = {
  alert_hash: string;
  network: Network;
  token_address: string;
  severity: "critical";
  categories: string[];
  title: string;
  message: string;
  anchor?: number;
  anchor_type?: string;
  observed_at: string;
  new_hard_stops?: string[];
  timechain?: {
    ring?: number;
    ring_hash?: string;
    decision?: string;
  };
};

type MemoryPillar = {
  ok?: boolean;
  ring_count?: number;
  head_index?: number;
  subject_count?: number;
  analysis_ring_count?: number;
  faculty_count?: number;
  pattern_count?: number;
  canonical_records?: number;
  learning_eligible?: number;
  citation_policy?: string;
  tighten_only?: boolean;
};

type MemoryCoreStatus = {
  schema_version: string;
  status: "healthy" | "degraded";
  checked_at: string;
  status_hash: string;
  pillars: Record<string, MemoryPillar>;
  recovery?: {
    last_drill?: {
      status?: string;
      completed_at?: string;
      recovery?: {
        rpo_rings?: number;
        rto_seconds?: number;
        deterministic_rebuild?: boolean;
      };
    } | null;
    last_drill_freshness?: {
      current_head_index?: number;
      drill_head_index?: number;
      rings_behind?: number;
      current_head_match?: boolean;
    };
  };
};

const MONITOR_STORAGE_KEY = "chainseer-critical-monitors-v1";
const MAX_DEVICE_MONITORS = 10;

function reportNetwork(chain: string): Network {
  const normalized = chain.toLowerCase();
  if (normalized.includes("solana")) return "solana";
  if (normalized === "base" || normalized.includes("base mainnet")) {
    return "base";
  }
  return "robinhood";
}

function networkLabel(network: Network) {
  if (network === "robinhood") return "Robinhood Chain";
  if (network === "base") return "Base";
  return "Solana";
}

const factorNames: Record<string, string> = {
  security: "Token controls",
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

// Plain-language explanations for the Q&A section below -- kept as a
// separate map (rather than folded into factorNames) so the short label
// used on live reports and the longer explanation used in the FAQ can
// each be edited without disturbing the other. Keys must match
// factorNames so every dimension a report can actually return has an
// explanation here.
const factorDescriptions: Record<string, string> = {
  security:
    "Whether privileged contract functions -- mint authority, ownership, upgradability, pause or blacklist controls -- are still active and could change the rules after you buy.",
  honeypot_safety:
    "Whether provider evidence and chain-specific simulations or route quotes indicate that buying and selling are possible without prohibitive restrictions. The public scan does not execute a funded trade.",
  liquidity:
    "How much capital is actually sitting in the trading pool relative to typical trade sizes. Thin liquidity means even a modest sell can move the price sharply.",
  lp_lock:
    "Who controls the primary liquidity: whether custody is burned, timelocked, framework-controlled, creator-withdrawable, or still unverified. An unlocked label alone is not treated as proof of danger.",
  holder_distribution:
    "How concentrated the circulating supply is among the largest wallets, with pool and program vaults excluded so they don't distort the picture.",
  volume:
    "Whether trading volume looks organic relative to liquidity, versus signs of wash trading or a market that's effectively dead.",
  maturity:
    "How long the token has actually been trading. Very new tokens carry structurally higher uncertainty regardless of what else checks out.",
  creator_risk:
    "What's knowable about the wallet(s) that deployed or control the token, when that can be established from on-chain evidence.",
  wash_trading:
    "Whether the observed trading pattern shows signs of self-dealing meant to inflate apparent activity.",
  deployer:
    "The deploying wallet's track record -- whether it has launched other tokens, and how those played out.",
  sentiment: "The balance of buys versus sells in recent activity.",
  trend: "Recent price direction and momentum.",
};

// A factor dimension is 0-100, "higher is safer." Two tones (good/warn)
// made a near-zero score look identical to a merely-mediocre one, hiding
// exactly the signal this panel exists to surface. Thresholds mirror the
// headline risk badge's red/amber split (see .live-risk.risk-critical /
// .risk-medium in globals.css) so a bad sub-score and a bad overall verdict
// read as the same color everywhere on the page.
function factorTone(score: number): "good" | "warn" | "bad" {
  if (score >= 70) return "good";
  if (score >= 40) return "warn";
  return "bad";
}

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

function formatCount(value: number | null | undefined) {
  const numericValue = Number(value);
  if (value === null || value === undefined || !Number.isFinite(numericValue) || numericValue <= 0) {
    return "Unavailable";
  }
  return new Intl.NumberFormat("en-US", {
    notation: numericValue >= 10_000 ? "compact" : "standard",
    maximumFractionDigits: 1,
  }).format(numericValue);
}

const privilegedRoles = new Set([
  "deployer",
  "contract_owner",
  "liquidity_controller",
  "mint_authority",
  "freeze_authority",
]);
const confirmedStatuses = new Set(["onchain_confirmed", "cross_source_confirmed"]);
type GraphFilter = "all" | "confirmed" | "attested";

function titleCase(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function shortAddress(value: string) {
  if (!value) return "Address unavailable";
  return value.length > 20 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value;
}

const memoryPillarLabels: Record<string, { index: string; title: string; description: string }> = {
  timechain_ledger: {
    index: "01",
    title: "Timechain Ledger",
    description: "Immutable, hash-linked analysis and reflection history.",
  },
  entity_knowledge_graph: {
    index: "02",
    title: "Entity Knowledge Graph",
    description: "Temporal relationships and risk evolution rebuilt from Rings.",
  },
  pattern_faculty_store: {
    index: "03",
    title: "Pattern & Faculty Store",
    description: "Governed learning with an enforced tighten-only activation gate.",
  },
  outcome_ledger: {
    index: "04",
    title: "Outcome Ledger",
    description: "Ground truth bound to the original Ring, evidence hash, and pin.",
  },
  query_recall_engine: {
    index: "05",
    title: "Query & Recall Engine",
    description: "Subject-scoped answers with exact evidence citations or no claim.",
  },
};

function MemoryCoreDashboard() {
  const [status, setStatus] = useState<MemoryCoreStatus | null>(null);
  const [unavailable, setUnavailable] = useState(false);

  useEffect(() => {
    let active = true;
    async function loadStatus() {
      try {
        const response = await fetch("/api/memory", { cache: "no-store" });
        const body = await response.json().catch(() => null);
        if (!active) return;
        if (!response.ok || !body?.pillars) {
          setUnavailable(true);
          return;
        }
        setStatus(body as MemoryCoreStatus);
        setUnavailable(false);
      } catch {
        if (active) setUnavailable(true);
      }
    }
    void loadStatus();
    const timer = window.setInterval(() => void loadStatus(), 60_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  const lastDrill = status?.recovery?.last_drill;
  const drillFreshness = status?.recovery?.last_drill_freshness;
  return (
    <section className="memory-core" id="memory-core" aria-labelledby="memory-core-title">
      <div className="section-heading memory-core-heading">
        <div>
          <div className="eyebrow">Persistent · verifiable · self-improving</div>
          <h2 id="memory-core-title">Timechain Memory Core</h2>
        </div>
        <p>
          Five linked layers turn individual scans into auditable intelligence.
          Derived views can be deleted and rebuilt; the verified Timechain remains
          the source of truth.
        </p>
      </div>
      <div className="memory-integrity-bar">
        <div>
          <span className={`memory-status-dot ${status?.status === "healthy" ? "is-healthy" : ""}`} />
          <strong>
            {status
              ? status.status === "healthy"
                ? "Memory integrity verified"
                : "Memory integrity needs attention"
              : unavailable
                ? "Live integrity status unavailable"
                : "Checking Memory Core…"}
          </strong>
        </div>
        <code>
          {status?.status_hash ? `${status.status_hash.slice(0, 12)}…` : "STATUS PENDING"}
        </code>
      </div>
      <div className="memory-pillar-grid">
        {Object.entries(memoryPillarLabels).map(([key, label]) => {
          const pillar = status?.pillars?.[key];
          let metric = "Verifiable by design";
          if (key === "timechain_ledger" && pillar?.ring_count !== undefined) {
            metric = `${pillar.ring_count.toLocaleString()} Rings`;
          } else if (key === "entity_knowledge_graph" && pillar?.subject_count !== undefined) {
            metric = `${pillar.subject_count.toLocaleString()} subjects`;
          } else if (key === "pattern_faculty_store" && pillar?.faculty_count !== undefined) {
            metric = `${pillar.faculty_count} faculties · ${pillar.pattern_count ?? 0} patterns`;
          } else if (key === "outcome_ledger" && pillar?.canonical_records !== undefined) {
            metric = `${pillar.canonical_records} canonical · ${pillar.learning_eligible ?? 0} eligible`;
          } else if (key === "query_recall_engine") {
            metric = "100% citation policy";
          }
          return (
            <article key={key}>
              <div className="memory-pillar-head">
                <span>{label.index}</span>
                <i className={pillar?.ok === false ? "is-degraded" : "is-verified"}>
                  {pillar?.ok === false ? "CHECK" : "VERIFIED"}
                </i>
              </div>
              <h3>{label.title}</h3>
              <p>{label.description}</p>
              <strong>{metric}</strong>
            </article>
          );
        })}
      </div>
      <div className="memory-operations">
        <div>
          <span>Recovery proof</span>
          <strong>
            {lastDrill?.status === "passed" ? "Last isolated drill passed" : "Drill evidence pending"}
          </strong>
          <small>
            {lastDrill?.recovery
              ? `RPO ${lastDrill.recovery.rpo_rings ?? "—"} Rings · RTO ${lastDrill.recovery.rto_seconds ?? "—"}s · deterministic rebuild ${lastDrill.recovery.deterministic_rebuild ? "verified" : "not verified"} · ${drillFreshness?.current_head_match ? "current head" : `${drillFreshness?.rings_behind ?? "—"} Rings behind`}`
              : "Backups and rebuilds are verified outside the live Timechain before recovery is trusted."}
          </small>
        </div>
        <div>
          <span>Autonomy boundary</span>
          <strong>Tighten-only is enforced</strong>
          <small>
            Learned faculties cannot lower risk thresholds, remove hard stops,
            sign, broadcast, or enable live capital.
          </small>
        </div>
      </div>
    </section>
  );
}

function nodePriority(node: EntityGraphNode, graph: EntityGraph) {
  let score = 0;
  if (node.id === graph.root_entity_id) score += 1000;
  if (graph.signals.some((signal) => signal.entity_ids.includes(node.id))) score += 500;
  if (node.roles.some((role) => privilegedRoles.has(role))) score += 300;
  if (["primary_market", "proxy", "implementation"].some((role) => node.roles.includes(role))) {
    score += 180;
  }
  const holderRank = Number(node.attributes?.rank);
  if (Number.isFinite(holderRank) && holderRank > 0) score += Math.max(0, 120 - holderRank);
  return score;
}

function EntityEvidenceGraph({ graph }: { graph?: EntityGraph }) {
  const [filter, setFilter] = useState<GraphFilter>("all");
  const [selectedId, setSelectedId] = useState(graph?.root_entity_id || "");
  const titleId = useId();

  if (!graph?.graph_hash || !graph.nodes?.length) {
    return (
      <section className="entity-graph entity-graph-empty" aria-label="Entity evidence graph">
        <details className="analysis-disclosure">
          <summary className="analysis-disclosure-trigger">
            <div>
              <span className="panel-index">03</span>
              <h3>Entity &amp; insider evidence</h3>
            </div>
          </summary>
          <div className="analysis-disclosure-body">
            <p>
              Entity graph unavailable for this report. Run a fresh analysis after the graph
              rollout to map privileged actors and evidence-backed relationships.
            </p>
          </div>
        </details>
      </section>
    );
  }

  const visibleNodes = [...graph.nodes]
    .sort((left, right) => nodePriority(right, graph) - nodePriority(left, graph))
    .slice(0, 16);
  const visibleIds = new Set(visibleNodes.map((node) => node.id));
  const visibleEdges = graph.edges.filter((edge) => {
    if (!visibleIds.has(edge.source) || !visibleIds.has(edge.target)) return false;
    if (filter === "confirmed") return confirmedStatuses.has(edge.evidence_status);
    if (filter === "attested") return edge.evidence_status === "provider_attested";
    return true;
  });
  const selectedNode =
    graph.nodes.find((node) => node.id === selectedId) ||
    graph.nodes.find((node) => node.id === graph.root_entity_id) ||
    visibleNodes[0];
  const selectedEdges = graph.edges
    .filter((edge) => edge.source === selectedNode.id || edge.target === selectedNode.id)
    .slice(0, 8);
  const selectedSignals = graph.signals.filter((signal) =>
    signal.entity_ids.includes(selectedNode.id),
  );
  const signalEntityIds = new Set(graph.signals.flatMap((signal) => signal.entity_ids));
  const root = visibleNodes.find((node) => node.id === graph.root_entity_id) || visibleNodes[0];
  const orbitingNodes = visibleNodes.filter((node) => node.id !== root.id);
  const positions = new Map<string, { x: number; y: number }>([[root.id, { x: 500, y: 280 }]]);
  orbitingNodes.forEach((node, index) => {
    const angle = -Math.PI / 2 + (Math.PI * 2 * index) / Math.max(orbitingNodes.length, 1);
    const wideOrbit = index % 2 === 0;
    positions.set(node.id, {
      x: 500 + Math.cos(angle) * (wideOrbit ? 375 : 285),
      y: 280 + Math.sin(angle) * (wideOrbit ? 205 : 165),
    });
  });
  const hiddenCount = Math.max(0, graph.nodes.length - visibleNodes.length);
  const attributeEntries = Object.entries(selectedNode.attributes || {})
    .filter(([, value]) => value !== null && value !== "")
    .slice(0, 6);

  return (
    <section className="entity-graph" aria-labelledby={titleId}>
      <details className="analysis-disclosure">
        <summary className="entity-graph-head analysis-disclosure-trigger">
          <div>
            <span className="panel-index">03</span>
            <p className="entity-kicker">Evidence-linked ownership and control</p>
            <h3 id={titleId}>Entity &amp; insider evidence</h3>
          </div>
          <div className={`insider-risk risk-${graph.summary.insider_risk_level.toLowerCase()}`}>
            <span>Insider risk</span>
            <strong>{graph.summary.insider_risk_level}</strong>
            <small>{graph.summary.coverage} coverage</small>
          </div>
        </summary>

        <div className="analysis-disclosure-body">
          <div className="entity-graph-summary" aria-label="Entity graph summary">
            <article><span>Entities</span><strong>{graph.summary.entity_count}</strong></article>
            <article><span>Privileged</span><strong>{graph.summary.privileged_entity_count}</strong></article>
            <article><span>Confirmed links</span><strong>{graph.summary.confirmed_relationship_count}</strong></article>
            <article><span>High / critical signals</span><strong>{graph.summary.high_or_critical_signal_count}</strong></article>
          </div>

          <div className="entity-graph-toolbar">
        <div className="graph-filters" aria-label="Relationship evidence filter">
          {([
            ["all", "All evidence"],
            ["confirmed", "Confirmed"],
            ["attested", "Provider-attested"],
          ] as Array<[GraphFilter, string]>).map(([value, label]) => (
            <button
              type="button"
              key={value}
              className={filter === value ? "active" : ""}
              aria-pressed={filter === value}
              onClick={() => setFilter(value)}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="graph-legend" aria-label="Evidence legend">
          <span className="legend-onchain">Onchain</span>
          <span className="legend-cross">Cross-source</span>
          <span className="legend-attested">Attested</span>
        </div>
          </div>

          <div className="entity-graph-layout">
        <div className="entity-graph-stage">
          <svg viewBox="0 0 1000 560" role="img" aria-label="Interactive entity relationship graph">
            <g aria-hidden="true">
              {visibleEdges.map((edge) => {
                const source = positions.get(edge.source);
                const target = positions.get(edge.target);
                if (!source || !target) return null;
                const connected = edge.source === selectedNode.id || edge.target === selectedNode.id;
                return (
                  <line
                    key={edge.id}
                    x1={source.x}
                    y1={source.y}
                    x2={target.x}
                    y2={target.y}
                    className={`graph-link link-${edge.evidence_status}${connected ? " selected" : ""}`}
                  />
                );
              })}
            </g>
            {visibleNodes.map((node) => {
              const position = positions.get(node.id) || { x: 500, y: 280 };
              const isSelected = node.id === selectedNode.id;
              const isRoot = node.id === root.id;
              const hasSignal = signalEntityIds.has(node.id);
              const label = (node.label || titleCase(node.type)).slice(0, 20);
              return (
                <g
                  key={node.id}
                  transform={`translate(${position.x} ${position.y})`}
                  className={`graph-node${isSelected ? " selected" : ""}${isRoot ? " root" : ""}${hasSignal ? " signal" : ""}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`${label}, ${shortAddress(node.address)}`}
                  aria-pressed={isSelected}
                  onClick={() => setSelectedId(node.id)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setSelectedId(node.id);
                    }
                  }}
                >
                  <rect x="-66" y="-29" width="132" height="58" rx="4" />
                  <text className="graph-node-label" textAnchor="middle" y="-4">{label}</text>
                  <text className="graph-node-address" textAnchor="middle" y="13">
                    {shortAddress(node.address)}
                  </text>
                </g>
              );
            })}
          </svg>
          <p className="graph-stage-note">
            Showing {visibleNodes.length} priority entities
            {hiddenCount ? ` · ${hiddenCount} lower-priority entities collapsed` : ""}
          </p>
        </div>

        <aside className="entity-inspector" aria-live="polite">
          <span className="inspector-type">{titleCase(selectedNode.type)}</span>
          <h4>{selectedNode.label || shortAddress(selectedNode.address)}</h4>
          <code title={selectedNode.address}>{selectedNode.address}</code>

          <div className="role-list">
            {(selectedNode.roles.length ? selectedNode.roles : ["observed_entity"]).map((role) => (
              <span key={role}>{titleCase(role)}</span>
            ))}
          </div>

          {selectedSignals.length > 0 && (
            <div className="inspector-block signal-block">
              <strong>Related signals</strong>
              {selectedSignals.map((signal) => (
                <article key={signal.id}>
                  <span>{signal.severity} · {titleCase(signal.code)}</span>
                  <p>{signal.reason}</p>
                </article>
              ))}
            </div>
          )}

          <div className="inspector-block">
            <strong>Relationships</strong>
            {selectedEdges.length ? (
              <ul>
                {selectedEdges.map((edge) => (
                  <li key={edge.id}>
                    <span>{titleCase(edge.relationship)}</span>
                    <small>{titleCase(edge.evidence_status)} · {edge.confidence}</small>
                  </li>
                ))}
              </ul>
            ) : <p>No mapped relationship for this entity.</p>}
          </div>

          {attributeEntries.length > 0 && (
            <dl className="inspector-attributes">
              {attributeEntries.map(([key, value]) => (
                <div key={key}><dt>{titleCase(key)}</dt><dd>{String(value)}</dd></div>
              ))}
            </dl>
          )}
        </aside>
          </div>

          <div className="entity-graph-foot">
            <p>{graph.limitations[0] || "Relationships are limited to the evidence collected for this analysis."}</p>
            <code title={graph.graph_hash}>Graph hash {graph.graph_hash.slice(0, 18)}…</code>
          </div>
        </div>
      </details>
    </section>
  );
}

function LiveReport({
  report,
  isMonitoring,
  monitorBusy,
  onToggleMonitor,
  isDemo = false,
}: {
  report: PublicReport;
  isMonitoring: boolean;
  monitorBusy: boolean;
  onToggleMonitor: () => void;
  isDemo?: boolean;
}) {
  const factorEntries = Object.entries(report.factors)
    .filter(([key, value]) => key !== "legitimacy" && Number.isFinite(value))
    .map(([key, value]) => {
      const score = Math.max(0, Math.min(100, Number(value)));
      return {
        key,
        label: factorNames[key] || key.replaceAll("_", " "),
        score,
        tone: factorTone(score),
      };
    });
  const hardStops = report.decision.hard_stops || [];
  const risk = (report.decision.risk_level || "Unknown").toLowerCase();
  const isSlotAnchor = report.evidence.anchor_type === "confirmed_slot_anchor";
  const anchorLabel = isSlotAnchor ? "Slot anchor" : "Block pin";
  const marketCapKind = report.market.market_cap_kind || "unavailable";
  const marketCapValue = marketCapKind === "unavailable"
    ? report.market.fdv_usd
    : report.market.market_cap_usd;
  const marketCapLabel = marketCapKind === "fdv_proxy"
    ? "FDV estimate"
    : marketCapKind === "unavailable" && report.market.fdv_usd
      ? "Fully diluted value"
      : "Market cap";
  const holderCount = report.holders?.count;
  const holderValue = holderCount
    ? formatCount(holderCount)
    : report.holders?.sample_size
      ? `${report.holders.sample_size} sampled`
      : "Unavailable";
  const holderDetail = holderCount
    ? `${report.holders.count_source || "Provider"} reported`
    : report.holders?.sample_size
      ? "Largest accounts only"
      : "No reliable count returned";
  const cognition = report.timechain.cognition;
  const activeSenses = cognition?.senses?.filter((item) => item.name) || [];
  const activeModalities = cognition?.modalities?.filter((item) => item.name) || [];
  const cognitiveGrowth = cognition?.growth?.filter(
    (item) => item.action && item.action !== "none",
  ) || [];

  return (
    <section
      className={`live-report ${isDemo ? "demo-live-report" : ""}`}
      id={isDemo ? "demo-live-report" : "live-report"}
      aria-live={isDemo ? undefined : "polite"}
    >
      <div className="live-report-head">
        <div>
          <div className="eyebrow">
            <span className="status-dot" />
            {isDemo ? "Fictional report preview · not sealed" : "Live sealed analysis"}
          </div>
          <h2>
            {report.token.name || "Unknown token"}
            {report.token.symbol ? ` (${report.token.symbol})` : ""}
          </h2>
          <p className="live-address">{report.token.address}</p>
          <p className="live-chain">{report.token.chain}</p>
        </div>
        <div className={`live-risk risk-${risk}`}>
          <span>{report.decision.action || "REVIEW"}</span>
          <strong>{report.decision.score ?? "—"}<small>/100</small></strong>
          <em>{report.decision.risk_level || "Unknown"} risk</em>
        </div>
      </div>

      <div className="live-market-grid" aria-label="Token market and holder snapshot">
        <article>
          <span>{marketCapLabel}</span>
          <strong>{formatMoney(marketCapValue)}</strong>
          <p>
            {marketCapKind === "fdv_proxy"
              ? "Circulating market cap was unavailable"
              : report.market.market_cap_source || "Market source unavailable"}
          </p>
        </article>
        <article title={report.holders?.caveat}>
          <span>Holders</span>
          <strong>{holderValue}</strong>
          <p>{holderDetail}</p>
        </article>
        <article>
          <span>Liquidity</span>
          <strong>{formatMoney(report.market.liquidity_usd)}</strong>
          <p>Observed across reported markets</p>
        </article>
        <article>
          <span>24h volume</span>
          <strong>{formatMoney(report.market.volume_24h_usd)}</strong>
          <p>{report.market.age || "Market age unknown"}</p>
        </article>
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
          <span>Trading snapshot</span>
          <strong>{formatMoney(report.market.price_usd)} price</strong>
          <p>
            {formatMoney(report.market.volume_24h_usd)} volume ·{" "}
            {report.holders?.largest_holder_pct != null
              ? `Largest observed holder ${Number(report.holders.largest_holder_pct).toFixed(1)}%`
              : report.market.age || "Market age unknown"}
          </p>
        </article>
        <article>
          <span>Evidence</span>
          <strong>{report.evidence.fact_count} facts</strong>
          <p>
            {isSlotAnchor ? "Slot" : "Block"}{" "}
            {report.evidence.block_pin?.toLocaleString() || "unknown"}
            {(report.evidence.infrastructure_indeterminate?.length || 0) > 0
              ? ` · ${report.evidence.infrastructure_indeterminate?.length} source indeterminate`
              : ""}
          </p>
        </article>
      </div>

      <div className="live-recommendation">
        <span>Investor interpretation</span>
        <p>{report.decision.recommendation || "Review the evidence before making a decision."}</p>
      </div>

      <div className={`critical-monitor ${isMonitoring ? "monitor-active" : ""} ${isDemo ? "monitor-demo" : ""}`}>
        <div>
          <span>Critical state monitor</span>
          <strong>
            {isDemo
              ? "Available after a real token scan"
              : isMonitoring
              ? "Watching liquidity, authority, upgrades, and sellability"
              : "Get alerted when a critical token state changes"}
          </strong>
          <p>
            {isDemo
              ? "Monitoring is disabled for fictional sample data. A real report can activate the device-local critical-event watcher."
              : "Confirmed events are checked near real time while this site is open. Delivery follows watcher cadence and cannot be guaranteed before a transaction."}
          </p>
        </div>
        <button
          type="button"
          onClick={onToggleMonitor}
          disabled={isDemo || monitorBusy}
          aria-pressed={isDemo ? undefined : isMonitoring}
        >
          {isDemo
            ? "Demo only"
            : monitorBusy
            ? "Updating…"
            : isMonitoring
              ? "Stop monitoring"
              : "Monitor critical events"}
        </button>
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

      <EntityEvidenceGraph graph={report.entity_graph} />

      <div className="live-detail-grid">
        <details className="live-factors analysis-disclosure">
          <summary className="panel-head analysis-disclosure-trigger">
            <div><span className="panel-index">01</span><h3>Risk dimensions</h3></div>
            <span>Higher is safer</span>
          </summary>
          <div className="analysis-disclosure-body">
            <div className="factor-grid">
              {factorEntries.map((factor) => (
                <div className="factor" key={factor.key}>
                  <div className="factor-label">
                    <span>{factor.label}</span><strong>{factor.score}</strong>
                  </div>
                  <div className="bar">
                    <span
                      className={factor.tone}
                      style={{ width: `${factor.score}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </details>

        <details className="live-evidence analysis-disclosure">
          <summary className="panel-head analysis-disclosure-trigger">
            <div><span className="panel-index">02</span><h3>Verification</h3></div>
            <div className="verified-seal"><span>◆</span> {isDemo ? "DEMO · NOT SEALED" : report.timechain.decision || "SEALED"}</div>
          </summary>
          <div className="analysis-disclosure-body">
            <dl>
              <div><dt>Timechain Ring</dt><dd>{isDemo ? "Not sealed" : report.timechain.ring ?? "—"}</dd></div>
              <div><dt>Ring hash</dt><dd>{isDemo ? "Not created" : `${report.timechain.ring_hash?.slice(0, 20) || "—"}…`}</dd></div>
              <div><dt>Ledger hash</dt><dd>{isDemo ? "Illustrative only" : `${report.evidence.ledger_hash?.slice(0, 20) || "—"}…`}</dd></div>
              <div><dt>{isDemo ? `Illustrative ${anchorLabel.toLowerCase()}` : anchorLabel}</dt><dd>{report.evidence.block_pin?.toLocaleString() || "—"}</dd></div>
            </dl>
            {report.evidence.anchor_caveat && (
              <p className="anchor-caveat">{report.evidence.anchor_caveat}</p>
            )}
            {report.token.explorer_url && (
              <a href={report.token.explorer_url} target="_blank" rel="noreferrer">
                Open token explorer ↗
              </a>
            )}
          </div>
        </details>

        <details className="live-cognition analysis-disclosure">
          <summary className="panel-head analysis-disclosure-trigger">
            <div><span className="panel-index">03</span><h3>Cognitive trace</h3></div>
            <span>
              {isDemo
                ? "Not run for demo"
                : cognition?.status === "complete"
                  ? "Timechain-complete"
                  : "Unavailable"}
            </span>
          </summary>
          <div className="analysis-disclosure-body cognitive-trace-body">
            {cognition ? (
              <>
                <div className="cognitive-overview">
                  <div>
                    <span>Input boundary</span>
                    <strong>
                      {cognition.input_policy === "trusted_structured_fields_only"
                        ? "Trusted structured evidence only"
                        : cognition.input_policy || "Unspecified"}
                    </strong>
                  </div>
                  <div>
                    <span>Cognitive ring</span>
                    <strong>{report.timechain.cognitive_ring ?? "—"}</strong>
                  </div>
                  <div>
                    <span>Salience</span>
                    <strong>{cognition.salience ?? "—"}</strong>
                  </div>
                  <div>
                    <span>Dissonance</span>
                    <strong>{cognition.dissonance ?? "—"}</strong>
                  </div>
                  <div>
                    <span>Covenant membrane</span>
                    <strong>{cognition.immune?.status || "—"}</strong>
                  </div>
                </div>

                <div className="cognitive-lens-grid">
                  <div>
                    <span>Active senses</span>
                    <div className="cognitive-chips">
                      {activeSenses.length ? activeSenses.map((item) => (
                        <span key={`sense-${item.id}-${item.name}`}>{item.name}</span>
                      )) : <em>No sense labels returned</em>}
                    </div>
                  </div>
                  <div>
                    <span>Reasoning modalities</span>
                    <div className="cognitive-chips">
                      {activeModalities.length ? activeModalities.map((item) => (
                        <span key={`modality-${item.id}-${item.name}`}>{item.name}</span>
                      )) : <em>No modality labels returned</em>}
                    </div>
                  </div>
                </div>

                {(cognition.frames?.length || 0) > 0 && (
                  <div className="cognitive-frames">
                    <span>Applied reasoning frames</span>
                    <ul>
                      {cognition.frames?.map((frame, index) => (
                        <li key={`${frame}-${index}`}>{frame}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {cognitiveGrowth.length > 0 && (
                  <div className="cognitive-growth">
                    <span>Faculty changes sealed after this analysis</span>
                    <ul>
                      {cognitiveGrowth.map((item, index) => (
                        <li key={`${item.action}-${item.faculty}-${index}`}>
                          <strong>{item.action}</strong>{" "}
                          {item.faculty || item.kind || "faculty"}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                <p className="cognitive-boundary">
                  The trace shows which Timechain lenses fired and what was
                  sealed. Deterministic on-chain evidence, scoring rules, and
                  hard-stop gates remain authoritative; faculties cannot sign
                  or broadcast transactions.
                  {(cognition.relevant_rings?.length || 0) > 0
                    ? ` Recalled rings: ${cognition.relevant_rings?.join(", ")}.`
                    : ""}
                </p>
              </>
            ) : (
              <p className="cognitive-empty">
                {isDemo
                  ? "The fictional preview does not execute or seal a cognitive loop. Run a real scan to see active senses, modalities, recall, and the cognitive completion ring."
                  : "This report predates the public cognitive-trace schema or the cognitive loop did not return a trace."}
              </p>
            )}
          </div>
        </details>
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

const demoReport: PublicReport = {
  schema_version: "chainseer.public.v1",
  token: {
    address: "No contract address — fictional sample",
    name: "Sentinel Demo",
    symbol: "DEMO",
    chain: "Base · fictional example",
    chain_id: 8453,
  },
  decision: {
    action: "WATCHLIST",
    risk_level: "Medium",
    model_risk_level: "Medium",
    score: 78,
    confidence: "MODERATE",
    confidence_detail:
      "Illustrative evidence coverage with one deliberately unresolved deployer-history check.",
    recommendation:
      "No deterministic hard stop appears in this fictional example, but concentration and market maturity warrant monitoring before exposure.",
    hard_stops: [],
  },
  factors: {
    security: 94,
    honeypot_safety: 95,
    liquidity: 76,
    lp_lock: 82,
    holder_distribution: 61,
    volume: 72,
    maturity: 45,
    creator_risk: 68,
    wash_trading: 87,
    deployer: 50,
    sentiment: 70,
    trend: 66,
  },
  flags: {
    red: [],
    yellow: [
      "Top 10 non-pool holders control an illustrative 31.8% of supply",
      "Fictional market is only three days old",
    ],
    green: [
      "Illustrative source code is verified",
      "No sell restriction appears in the sample evidence",
      "Primary liquidity custody appears constrained in the sample",
    ],
    unknown: {
      deployer: "Historical deployer outcomes were intentionally omitted from the demo.",
    },
  },
  market: {
    price_usd: 0.00428,
    market_cap_usd: 4_280_000,
    market_cap_kind: "reported_market_cap",
    market_cap_source: "Fictional DexScreener-style sample",
    fdv_usd: 4_280_000,
    liquidity_usd: 185_000,
    volume_24h_usd: 92_400,
    age: "3d old · fictional",
  },
  holders: {
    count: 1_842,
    count_status: "reported",
    count_source: "Fictional explorer sample",
    sample_size: 20,
    sample_kind: "largest non-pool holders",
    largest_holder_pct: 12.4,
    top10_holder_pct: 31.8,
    concentration_basis: "illustrative circulating supply",
    pool_and_program_vaults_excluded: true,
    caveat: "All holder values in this demonstration are fictional.",
  },
  evidence: {
    fact_count: 7,
    block_pin: 21_234_567,
    anchor_type: "block_pin",
    anchor_caveat:
      "This block number is illustrative. No node or provider was queried and no evidence ledger was created.",
    infrastructure_indeterminate: [],
    facts: [
      { id: "DEMO-001", source: "Fictional RPC", block: 21_234_567 },
      { id: "DEMO-002", source: "Fictional Blockscout", block: 21_234_567 },
      { id: "DEMO-003", source: "Fictional GoPlus", block: 21_234_567 },
      { id: "DEMO-004", source: "Fictional DexScreener", block: 21_234_567 },
      { id: "DEMO-005", source: "Fictional holder sample", block: 21_234_567 },
      { id: "DEMO-006", source: "Fictional liquidity check", block: 21_234_567 },
      { id: "DEMO-007", source: "Fictional market snapshot", block: 21_234_567 },
    ],
  },
  timechain: {
    decision: "DEMO · NOT SEALED",
    scores: {},
  },
  entity_graph: {
    schema_version: "chainseer.entity_graph.v1",
    network: "base",
    root_entity_id: "demo-token",
    anchor: { block_pin: 21_234_567, illustrative: true },
    summary: {
      entity_count: 4,
      relationship_count: 3,
      privileged_entity_count: 1,
      confirmed_relationship_count: 2,
      provider_attested_relationship_count: 1,
      signal_count: 1,
      high_or_critical_signal_count: 0,
      insider_risk_level: "MEDIUM",
      coverage: "ILLUSTRATIVE",
      scoring_scope: "demo_only",
      changes_legitimacy_score: false,
    },
    nodes: [
      {
        id: "demo-token",
        type: "token",
        address: "demo:token",
        label: "Sentinel Demo",
        roles: ["analyzed_token"],
        attributes: { symbol: "DEMO", fictional: true },
      },
      {
        id: "demo-deployer",
        type: "wallet",
        address: "demo:deployer",
        label: "Fictional deployer",
        roles: ["deployer"],
        attributes: { attribution: "illustrative" },
      },
      {
        id: "demo-market",
        type: "market",
        address: "demo:primary-market",
        label: "Fictional primary market",
        roles: ["primary_market"],
        attributes: { liquidity_usd: 185_000 },
      },
      {
        id: "demo-holder",
        type: "wallet",
        address: "demo:largest-holder",
        label: "Largest observed holder",
        roles: ["holder"],
        attributes: { rank: 1, supply_pct: 12.4 },
      },
    ],
    edges: [
      {
        id: "demo-edge-1",
        source: "demo-deployer",
        target: "demo-token",
        relationship: "deployed",
        evidence_status: "onchain_confirmed",
        confidence: "illustrative",
        evidence_refs: ["DEMO-002"],
        attributes: {},
      },
      {
        id: "demo-edge-2",
        source: "demo-token",
        target: "demo-market",
        relationship: "trades_on",
        evidence_status: "cross_source_confirmed",
        confidence: "illustrative",
        evidence_refs: ["DEMO-004", "DEMO-006"],
        attributes: {},
      },
      {
        id: "demo-edge-3",
        source: "demo-holder",
        target: "demo-token",
        relationship: "holds",
        evidence_status: "provider_attested",
        confidence: "illustrative",
        evidence_refs: ["DEMO-005"],
        attributes: { supply_pct: 12.4 },
      },
    ],
    signals: [
      {
        id: "demo-signal-1",
        code: "holder_concentration_watch",
        severity: "medium",
        reason: "The fictional top-10 concentration merits continued monitoring.",
        entity_ids: ["demo-holder", "demo-token"],
        evidence_refs: ["DEMO-005"],
        confidence: "illustrative",
      },
    ],
    limitations: [
      "This graph demonstrates the report format only; every entity and relationship is fictional.",
    ],
    graph_hash: "demo_graph_hash_not_sealed",
  },
  analyzed_at: "Fictional preview",
  disclaimer:
    "SAMPLE DATA · NOT LIVE. This report was not generated from a token, no external source was queried, and nothing was sealed. It is not financial advice.",
};

export default function Home() {
  const [network, setNetwork] = useState<Network>("robinhood");
  const [address, setAddress] = useState("");
  const [notice, setNotice] = useState("");
  const [scanState, setScanState] = useState<ScanState>("idle");
  const [scanProgress, setScanProgress] = useState(0);
  const [liveReport, setLiveReport] = useState<PublicReport | null>(null);
  const [showExample, setShowExample] = useState(false);
  const [monitors, setMonitors] = useState<TokenMonitor[]>([]);
  const [monitorsLoaded, setMonitorsLoaded] = useState(false);
  const [criticalAlerts, setCriticalAlerts] = useState<CriticalAlert[]>([]);
  const [monitorBusy, setMonitorBusy] = useState(false);
  const [monitorNotice, setMonitorNotice] = useState("");

  useEffect(() => {
    let active = true;
    queueMicrotask(() => {
      if (!active) return;
      try {
        const stored = JSON.parse(
          window.localStorage.getItem(MONITOR_STORAGE_KEY) || "[]",
        );
        if (Array.isArray(stored)) {
          setMonitors(
            stored
              .filter(
                (item): item is TokenMonitor =>
                  item &&
                  typeof item === "object" &&
                  (item.network === "robinhood" ||
                    item.network === "base" ||
                    item.network === "solana") &&
                  typeof item.address === "string" &&
                  typeof item.key === "string" &&
                  typeof item.cursor === "string",
              )
              .slice(0, MAX_DEVICE_MONITORS),
          );
        }
      } catch {
        window.localStorage.removeItem(MONITOR_STORAGE_KEY);
      } finally {
        if (active) setMonitorsLoaded(true);
      }
    });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!monitorsLoaded) return;
    window.localStorage.setItem(
      MONITOR_STORAGE_KEY,
      JSON.stringify(monitors),
    );
  }, [monitors, monitorsLoaded]);

  useEffect(() => {
    if (!monitorsLoaded || monitors.length === 0) return;
    let active = true;
    let inFlight = false;

    async function pollAlerts(resubscribe: boolean) {
      if (inFlight) return;
      inFlight = true;
      try {
        const nextCursors = new Map<string, string>();
        const incoming: CriticalAlert[] = [];
        for (const monitor of monitors) {
          if (resubscribe) {
            await fetch("/api/watch", {
              method: "POST",
              cache: "no-store",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                network: monitor.network,
                address: monitor.address,
              }),
            });
          }
          const query = new URLSearchParams({
            network: monitor.network,
            address: monitor.address,
          });
          if (monitor.cursor) query.set("after", monitor.cursor);
          const response = await fetch(`/api/watch?${query.toString()}`, {
            cache: "no-store",
          });
          if (!response.ok) continue;
          const body = await response.json().catch(() => null);
          if (Array.isArray(body?.alerts)) {
            incoming.push(...(body.alerts as CriticalAlert[]));
          }
          if (typeof body?.cursor === "string" && body.cursor) {
            nextCursors.set(monitor.key, body.cursor);
          }
        }
        if (!active) return;
        if (nextCursors.size) {
          setMonitors((current) => {
            let changed = false;
            const next = current.map((monitor) => {
              const cursor = nextCursors.get(monitor.key) || monitor.cursor;
              if (cursor === monitor.cursor) return monitor;
              changed = true;
              return { ...monitor, cursor };
            });
            return changed ? next : current;
          });
        }
        if (incoming.length) {
          setCriticalAlerts((current) => {
            const byHash = new Map(
              [...incoming, ...current].map((alert) => [
                alert.alert_hash,
                alert,
              ]),
            );
            return Array.from(byHash.values())
              .sort((a, b) => b.observed_at.localeCompare(a.observed_at))
              .slice(0, 20);
          });
          if (
            "Notification" in window &&
            window.Notification.permission === "granted"
          ) {
            for (const alert of incoming.slice(0, 3)) {
              new window.Notification(alert.title, {
                body: `${networkLabel(alert.network)} ${shortAddress(alert.token_address)}: ${alert.message}`,
                tag: alert.alert_hash,
              });
            }
          }
        }
      } finally {
        inFlight = false;
      }
    }

    void pollAlerts(true);
    const timer = window.setInterval(() => void pollAlerts(false), 5_000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [monitors, monitorsLoaded]);

  const liveMonitorKey = liveReport
    ? `${reportNetwork(liveReport.token.chain)}:${liveReport.token.address.toLowerCase()}`
    : "";
  const liveIsMonitored = monitors.some(
    (monitor) => monitor.key === liveMonitorKey,
  );

  async function toggleLiveMonitor() {
    if (!liveReport || monitorBusy) return;
    const activeNetwork = reportNetwork(liveReport.token.chain);
    const reportAddress = liveReport.token.address;
    const key = `${activeNetwork}:${reportAddress.toLowerCase()}`;
    setMonitorBusy(true);
    setMonitorNotice("");
    try {
      if (monitors.some((monitor) => monitor.key === key)) {
        const query = new URLSearchParams({
          network: activeNetwork,
          address: reportAddress,
        });
        const response = await fetch(`/api/watch?${query.toString()}`, {
          method: "DELETE",
          cache: "no-store",
        });
        if (!response.ok) {
          const body = await response.json().catch(() => null);
          throw new Error(errorMessage(body, "Monitoring could not be stopped."));
        }
        setMonitors((current) =>
          current.filter((monitor) => monitor.key !== key),
        );
        setMonitorNotice("Critical monitoring stopped for this token.");
        return;
      }
      if (monitors.length >= MAX_DEVICE_MONITORS) {
        throw new Error(
          `This device can monitor up to ${MAX_DEVICE_MONITORS} tokens.`,
        );
      }
      const response = await fetch("/api/watch", {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          network: activeNetwork,
          address: reportAddress,
        }),
      });
      const body = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(errorMessage(body, "Monitoring could not be started."));
      }
      if (
        "Notification" in window &&
        window.Notification.permission === "default"
      ) {
        await window.Notification.requestPermission();
      }
      const cursor =
        typeof body?.subscription?.created_at === "string"
          ? body.subscription.created_at
          : new Date().toISOString();
      setMonitors((current) => [
        ...current,
        {
          key,
          network: activeNetwork,
          address: reportAddress,
          label:
            liveReport.token.symbol ||
            liveReport.token.name ||
            reportAddress.slice(0, 10),
          cursor,
        },
      ]);
      setMonitorNotice(
        "Critical monitoring is active. Keep Chainseer open for in-page and browser notifications.",
      );
    } catch (error) {
      setMonitorNotice(
        error instanceof Error ? error.message : "Monitoring could not be updated.",
      );
    } finally {
      setMonitorBusy(false);
    }
  }

  async function submitScan(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedAddress = address.trim();

    const validAddress =
      network === "solana"
        ? SOLANA_ADDRESS_RE.test(normalizedAddress)
        : ADDRESS_RE.test(normalizedAddress);
    if (!validAddress) {
      setNotice(
        network === "solana"
          ? "Enter a valid Solana SPL mint address."
          : "Enter a valid 42-character EVM contract address.",
      );
      setScanState("failed");
      return;
    }

    setScanState("submitting");
    setScanProgress(1);
    setLiveReport(null);
    setShowExample(false);
    setNotice("Submitting the token to the serialized analysis queue…");

    try {
      const submission = await fetch("/api/analyses", {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address: normalizedAddress, network }),
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
      setScanProgress(2);
      setNotice(
        submissionBody.cached
          ? "Loading the recent sealed result…"
          : network === "solana"
            ? "Analyzing SPL mint controls, markets, holders, Jupiter routes, and provenance…"
            : network === "base"
              ? "Analyzing Base contract, liquidity, holders, deployer history, and provenance…"
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
        if (
          (job.status === "queued" || job.status === "running") &&
          typeof job.progress_percent === "number"
        ) {
          setScanProgress(Math.max(0, Math.min(100, job.progress_percent)));
          if (typeof job.stage_detail === "string" && job.stage_detail) {
            setNotice(
              `${job.stage_detail} · ${Math.round(job.progress_percent)}%`,
            );
          }
        }
        if (job.status === "succeeded" && job.result) {
          setLiveReport(job.result as PublicReport);
          setScanState("succeeded");
          setScanProgress(100);
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
      setScanProgress(0);
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
    setScanProgress(0);
    setLiveReport(null);
    document.getElementById("report")?.scrollIntoView({ behavior: "smooth" });
  }

  function selectNetwork(nextNetwork: Network) {
    if (nextNetwork === network) return;
    setNetwork(nextNetwork);
    setAddress("");
    setNotice("");
    setLiveReport(null);
    setScanState("idle");
    setScanProgress(0);
  }

  return (
    <main className={`chain-theme theme-${network}`} data-network={network}>
      <header className="site-header">
        <a className="brand" href="#top" aria-label="Chainseer home">
          <span className="brand-mark" aria-hidden="true">C</span>
          <span>CHAINSEER</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#report">Demo report</a>
          <a href="#method">Method</a>
          <a href="#faq">Q&amp;A</a>
          <a href="#memory-core">Memory Core</a>
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
          <a className="header-cta" href="#scanner">Scan token</a>
        </div>
      </header>

      <section className="hero" id="top">
        <div className="hero-glow" aria-hidden="true" />
        <div className="eyebrow">
          <span className="status-dot" />
          On-chain intelligence · Powered by Cypher Tempre Timechain
        </div>
        <h1>
          Know the token
          <br />
          <span>before it knows your wallet.</span>
        </h1>
        <p className="hero-copy">
          Chainseer turns fragmented on-chain signals into one evidence-backed
          risk decision—before you buy, bridge, or connect.
        </p>

        <form className="scanner" id="scanner" onSubmit={submitScan}>
          <fieldset className="chain-switch">
            <legend>Analysis network</legend>
            <button
              type="button"
              className={`chain-option robinhood-option ${
                network === "robinhood" ? "selected" : ""
              }`}
              aria-pressed={network === "robinhood"}
              onClick={() => selectNetwork("robinhood")}
            >
              <span className="chain-icon robinhood-icon" aria-hidden="true">◆</span>
              <span className="chain-option-copy">
                <strong>Robinhood Chain</strong>
                <small>EVM · block-pinned</small>
              </span>
              <span className="chain-live">Live</span>
            </button>
            <button
              type="button"
              className={`chain-option base-option ${
                network === "base" ? "selected" : ""
              }`}
              aria-pressed={network === "base"}
              onClick={() => selectNetwork("base")}
            >
              <span className="chain-icon base-icon" aria-hidden="true">B</span>
              <span className="chain-option-copy">
                <strong>Base</strong>
                <small>EVM · block-pinned</small>
              </span>
              <span className="chain-live">Live</span>
            </button>
            <button
              type="button"
              className={`chain-option solana-option ${
                network === "solana" ? "selected" : ""
              }`}
              aria-pressed={network === "solana"}
              onClick={() => selectNetwork("solana")}
            >
              <span className="chain-icon solana-icon" aria-hidden="true">
                <i /><i /><i />
              </span>
              <span className="chain-option-copy">
                <strong>Solana</strong>
                <small>SPL · slot-anchored</small>
              </span>
              <span className="chain-live">Live</span>
            </button>
          </fieldset>
          <div className="scanner-entry">
            <label className="sr-only" htmlFor="contract-address">
              {network === "solana" ? "SPL mint address" : "Contract address"}
            </label>
            <input
              id="contract-address"
              value={address}
              onChange={(event) => {
                setAddress(event.target.value);
                setNotice("");
              }}
              placeholder={
                network === "solana"
                  ? "Paste a Solana mint address..."
                  : "Paste a contract address 0x..."
              }
              autoComplete="off"
              spellCheck={false}
            />
            <button type="submit" disabled={scanState === "submitting" || scanState === "analyzing"}>
              {scanState === "submitting" || scanState === "analyzing"
                ? "Analysis running…"
                : "Run risk scan"}
            </button>
          </div>
        </form>
        <div className="scanner-meta">
          <span>No wallet connection</span>
          <span>No signature</span>
          <span>Evidence anchored to block or slot</span>
          <button type="button" onClick={loadExample}>Open demo report →</button>
        </div>
        {notice && <p className="scan-notice" role="status">{notice}</p>}
        {(scanState === "submitting" || scanState === "analyzing") && (
          <div
            className="scan-progress"
            role="progressbar"
            aria-label="Analysis progress"
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={Math.round(scanProgress)}
          >
            <span style={{ width: `${scanProgress}%` }} />
          </div>
        )}
      </section>

      <section className="signal-strip" aria-label="Chainseer capabilities">
        <div><strong>12</strong><span>risk dimensions</span></div>
        <div><strong>6</strong><span>hard-stop gates</span></div>
        <div><strong>Block / slot</strong><span>anchored evidence</span></div>
        <div><strong>Timechain</strong><span>tamper-evident memory</span></div>
      </section>

      {monitorNotice && (
        <p className="monitor-notice" role="status">{monitorNotice}</p>
      )}

      {criticalAlerts.length > 0 && (
        <section className="critical-alert-feed" aria-live="assertive">
          <div className="critical-alert-feed-head">
            <div>
              <span>Critical alerts</span>
              <strong>Confirmed token state changes</strong>
            </div>
            <button type="button" onClick={() => setCriticalAlerts([])}>
              Clear
            </button>
          </div>
          {criticalAlerts.map((alert) => (
            <article key={alert.alert_hash}>
              <div>
                <span>
                  {networkLabel(alert.network)} ·{" "}
                  {shortAddress(alert.token_address)} ·{" "}
                  {alert.categories.join(" · ") || "critical"}
                </span>
                <strong>{alert.title}</strong>
                <p>{alert.message}</p>
              </div>
              <small>
                {new Date(alert.observed_at).toLocaleTimeString()} ·{" "}
                {alert.anchor_type === "confirmed_slot" ? "slot" : "block"}{" "}
                {alert.anchor?.toLocaleString() || "confirmed"}
                {alert.timechain?.ring != null
                  ? ` · ring ${alert.timechain.ring}`
                  : ""}
              </small>
            </article>
          ))}
        </section>
      )}

      {liveReport && (
        <LiveReport
          report={liveReport}
          isMonitoring={liveIsMonitored}
          monitorBusy={monitorBusy}
          onToggleMonitor={toggleLiveMonitor}
        />
      )}

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
          <div className="demo-content" id="evidence">
            <LiveReport
              report={demoReport}
              isMonitoring={false}
              monitorBusy={false}
              onToggleMonitor={() => undefined}
              isDemo
            />
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
          <article><span>01</span><h3>Observe</h3><p>Collect token controls, liquidity, holder, execution, and market signals.</p></article>
          <article><span>02</span><h3>Challenge</h3><p>Run hard-stop gates and test conflicting evidence before scoring.</p></article>
          <article><span>03</span><h3>Explain</h3><p>Translate the result into an investor-friendly action and watchlist.</p></article>
          <article><span>04</span><h3>Remember</h3><p>Seal evidence and compare later outcomes without rewriting history.</p></article>
        </div>
      </section>

      <section className="faq" id="faq">
        <div className="section-heading">
          <div>
            <div className="eyebrow muted">Q&amp;A</div>
            <h2>What Chainseer actually checks, and how.</h2>
          </div>
          <p>
            A verdict is only as trustworthy as the reasoning behind it. This
            is the plain-language version of the twelve-factor engine and the
            evidence rules that decide what counts as a hard stop, a warning,
            or simply unknown.
          </p>
        </div>
        <div className="faq-list">
          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">01</span>
                <h3>How does Chainseer actually analyze a token?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                Every report is built from direct evidence, not a single
                third-party score. On Robinhood Chain and Base that means
                GoPlus Security, DexScreener, Blockscout, and direct RPC calls.
                RPC reads are pinned to a specific block; external-provider
                responses are timestamped and hash-committed when collected,
                but are not historical reads at that block. On Solana, RPC
                evidence is anchored to a confirmed slot and supplemented by
                bounded Jupiter route quotes and DexScreener market data.
              </p>
              <p>
                That evidence runs through four stages: <strong>Observe</strong>{" "}
                collects the token&rsquo;s controls, liquidity, holder, execution,
                and market signals; <strong>Challenge</strong> runs deterministic
                hard-stop gates and checks for conflicting evidence before any
                score is computed; <strong>Explain</strong> turns the result into
                an investor-readable verdict and watchlist, not just a number;
                and <strong>Remember</strong> seals the completed analysis so it
                can be checked against outcomes later without quietly rewriting
                what was actually said at the time.
              </p>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">02</span>
                <h3>What do the twelve risk dimensions measure?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                Each report scores twelve risk dimensions from 0-100,
                higher is safer, shown on every report&rsquo;s Risk dimensions
                panel:
              </p>
              <dl className="faq-factors">
                {Object.entries(factorNames).map(([key, label]) => (
                  <div key={key}>
                    <dt>{label}</dt>
                    <dd>{factorDescriptions[key]}</dd>
                  </div>
                ))}
              </dl>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">03</span>
                <h3>What&rsquo;s the difference between a hard stop, a warning, and an unknown?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                A <strong>hard stop</strong> is a deterministic, evidence-backed
                condition -- for example a confirmed honeypot, a blocked sell,
                verified creator-withdrawable primary V2 liquidity, extreme
                non-AMM holder concentration, or an unverified proxy
                implementation -- that blocks a positive verdict regardless of
                how the other dimensions score. An &ldquo;unlocked&rdquo; label by
                itself is not enough: Chainseer requires verified custody and
                withdrawal evidence before liquidity becomes a hard stop. A{" "}
                <strong>warning</strong> (a yellow flag) is material but not
                disqualifying on its own: worth weighing, not automatically
                fatal. An <strong>unknown</strong> means a piece of evidence
                genuinely couldn&rsquo;t be obtained or verified -- it is never
                treated as a passing result.
              </p>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">04</span>
                <h3>Why does Chainseer say &ldquo;unknown&rdquo; instead of assuming something is safe?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                Because a missing answer and a good answer are not the same
                thing, even though both can look reassuring at a glance. If a
                data source is unavailable or a check can&rsquo;t be completed,
                Chainseer reports that gap explicitly under Unknowns and
                limits rather than quietly defaulting to safe. A confident
                wrong answer is worse than an honest gap.
              </p>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">05</span>
                <h3>Does the analysis differ by chain?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                Robinhood Chain and Base share the same EVM analysis engine.
                Solana uses the same report schema and dimension labels, but
                the evidence method is chain-specific. Creator/deployer
                history requires the mint to resolve to an on-chain-verified
                Pump.fun or Meteora Dynamic Bonding Curve launch, and
                pool-custody verification currently covers PumpSwap, Meteora
                DAMM v2, and Meteora DLMM pools. Solana wash-trading is
                currently a lightweight DexScreener market heuristic; EVM
                analysis uses transfer-window evidence. When provenance or
                coverage cannot be established, the affected checks report
                unknown rather than borrowing confidence from a different
                chain or launch platform.
              </p>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">06</span>
                <h3>What does the score mean, and is a high score a guarantee?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                The score summarizes the twelve dimensions into one number,
                but it is not the verdict on its own -- a single hard stop caps
                the outcome regardless of score. No score, however high, is a
                guarantee: markets and on-chain state can change the moment
                after an analysis is sealed, which is exactly why every report
                is pinned to a specific block and timestamped rather than
                presented as a standing truth.
              </p>
            </div>
          </details>

          <details className="analysis-disclosure faq-item">
            <summary className="panel-head analysis-disclosure-trigger">
              <div>
                <span className="panel-index">07</span>
                <h3>Does a public risk scan execute a trade or guarantee sellability?</h3>
              </div>
            </summary>
            <div className="analysis-disclosure-body">
              <p>
                No. The public scanner does not hold a wallet key, sign a
                transaction, broadcast a transaction, or place a funded trade.
                EVM sellability uses provider evidence and read-only on-chain
                checks; Solana uses a bounded Jupiter buy-and-sell route quote.
                These checks can expose important restrictions, but they cannot
                guarantee that the same route, price, or token state will still
                exist when a user later trades.
              </p>
            </div>
          </details>
        </div>
      </section>

      <MemoryCoreDashboard />

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
        <a href="#scanner">Scan a token</a>
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
