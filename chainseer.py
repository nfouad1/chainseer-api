"""
Chainseer v7.0 -- On-Chain Intelligence Agent for Robinhood Chain

Integrates with:
  - Cypher Tempe Timechain for persistent, verifiable memory & trend analysis
  - GoPlus Security API for real token security analysis
  - DexScreener API for price, volume, liquidity, boosts, and DEX data
  - Blockscout API for deployer, holders (EOA/contract enrichment), and contract data
  - Direct RPC for DEX pair reserves, self-contained tax estimation,
    timelock verification, and 3-window wash trading detection

v7.0 provenance tracking (modeled on Mizukara's Fact/Provenance architecture):
  - Every data fetch (RPC + HTTP) is recorded in a ProvenanceLedger
  - Each fact cites its exact query spec + SHA-256 hash of the response
  - Scans are pinned to a single block number for reproducibility
  - The full report is independently verifiable end-to-end

v6.0 features:
  - 12-factor weighted scoring model (security, honeypot_safety, liquidity,
    lp_lock, holder_distribution, volume, maturity, creator_risk, wash_trading,
    deployer, sentiment, trend)
  - Wash trading 3-window stabilization (500/1000/1500 blocks)
  - Timelock contract verification (Unicrypt, TeamFinance, PinkLock, TrustSwap)
  - Historical trend analysis from Timechain rings
  - Holder enrichment (EOA vs contract, scam flags, proxy detection)
  - Deployer rug history via Blockscout flags + GoPlus cross-reference
  - DexScreener boosts as sentiment signal

Every analysis is sealed as a ring with PoQ self-evaluation.

Usage:
    py chainseer.py <token_contract_address>

Dependencies:
    pip install requests

Robinhood Chain:
    Chain ID:  4663
    RPC:       https://rpc.mainnet.chain.robinhood.com
    Explorer:  https://robinhoodchain.blockscout.com
    WETH:      0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73
"""

import importlib.util
import hashlib
import json
import math
import os
import re
import shutil
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

# ── Force UTF-8 stdout/stderr (Windows defaults to cp1252 which corrupts
#    em-dashes, smart quotes, and emojis when written to the Timechain) ─────
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, Exception):
        pass

import requests

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ZERO_ADDRESS = "0x" + "0" * 40


def ensure_utf8_runtime():
    """Restart Windows CLI entry points in Python UTF-8 mode when needed.

    Cypher Tempre persists JSON through a mix of explicitly UTF-8 and
    locale-default text operations.  Reconfiguring stdout is insufficient:
    Python's default encoding for ordinary ``open()`` calls is selected when
    the interpreter starts.  ``-X utf8`` makes all those paths agree without
    affecting callers that merely import Chainseer (including the test suite).
    """
    if os.name == "nt" and not sys.flags.utf8_mode:
        os.execv(sys.executable, [sys.executable, "-X", "utf8", *sys.argv])


class TTLCache:
    """Small bounded TTL cache for mutable HTTP responses."""

    def __init__(self, ttl_seconds: int = 30, maxsize: int = 256):
        self.ttl_seconds = ttl_seconds
        self.maxsize = maxsize
        self._values = {}
        self._lock = threading.Lock()

    def get(self, key):
        now = time.monotonic()
        with self._lock:
            item = self._values.get(key)
            if item is None:
                return None
            expires_at, value, fetched_at = item
            if expires_at <= now:
                self._values.pop(key, None)
                return None
            return value, fetched_at

    def set(self, key, value):
        with self._lock:
            if len(self._values) >= self.maxsize:
                oldest = min(self._values, key=lambda k: self._values[k][2])
                self._values.pop(oldest, None)
            fetched_at = time.time()
            self._values[key] = (time.monotonic() + self.ttl_seconds, value, fetched_at)
        return fetched_at


_HTTP_CACHE = TTLCache(
    ttl_seconds=int(os.environ.get("CHAINSEER_API_CACHE_TTL", "30")),
    maxsize=int(os.environ.get("CHAINSEER_API_CACHE_SIZE", "256")),
)


@dataclass
class ScanContext:
    """Immutable chain snapshot plus request-scoped state for one analysis."""

    chain_id: int
    block_pin: int
    ledger: "ProvenanceLedger"
    rpc_cache: dict = field(default_factory=dict)

    @property
    def block_tag(self) -> str:
        return hex(self.block_pin)


ANALYSIS_OUTPUT_SCHEMA = {
    "type": "object",
    "required": ["risk_level", "legitimacy_score", "recommendation", "evidence_fact_ids"],
    "properties": {
        "risk_level": {"type": "string", "enum": ["Low", "Medium", "High", "Critical"]},
        "legitimacy_score": {"type": "number", "minimum": 0, "maximum": 100},
        "recommendation": {"type": "string"},
        "evidence_fact_ids": {"type": "array", "items": {"type": "string", "pattern": "^F[0-9]{4}$"}},
        "uncertainties": {"type": "array", "items": {"type": "string"}},
    },
    "additionalProperties": True,
}


def validate_structured_analysis(value: dict) -> dict:
    """Dependency-free validation boundary for a future LLM output stage."""
    if not isinstance(value, dict):
        raise ValueError("structured analysis must be a JSON object")
    missing = [key for key in ANALYSIS_OUTPUT_SCHEMA["required"] if key not in value]
    if missing:
        raise ValueError(f"structured analysis missing required fields: {', '.join(missing)}")
    if value["risk_level"] not in {"Low", "Medium", "High", "Critical"}:
        raise ValueError("invalid risk_level")
    score = value["legitimacy_score"]
    if not isinstance(score, (int, float)) or isinstance(score, bool) or not 0 <= score <= 100:
        raise ValueError("legitimacy_score must be a number from 0 to 100")
    fact_ids = value["evidence_fact_ids"]
    if not isinstance(fact_ids, list) or any(not re.fullmatch(r"F[0-9]{4}", str(fid)) for fid in fact_ids):
        raise ValueError("evidence_fact_ids must contain F0000-style identifiers")
    return value

# ═══════════════════════════════════════════════════════════════════════════════
# Timechain Integration
# ═══════════════════════════════════════════════════════════════════════════════

def _load_skill_module(skill_dir: str, module_name: str):
    module_path = os.path.join(skill_dir, f"{module_name}.py")
    if skill_dir not in sys.path:
        sys.path.insert(0, skill_dir)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name} from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_timechain_module(skill_dir: str):
    return _load_skill_module(skill_dir, "timechain")


def _get_skill_dir() -> str:
    configured = os.environ.get("CHAINSEER_SKILL_DIR", "").strip()
    if configured:
        configured = os.path.abspath(os.path.expanduser(configured))
        if os.path.isfile(os.path.join(configured, "timechain.py")):
            return configured
        raise FileNotFoundError(
            "CHAINSEER_SKILL_DIR does not contain timechain.py: "
            f"{configured}"
        )
    candidates = [
        os.path.join(os.path.expanduser("~"), ".zcode", "skills", "cypher-tempre-self-model"),
        os.path.join(os.path.expanduser("~"), ".claude", "skills", "cypher-tempre-self-model"),
        os.path.join(os.path.expanduser("~"), ".codex", "skills", "cypher-tempre-self-model"),
    ]
    for c in candidates:
        if os.path.isfile(os.path.join(c, "timechain.py")):
            return c
    raise FileNotFoundError(
        "Cypher Tempe skill not found. Expected timechain.py in one of:\n"
        + "\n".join(f"  {c}" for c in candidates)
    )


COGNITIVE_LOOP_VERSION = "1.0"
_COGNITIVE_REQUIRED_MODULES = (
    "recall", "cambium", "epochs", "immune", "modality_ops",
)
_COGNITIVE_BASE_REGISTRIES = ("senses.json", "modalities.json")


def _bootstrap_faculty_registry(chain_root: str | Path, skill_dir: str) -> Path:
    """Create a private, persistent faculty registry without masking corruption.

    A completely absent registry is a legitimate first boot and is seeded from
    the pinned, read-only skill runtime. A partial registry is not repaired:
    silently replacing one missing half would erase evidence of an interrupted
    write or tampering.
    """
    root = Path(chain_root)
    registry = root / "registry"
    source = Path(skill_dir) / "registry"
    present = {name: (registry / name).is_file() for name in _COGNITIVE_BASE_REGISTRIES}
    if any(present.values()) and not all(present.values()):
        missing = ", ".join(name for name, exists in present.items() if not exists)
        raise RuntimeError(f"Faculty registry is incomplete; refusing repair: {missing}")
    if not any(present.values()):
        missing_source = [name for name in _COGNITIVE_BASE_REGISTRIES
                          if not (source / name).is_file()]
        if missing_source:
            raise RuntimeError(
                "Pinned Cypher Tempre runtime lacks faculty registries: "
                + ", ".join(missing_source)
            )
        registry.mkdir(parents=True, exist_ok=True)
        for name in _COGNITIVE_BASE_REGISTRIES:
            shutil.copy2(source / name, registry / name)
    return registry


class ChainseerCognitiveLoop:
    """Non-bypassable Timechain perception, recall, conscience, and growth."""

    def __init__(self, chain_root: str | Path, skill_dir: str):
        self.root = Path(chain_root)
        _bootstrap_faculty_registry(self.root, skill_dir)
        self.recall_module = _load_skill_module(skill_dir, "recall")
        self.cambium_module = _load_skill_module(skill_dir, "cambium")
        self.epochs_module = _load_skill_module(skill_dir, "epochs")
        self.immune_module = _load_skill_module(skill_dir, "immune")
        _load_skill_module(skill_dir, "modality_ops")
        self.recall = self.recall_module.Recall(
            self.root, registry_root=self.root
        )
        self.immune = self.immune_module.Immune(self.root)

    def seal_bootstrap_epoch(self) -> dict | None:
        return self.epochs_module.seal_epoch(
            self.root, reason="Chainseer production faculty bootstrap"
        )

    def verify_registry(self) -> None:
        ok, report = self.epochs_module.check_epoch(self.root)
        if not ok:
            raise RuntimeError(
                "Faculty registry integrity verification failed: "
                + "; ".join(report)
            )

    @staticmethod
    def _safe_input(report: dict) -> str:
        """Build a bounded cognition surface from trusted, deterministic fields.

        Raw API bodies, token names, source code, and user-authored metadata are
        deliberately excluded so external content cannot teach the self-model.
        """
        analysis = report.get("analysis") or {}
        provenance = report.get("provenance") or {}
        data = report.get("data") or {}
        source = data.get("source_code") or {}
        dex = data.get("dex_pairs") or {}
        liquidity_custody = data.get("lp_lock") or {}
        safe = {
            "task": "on-chain token risk analysis",
            "chain_id": report.get("chain_id"),
            "token_address": report.get("token_address"),
            "block_pin": provenance.get("block_pin"),
            "evidence_fact_count": provenance.get("fact_count", 0),
            "risk_level": analysis.get("risk_level"),
            "legitimacy_score": analysis.get("legitimacy_score"),
            "action_label": analysis.get("action_label"),
            "component_scores": analysis.get("component_scores") or {},
            "hard_stop_types": sorted(
                str(value)[:120]
                for value in (analysis.get("hard_stop_overrides") or [])
            ),
            "red_flag_count": len(analysis.get("red_flags") or []),
            "yellow_flag_count": len(analysis.get("yellow_flags") or []),
            "uncertain_components": sorted(
                str(key)[:80]
                for key in (analysis.get("uncertain_components") or {}).keys()
            ),
            "source_verified": bool(source.get("is_verified")),
            "market_data_available": bool(dex.get("primary_price_usd")),
            "liquidity_usd": dex.get("total_liquidity_usd"),
            "primary_amm_version": dex.get("primary_amm_version"),
            "liquidity_custody_state": liquidity_custody.get("state"),
        }
        return json.dumps(safe, sort_keys=True, separators=(",", ":"), default=str)

    @staticmethod
    def _public_labels(labels: dict) -> dict:
        computed = labels.get("computed") or {}
        return {
            "senses": [
                {"id": item.get("id"), "name": item.get("name")}
                for item in (labels.get("senses") or [])
            ],
            "modalities": [
                {"id": item.get("id"), "name": item.get("name")}
                for item in (labels.get("modalities") or [])
            ],
            "salience": labels.get("salience"),
            "dissonance": labels.get("dissonance"),
            "frames": list(labels.get("frames") or []),
            "computed": computed,
            "retrieved_faculties": list(labels.get("retrieved") or []),
        }

    @staticmethod
    def _contains_injection_signal(computed: dict) -> bool:
        for name in (
            "Value-Breach and Injection Detection",
            "Embedded-Intent Sensing",
        ):
            result = computed.get(name) or {}
            injection = result.get("injection") or {}
            if injection.get("count", 0) or result.get("override"):
                return True
        return False

    def prepare(self, report: dict) -> dict:
        self.verify_registry()
        cognitive_input = self._safe_input(report)
        screened = self.immune.screen(cognitive_input)
        if screened.get("blocked"):
            raise RuntimeError(
                "Cognitive intake refused by the Timechain covenant membrane"
            )
        recalled = self.recall.retrieve(
            cognitive_input,
            context="Chainseer risk analysis; deterministic evidence remains authoritative.",
            budget_tokens=650,
            max_blocks=5,
            neighbors=0,
            use_index=True,
        )
        labels = recalled.get("query_labels") or self.recall.label(cognitive_input)
        computed = labels.get("computed") or {}
        if self._contains_injection_signal(computed):
            raise RuntimeError(
                "Cognitive faculty detected an unsafe instruction signal"
            )
        relevant = [
            block.get("index") for block in (recalled.get("blocks") or [])
            if isinstance(block.get("index"), int)
        ]
        cognition = {
            "version": COGNITIVE_LOOP_VERSION,
            "status": "prepared",
            "input_policy": "trusted_structured_fields_only",
            **self._public_labels(labels),
            "relevant_rings": relevant,
            "immune": {
                "status": "admitted",
                "covenant": screened.get("covenant"),
            },
            "growth": [],
        }
        if not cognition["senses"] and not cognition["modalities"]:
            raise RuntimeError("Cognitive loop produced no active faculties")
        report["cognition"] = cognition
        report["_cognitive_input"] = cognitive_input
        return cognition

    def finalize(self, report: dict, analysis_ring: dict) -> dict:
        cognition = report.get("cognition") or {}
        if cognition.get("status") != "prepared":
            raise RuntimeError("Cognitive loop was not prepared before sealing")
        guard = self.immune_module.guard_turn(
            self.root,
            analysis_ring["index"],
            input_text=report.pop("_cognitive_input", ""),
            lesson="Chainseer production analysis covenant guard",
        )
        if guard.get("action") not in {None, "none", "clean"}:
            raise RuntimeError(
                "Cognitive post-seal guard rejected the analysis: "
                + str(guard.get("action"))
            )

        salience = cognition.get("salience")
        prior_salience = os.environ.get("CT_TURN_SALIENCE")
        if isinstance(salience, (int, float)):
            os.environ["CT_TURN_SALIENCE"] = str(int(salience))
        try:
            growth = self.cambium_module.fill_gap(
                self.root,
                self._safe_input(report),
                context="Chainseer production token-risk capability gap",
                both=True,
                registry_root=self.root,
            )
        finally:
            if prior_salience is None:
                os.environ.pop("CT_TURN_SALIENCE", None)
            else:
                os.environ["CT_TURN_SALIENCE"] = prior_salience

        cognition["growth"] = [
            {
                "action": item.get("action"),
                "faculty": (item.get("faculty") or {}).get("name"),
                "kind": (item.get("faculty") or {}).get("kind"),
                "reason": item.get("reason"),
            }
            for item in (growth or [])
        ]
        if any(item.get("action") in {"born", "promoted", "woken"}
               for item in (growth or [])):
            self.epochs_module.seal_epoch(
                self.root, reason=f"faculty change after analysis ring {analysis_ring['index']}"
            )
        self.verify_registry()
        cognition["status"] = "complete"
        cognition["analysis_ring"] = analysis_ring["index"]
        completion = self.recall.tc.seal(
            "cognitive_completion",
            {
                "summary": (
                    "Chainseer cognitive loop completed for token analysis "
                    f"ring {analysis_ring['index']}"
                ),
                "frame": "assertion",
                "analysis_ring": analysis_ring["index"],
                "analysis_ring_hash": analysis_ring["ring_hash"],
                "cognitive_loop": cognition,
            },
            poq={
                "coherence": 235,
                "relevance": 245,
                "novelty": 220,
                "consistency": 240,
                "depth": 230,
                "covenant": 250,
            },
        )
        completion_guard = self.immune_module.guard_turn(
            self.root,
            completion["index"],
            input_text="Complete an evidence-bound Chainseer cognitive audit.",
            lesson="Chainseer cognitive completion covenant guard",
        )
        if completion_guard.get("action") not in {None, "none", "clean"}:
            raise RuntimeError(
                "Cognitive completion guard rejected the analysis: "
                + str(completion_guard.get("action"))
            )
        ok, verify_report = self.recall.tc.verify()
        if not ok:
            raise RuntimeError(
                "Timechain failed after cognitive finalization: "
                + "; ".join(verify_report)
            )
        report["cognitive_ring"] = completion["index"]
        report["cognitive_ring_hash"] = completion["ring_hash"]
        return cognition


# ═══════════════════════════════════════════════════════════════════════════════
# Robinhood Chain Constants
# ═══════════════════════════════════════════════════════════════════════════════

CHAIN_ID = 4663
RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
EXPLORER = "https://robinhoodchain.blockscout.com"
EXPLORER_TOKEN = f"{EXPLORER}/token/"
DEXSCREENER_BASE = "https://api.dexscreener.com/latest/dex"
DEXSCREENER_CHAIN_ID = "robinhood"
GOPLUS_BASE = "https://api.gopluslabs.io/api/v1"
BLOCKSCOUT_BASE = "https://robinhoodchain.blockscout.com/api/v2"
WETH_ADDRESS = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"

# Official Uniswap deployments on Robinhood Chain (source: Uniswap SDK addresses.ts)
UNISWAP_V2_FACTORY = "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f"
UNISWAP_V2_ROUTER02 = "0x89e5db8b5aa49aa85ac63f691524311aeb649eba"
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
UNISWAP_V3_SWAP_ROUTER02 = "0xcaf681a66d020601342297493863e78c959e5cb2"
UNISWAP_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"

# ERC-20 ABI selectors
ABI_BALANCE_OF    = "0x70a08231"
ABI_TOTAL_SUPPLY  = "0x18160ddd"
ABI_DECIMALS      = "0x313ce567"
ABI_SYMBOL        = "0x95d89b41"
ABI_NAME          = "0x06fdde03"
ABI_OWNER         = "0x8da5cb5b"
ABI_GET_RESERVES  = "0x0902f1ac"  # Uniswap V2 pair getReserves()
ABI_TOKEN0        = "0x0dfe1681"  # Uniswap V2 pair token0()
ABI_TOKEN1        = "0xd21220a7"  # Uniswap V2 pair token1()

# ERC-20 transfer event topic
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Timelock/Locker contract function selectors (verified via 4byte.directory)
TIMELOCK_SELECTORS = {
    "unicrypt_getReleaseTime": "52db191f",      # getReleaseTime(uint256)
    "unicrypt_token":        "c3828bf0",       # lpTokenAddress()
    "teamfinance_releaseDate": "69bb28cd",      # releaseDate(uint256)
    "pinklock_unlockDate":   "b50c0410",        # unlockDate(uint256)
    "trustswap_releaseTime":  "5aeca126",        # releaseTime(uint256)
}

# Known locker platform names by selector
TIMELOCK_PLATFORM_NAMES = {
    "unicrypt_getReleaseTime": "Unicrypt",
    "unicrypt_token":         "Unicrypt",
    "teamfinance_releaseDate": "Team Finance",
    "pinklock_unlockDate":   "PinkLock",
    "trustswap_releaseTime": "TrustSwap",
}

# Known LP lock contracts on Robinhood Chain (verified + common patterns)
KNOWN_LOCKERS = {
    "0x000000000000000000000000000000000000dead": "Burned (dead address)",
    "0x0000000000000000000000000000000000000001": "Burned (null address)",
}

# Bytecode honeypot selectors (fallback if GoPlus has no data)
HONEYPOT_SELECTORS = {
    "blacklist":     "0xabfe3276",
    "setFee":        "0x8dbdbe1d",
    "setTax":        "0x1e3dd539",
    "setMaxTx":      "0x45e4cd25",
    "setCooldown":   "0xe2cb3297",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Provenance Ledger — every claim cites its exact query + response hash
# (Modeled on Mizukara's Fact/Provenance architecture, sealed in ring 33/34)
# ═══════════════════════════════════════════════════════════════════════════════

class ProvenanceLedger:
    """Records every data fetch with its query spec and response hash.

    Every number in the Chainseer report can be traced back to:
      (1) which query produced it (RPC method + params, or HTTP endpoint + params)
      (2) the SHA-256 hash of the raw response
      (3) the block number the scan was pinned to

    This makes every scan independently verifiable: two scans at the same
    block produce byte-for-byte identical provenance records.
    """

    def __init__(self, evidence_dir: str | Path = None):
        self._facts = []
        self._block_pin = None  # set once at scan start
        self._dedup = {}
        self.evidence_dir = Path(evidence_dir) if evidence_dir else None
        if self.evidence_dir:
            self.evidence_dir.mkdir(parents=True, exist_ok=True)

    @property
    def block_pin(self):
        return self._block_pin

    @block_pin.setter
    def block_pin(self, value):
        if self._block_pin is None:
            self._block_pin = value

    def checkpoint(self) -> int:
        return len(self._facts)

    def fact_ids_since(self, checkpoint: int) -> list:
        return [f["fact_id"] for f in self._facts[checkpoint:]]

    def record(self, source: str, query: dict, response_raw, *, cache_hit: bool = False,
               fetched_at: float = None) -> str:
        """Record a fetch and return its fact_id (for citation).

        Args:
            source: 'rpc' or 'http'
            query: dict describing the query — for RPC: {method, params};
                   for HTTP: {url, params}
            response_raw: the raw response string (JSON or hex) to hash

        Returns:
            fact_id like 'F0007' for citation in analysis results
        """
        canonical = json.dumps(response_raw, sort_keys=True, default=str, separators=(",", ":"))
        resp_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        query_hash = hashlib.sha256(
            json.dumps({"source": source, "query": query}, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        dedup_key = (query_hash, resp_hash)
        if dedup_key in self._dedup:
            return self._dedup[dedup_key]

        evidence_path = None
        if self.evidence_dir:
            evidence_file = self.evidence_dir / f"{resp_hash}.json"
            if not evidence_file.exists():
                temp_file = evidence_file.with_suffix(
                    f".{os.getpid()}.{threading.get_ident()}.tmp"
                )
                temp_file.write_text(canonical, encoding="utf-8")
                os.replace(temp_file, evidence_file)
            evidence_path = str(evidence_file)
        fact_id = f"F{len(self._facts):04d}"
        self._facts.append({
            "fact_id": fact_id,
            "source": source,
            "query": query,
            "response_hash": resp_hash,
            "response_hash_short": resp_hash[:16],
            "block": self._block_pin,
            "query_hash": query_hash,
            "evidence_path": evidence_path,
            "cache_hit": cache_hit,
            "fetched_at": datetime.fromtimestamp(fetched_at or time.time(), tz=timezone.utc).isoformat(),
        })
        self._dedup[dedup_key] = fact_id
        return fact_id

    def absorb(self, other: "ProvenanceLedger") -> list[str]:
        """Merge an isolated ledger while assigning deterministic local IDs."""
        snapshot = other.to_dict()
        other_block = snapshot.get("block_pin")
        if (
            self._block_pin is not None
            and other_block is not None
            and int(other_block) != int(self._block_pin)
        ):
            raise ValueError("cannot absorb provenance from another block pin")
        added = []
        for original in snapshot.get("facts") or []:
            dedup_key = (
                original.get("query_hash"),
                original.get("response_hash"),
            )
            if dedup_key in self._dedup:
                added.append(self._dedup[dedup_key])
                continue
            evidence_path = original.get("evidence_path")
            if evidence_path:
                path = Path(evidence_path)
                if (
                    not path.is_file()
                    or hashlib.sha256(path.read_bytes()).hexdigest()
                    != original.get("response_hash")
                ):
                    raise ValueError("isolated provenance evidence failed hash verification")
            fact_id = f"F{len(self._facts):04d}"
            fact = dict(original)
            fact["fact_id"] = fact_id
            fact["block"] = self._block_pin
            self._facts.append(fact)
            self._dedup[dedup_key] = fact_id
            added.append(fact_id)
        return added

    def to_dict(self) -> dict:
        return {
            "block_pin": self._block_pin,
            "fact_count": len(self._facts),
            "facts": list(self._facts),
        }

    def verify(self) -> bool:
        """Verify IDs and any locally retained content-addressed evidence."""
        seen = set()
        for i, f in enumerate(self._facts):
            expected_id = f"F{i:04d}"
            if f["fact_id"] != expected_id:
                return False
            if f["fact_id"] in seen:
                return False
            evidence_path = f.get("evidence_path")
            if evidence_path:
                path = Path(evidence_path)
                if not path.is_file():
                    return False
                if hashlib.sha256(path.read_bytes()).hexdigest() != f["response_hash"]:
                    return False
            seen.add(f["fact_id"])
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# RPC Client
# ═══════════════════════════════════════════════════════════════════════════════

class RobinhoodRPC:
    """Low-level JSON-RPC client for Robinhood Chain.

    When a ProvenanceLedger is attached, every call is recorded with its
    query spec and response hash.
    """

    def __init__(self, rpc_url: str = RPC_URL, timeout: int = 30,
                 ledger: "ProvenanceLedger" = None):
        self.rpc_url = rpc_url
        self.timeout = timeout
        self.ledger = ledger
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._req_id = 0
        self.context = None

    def bind_context(self, context: ScanContext):
        self.context = context
        self.ledger = context.ledger

    def _block_tag(self, explicit=None) -> str:
        if explicit is not None:
            return hex(explicit) if isinstance(explicit, int) else explicit
        return self.context.block_tag if self.context is not None else "latest"

    def _call(self, method: str, params: list = None) -> dict:
        call_params = params or []
        cache_key = (method, json.dumps(call_params, sort_keys=True, default=str))
        if self.context is not None and cache_key in self.context.rpc_cache:
            data = self.context.rpc_cache[cache_key]
            if self.ledger is not None:
                self.ledger.record("rpc", {"method": method, "params": call_params}, data,
                                   cache_hit=True)
            return data.get("result")

        self._req_id += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": call_params, "id": self._req_id}
        try:
            resp = self._session.post(self.rpc_url, json=payload, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            if self.context is not None:
                self.context.rpc_cache[cache_key] = data
            # Record provenance BEFORE error-checking (the response is the evidence)
            if self.ledger is not None:
                self.ledger.record(
                    source="rpc",
                    query={"method": method, "params": call_params},
                    response_raw=data,
                )
            if "error" in data:
                raise RPCError(data["error"].get("message", "Unknown RPC error"),
                                data["error"].get("code", -1))
            return data.get("result")
        except requests.exceptions.ConnectionError:
            raise RPCError(f"Cannot connect to {self.rpc_url}", -1)
        except requests.exceptions.Timeout:
            raise RPCError(f"RPC request timed out after {self.timeout}s", -2)

    def get_block_number(self) -> int:
        result = self._call("eth_blockNumber")
        return int(result, 16) if result else 0

    def get_balance(self, address: str, block=None) -> int:
        result = self._call("eth_getBalance", [address, self._block_tag(block)])
        return int(result, 16) if result else 0

    def get_code(self, address: str, block=None) -> str:
        return self._call("eth_getCode", [address, self._block_tag(block)])

    def get_transaction_count(self, address: str, block=None) -> int:
        result = self._call("eth_getTransactionCount", [address, self._block_tag(block)])
        return int(result, 16) if result else 0

    def get_logs(self, from_block: int, to_block: int,
                 address: str = None, topics: list = None) -> list:
        params = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address:
            params["address"] = address
        if topics:
            params["topics"] = topics
        return self._call("eth_getLogs", [params])

    def call(self, to_address: str, data: str, block=None) -> str:
        return self._call("eth_call", [{"to": to_address, "data": data}, self._block_tag(block)])

    def erc20_name(self, token: str) -> str:
        return _decode_string(self.call(token, ABI_NAME + "0" * 56))

    def erc20_symbol(self, token: str) -> str:
        return _decode_string(self.call(token, ABI_SYMBOL + "0" * 56))

    def erc20_decimals(self, token: str) -> int:
        raw = self.call(token, ABI_DECIMALS + "0" * 56)
        return int(raw, 16) if raw else 18

    def erc20_total_supply(self, token: str) -> int:
        raw = self.call(token, ABI_TOTAL_SUPPLY + "0" * 56)
        return int(raw, 16) if raw else 0

    def erc20_owner(self, token: str) -> str:
        return _decode_address(self.call(token, ABI_OWNER + "0" * 56))

    def erc20_balance_of(self, token: str, address: str) -> int:
        """Read ERC-20 balanceOf(address). Returns raw integer."""
        padded_addr = address.lower().replace("0x", "").zfill(64)
        raw = self.call(token, ABI_BALANCE_OF + padded_addr)
        return int(raw, 16) if raw else 0

    def pair_get_reserves(self, pair_address: str) -> dict:
        """Read Uniswap V2 pair reserves. Returns (reserve0, reserve1, blockTimestampLast)."""
        raw = self.call(pair_address, ABI_GET_RESERVES + "0" * 56)
        if not raw or raw == "0x" or len(raw) < 194:
            return {"reserve0": 0, "reserve1": 0, "block_timestamp_last": 0}
        raw_hex = raw.replace("0x", "")
        r0 = int(raw_hex[0:64], 16)
        r1 = int(raw_hex[64:128], 16)
        r2 = int(raw_hex[128:192], 16)
        return {"reserve0": r0, "reserve1": r1, "block_timestamp_last": r2}

    def pair_token0(self, pair_address: str) -> str:
        return _decode_address(self.call(pair_address, ABI_TOKEN0 + "0" * 56))

    def pair_token1(self, pair_address: str) -> str:
        return _decode_address(self.call(pair_address, ABI_TOKEN1 + "0" * 56))

    def factory_get_pair(self, token_a: str, token_b: str) -> str:
        """Find V2 pair via the factory. Returns zero address if none."""
        # getPair(address,address) selector: 0xe6a43905
        padded_a = token_a.lower().replace("0x", "").zfill(64)
        padded_b = token_b.lower().replace("0x", "").zfill(64)
        data = "0xe6a43905" + padded_a + padded_b
        result = self.call(UNISWAP_V2_FACTORY, data)
        if not result or result == "0x" or len(result) < 66:
            return "0x" + "0" * 40
        return "0x" + result[26:]


class RPCError(Exception):
    def __init__(self, message: str, code: int):
        self.code = code
        super().__init__(f"[RPC {code}] {message}")


# ═══════════════════════════════════════════════════════════════════════════════
# Helper decoders
# ═══════════════════════════════════════════════════════════════════════════════

def _decode_string(hex_data: str) -> str:
    if not hex_data or hex_data == "0x":
        return "Unknown"
    try:
        hex_data = hex_data.replace("0x", "")
        if len(hex_data) < 128:
            return "Unknown"
        length = int(hex_data[64:128], 16)
        if length == 0 or length > 1024:
            return "Unknown"
        start = 128
        end = start + length * 2
        if end > len(hex_data):
            return "Unknown"
        return bytes.fromhex(hex_data[start:end]).decode("utf-8", errors="replace").strip("\x00")
    except Exception:
        return "Unknown"


def _decode_address(hex_data: str) -> str:
    if not hex_data or hex_data == "0x":
        return "0x" + "0" * 40
    try:
        hex_data = hex_data.replace("0x", "")
        return "0x" + hex_data[24:64]
    except Exception:
        return "0x" + "0" * 40


def _safe_float(val, default=0.0) -> float:
    """Convert a value to float safely."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    """Convert a value to int safely."""
    if val is None:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError, OverflowError):
        return default


HOLDER_AGE_COHORTS = (
    (1.0, 100, "<1 day"),
    (3.0, 250, "1-3 days"),
    (7.0, 450, "3-7 days"),
    (14.0, 750, "7-14 days"),
    (30.0, 1250, "14-30 days"),
    (90.0, 2500, "30-90 days"),
    (float("inf"), 5000, "90+ days"),
)


def _holder_base_score(holder_count: int, age_days) -> dict:
    """Score holder adoption against an age cohort without rewarding tiny bases.

    The old linear curve treated every token as mature and required 5,000
    holders for a high score. This curve compares adoption with a transparent
    age-band target, while absolute caps keep a very young token with a handful
    of sybil wallets from receiving a strong score. Unknown age deliberately
    retains the mature 5,000-holder target.
    """
    holder_count = max(0, _safe_int(holder_count))
    parsed_age = None
    if age_days is not None:
        try:
            candidate_age = float(age_days)
            if math.isfinite(candidate_age) and candidate_age >= 0:
                parsed_age = candidate_age
        except (TypeError, ValueError, OverflowError):
            pass

    if parsed_age is None:
        cohort_label = "unknown age (conservative)"
        target = 5000
    else:
        _, target, cohort_label = next(
            cohort for cohort in HOLDER_AGE_COHORTS
            if parsed_age < cohort[0]
        )

    ratio = holder_count / target if target else 0.0
    score = 20.0 + 60.0 * math.sqrt(min(ratio, 1.0))
    score += 15.0 * min(max(ratio - 1.0, 0.0), 1.0)

    # Absolute adoption floors prevent age normalization from rewarding a
    # launch whose apparent holder count is still trivial or easily sybilable.
    if holder_count < 10:
        score = min(score, 25.0)
    elif holder_count < 25:
        score = min(score, 40.0)
    elif holder_count < 50:
        score = min(score, 55.0)
    elif holder_count < 100:
        score = min(score, 65.0)

    return {
        "score": round(max(0.0, min(95.0, score)), 2),
        "holder_count": holder_count,
        "age_days": parsed_age,
        "cohort": cohort_label,
        "target_holders": target,
        "cohort_ratio": round(ratio, 3),
        "method": "age_cohort_v1",
    }


def _http_get_json(url: str, *, params: dict = None, timeout: int = 15,
                   ledger: ProvenanceLedger = None):
    """Fetch JSON through the shared bounded TTL cache and preserve provenance."""
    query = {"url": url}
    if params:
        query["params"] = params
    key = (url, json.dumps(params or {}, sort_keys=True, default=str))
    cached = _HTTP_CACHE.get(key)
    if cached is not None:
        data, fetched_at = cached
        fact_id = ledger.record("http", query, data, cache_hit=True,
                                fetched_at=fetched_at) if ledger else None
        return data, fact_id, True

    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    fetched_at = _HTTP_CACHE.set(key, data)
    fact_id = ledger.record("http", query, data, fetched_at=fetched_at) if ledger else None
    return data, fact_id, False
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════════════════════════════════
# External API Clients
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_goplus_security(token_address: str, ledger: "ProvenanceLedger" = None) -> dict:
    """Fetch token security data from GoPlus API.

    GoPlus provides real buy/sell tax, honeypot detection, holder data,
    LP info, creator address, and 30+ security checks.
    """
    try:
        data, _, _ = _http_get_json(
            f"{GOPLUS_BASE}/token_security/{CHAIN_ID}",
            params={"contract_addresses": token_address}, ledger=ledger)
        if data.get("code") != 1:
            return {}
        result = data.get("result", {})
        key = token_address.lower()
        return result.get(key, {})
    except Exception:
        return {}


def _fetch_dexscreener_token(token_address: str, ledger: "ProvenanceLedger" = None) -> dict:
    """Fetch token market data from DexScreener API.

    Returns: price, volume, liquidity, txns, pairs, market cap, socials, etc.
    """
    try:
        data, _, _ = _http_get_json(
            f"{DEXSCREENER_BASE}/tokens/{token_address}", ledger=ledger)
        pairs = data.get("pairs", [])
        return {"pairs": pairs, "pair_count": len(pairs)}
    except Exception:
        return {"pairs": [], "pair_count": 0}


LIQUIDITY_CUSTODY_STATES = {
    "locked",
    "protocol_secured",
    "creator_withdrawable",
    "custody_unverified",
    "not_applicable",
}


def _amm_version(pair: dict) -> str:
    """Identify an AMM version without assuming every pair is Uniswap V2."""
    labels = {str(label).lower() for label in pair.get("labels", [])}
    for version in ("v4", "v3", "v2"):
        if version in labels:
            return version

    pair_id = str(pair.get("pairAddress") or "")
    if re.fullmatch(r"0x[a-fA-F0-9]{64}", pair_id):
        return "v4"
    return "unknown"


def _pick_primary_pair(dexscreener_data: dict) -> dict:
    """Select the highest-liquidity market, independent of AMM version."""
    pairs = dexscreener_data.get("pairs", [])
    if not pairs:
        return {}

    return max(
        pairs,
        key=lambda p: (
            _safe_float(p.get("liquidity", {}).get("usd")),
            p.get("quoteToken", {}).get("address", "").lower()
            == WETH_ADDRESS.lower(),
        ),
    )


def _fetch_blockscout_address(token_address: str, ledger: "ProvenanceLedger" = None) -> dict:
    """Fetch address info from Blockscout API: creator, creation tx, verification."""
    try:
        data, _, _ = _http_get_json(
            f"{BLOCKSCOUT_BASE}/addresses/{token_address}", ledger=ledger)
        return {
            "creator_address": data.get("creator_address_hash", ""),
            "creation_tx_hash": data.get("creation_transaction_hash", ""),
            "is_contract": data.get("is_contract", False),
            "is_verified": data.get("is_verified", False),
            "is_scam": data.get("is_scam", False),
            "has_token_transfers": data.get("has_token_transfers", False),
            "coin_balance": data.get("coin_balance", "0"),
        }
    except Exception:
        return {}


def _fetch_blockscout_token(token_address: str, ledger: "ProvenanceLedger" = None) -> dict:
    """Fetch token info from Blockscout: holders count, price, volume."""
    try:
        data, _, _ = _http_get_json(
            f"{BLOCKSCOUT_BASE}/tokens/{token_address}", ledger=ledger)
        return {
            "holders_count": data.get("holders_count", 0),
            "exchange_rate": data.get("exchange_rate"),
            "volume_24h": data.get("volume_24h"),
            "circulating_market_cap": data.get("circulating_market_cap"),
            "total_supply": data.get("total_supply"),
        }
    except Exception:
        return {}


def _fetch_blockscout_source(token_address: str,
                             ledger: "ProvenanceLedger" = None) -> dict:
    """Fetch verified source metadata, including proxy implementation details."""
    try:
        data, fact_id, cache_hit = _http_get_json(
            f"{BLOCKSCOUT_BASE}/smart-contracts/{token_address}", ledger=ledger)
        implementations = data.get("implementations") or []
        return {
            "available": True,
            "is_verified": bool(data.get("is_verified") or data.get("source_code")),
            "name": data.get("name"),
            "language": data.get("language"),
            "compiler_version": data.get("compiler_version"),
            "optimization_enabled": data.get("optimization_enabled"),
            "proxy_type": data.get("proxy_type"),
            "implementations": implementations,
            "implementation_verified": all(
                bool(item.get("is_verified", True)) for item in implementations
            ) if implementations else None,
            "source_code_hash": hashlib.sha256(
                str(data.get("source_code", "")).encode("utf-8")
            ).hexdigest() if data.get("source_code") else None,
            "fact_id": fact_id,
            "cache_hit": cache_hit,
        }
    except Exception as exc:
        return {"available": False, "is_verified": False, "error": str(exc)}


def _fetch_blockscout_holders(token_address: str, limit: int = 20,
                              ledger: "ProvenanceLedger" = None) -> list:
    """Fetch top token holders from Blockscout API."""
    try:
        data, _, _ = _http_get_json(
            f"{BLOCKSCOUT_BASE}/tokens/{token_address}/holders", ledger=ledger)
        items = data if isinstance(data, list) else data.get("items", [])
        holders = []
        for h in items[:limit]:
            addr_info = h.get("address", {})
            holders.append({
                "address": addr_info.get("hash", ""),
                "is_contract": addr_info.get("is_contract", False),
                "balance_raw": h.get("value", "0"),
                "name": addr_info.get("name"),
                "is_verified": addr_info.get("is_verified", False),
                # v6.0 enrichment fields
                "address_info": {
                    "hash": addr_info.get("hash", ""),
                    "is_contract": addr_info.get("is_contract", False),
                    "is_scam": addr_info.get("is_scam", False),
                    "proxy_type": addr_info.get("proxy_type"),
                    "reputation": addr_info.get("reputation", ""),
                    "public_tags": addr_info.get("public_tags", []),
                    "name": addr_info.get("name"),
                },
            })
        return holders
    except Exception:
        return []


def _fetch_serial_deployer(deployer_address: str, ledger: "ProvenanceLedger" = None) -> dict:
    """Check if deployer created other tokens and whether deployer is flagged."""
    result = {
        "deployer": deployer_address,
        "other_tokens": [],
        "is_serial_deployer": False,
        "rug_history": 0,
        "deployer_is_scam": False,
        "deployer_reputation": "unknown",
        "deployer_public_tags": [],
    }
    if not deployer_address or deployer_address == "0x" + "0" * 40:
        return result

    # Check the deployer's own Blockscout address for flags
    deployer_info = _fetch_blockscout_address(deployer_address, ledger=ledger)
    result["deployer_is_scam"] = deployer_info.get("is_scam", False)
    result["deployer_reputation"] = deployer_info.get("reputation", "unknown")
    result["deployer_public_tags"] = deployer_info.get("public_tags", [])

    try:
        # Use the 'created-contracts' endpoint for accurate count
        data, _, _ = _http_get_json(
            f"{BLOCKSCOUT_BASE}/addresses/{deployer_address}/created-contracts",
            ledger=ledger)
        items = data if isinstance(data, list) else data.get("items", [])
        result["total_creations"] = len(items)
        result["is_serial_deployer"] = len(items) > 5
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Analysis Engine
# ═══════════════════════════════════════════════════════════════════════════════

class Chainseer:
    """On-chain intelligence agent for Robinhood Chain."""

    def __init__(self, rpc_url: str = RPC_URL, chain_root: str = None):
        self.rpc_url = rpc_url
        # Provenance ledger — created per-analysis in analyze_token()
        self.ledger = None
        self.rpc = RobinhoodRPC(rpc_url)
        self.chain_id = CHAIN_ID
        self.explorer = EXPLORER

        # Initialize Timechain
        skill_dir = _get_skill_dir()
        tc_module = _load_timechain_module(skill_dir)
        self.poq_module = _load_skill_module(skill_dir, "poq")

        self.chain_root = chain_root or os.path.join(os.path.dirname(__file__), "chainseer_chain")
        _bootstrap_faculty_registry(self.chain_root, skill_dir)
        self.tc = tc_module.Timechain(root=self.chain_root)

        if self.tc.height() == 0:
            self.tc.genesis(name="Chainseer")
            print(f"  New Timechain initialized at {self.chain_root}")
        self.cognitive_loop = ChainseerCognitiveLoop(self.chain_root, skill_dir)
        self.cognitive_loop.seal_bootstrap_epoch()
        ok, verify_report = self.tc.verify()
        if not ok:
            raise RuntimeError(f"Timechain integrity verification failed: {verify_report}")
        self.cognitive_loop.verify_registry()

        print(f" Chainseer v7.0 -- Robinhood Chain On-Chain Intelligence")
        print(f" Chain ID: {self.chain_id} | RPC: {self.rpc_url}")
        print(f" Timechain root: {self.chain_root}")
        print(f" Latest block: {self.rpc.get_block_number():,}")
        print()

    # ── MAIN ENTRY POINT ───────────────────────────────────────────────────

    def analyze_token(self, token_address: str, full_report: bool = False) -> dict:
        """Full token analysis with GoPlus + DexScreener + on-chain RPC.

        Every fetch is recorded in the provenance ledger so the entire
        report can be independently verified: each claim cites its exact
        query and the SHA-256 hash of the response.

        Args:
            token_address: the token contract to analyze
            full_report: if True, print the detailed 12-factor report;
                         if False (default), print the investor summary
        """
        token = token_address.strip()
        if not ADDRESS_RE.fullmatch(token):
            return {"error": "Invalid token address", "address": token}
        print(f" TOKEN ANALYSIS: {token}")
        print(f" Explorer: {EXPLORER_TOKEN}{token}")
        print(f" Time: {datetime.now(timezone.utc).isoformat()}")
        print()

        # ── Initialize provenance ledger + pin to current block ─────────────
        self.rpc.ledger = None
        self.rpc.context = None
        pin_block = self.rpc.get_block_number()
        self.ledger = ProvenanceLedger(Path(self.chain_root) / "evidence")
        self.ledger.block_pin = pin_block
        self.scan_context = ScanContext(self.chain_id, pin_block, self.ledger)
        self.rpc.bind_context(self.scan_context)
        claim_evidence = {}
        print(f" Scan pinned to block {pin_block:,} (provenance tracking active)")
        print()

        # Validate contract
        code = self.rpc.get_code(token)
        if not code or code == "0x" or len(code) < 10:
            print(" ERROR: No contract bytecode at this address.")
            return {"error": "Not a contract", "address": token}

        # ── Phase 1: Gather data ────────────────────────────────────────────
        print(" Phase 1/8: External APIs (GoPlus + DexScreener + Blockscout)...")
        checkpoint = self.ledger.checkpoint()
        data = {}
        data["goplus_security"] = _fetch_goplus_security(token, ledger=self.ledger)
        data["dexscreener"] = _fetch_dexscreener_token(token, ledger=self.ledger)
        data["blockscout_address"] = _fetch_blockscout_address(token, ledger=self.ledger)
        data["blockscout_token"] = _fetch_blockscout_token(token, ledger=self.ledger)
        data["source_code"] = _fetch_blockscout_source(token, ledger=self.ledger)
        claim_evidence["external_apis"] = self.ledger.fact_ids_since(checkpoint)

        print(" Phase 2/8: On-chain reads...")
        checkpoint = self.ledger.checkpoint()
        data["basic_info"] = self._fetch_basic_info(token)
        data["contract_audit"] = self._analyze_contract(token)
        claim_evidence["contract"] = self.ledger.fact_ids_since(checkpoint)

        print(" Phase 3/8: DEX pair reserves...")
        checkpoint = self.ledger.checkpoint()
        data["dex_pairs"] = self._analyze_dex_pairs(token, data)
        claim_evidence["dex_pairs"] = self.ledger.fact_ids_since(checkpoint)

        print(" Phase 4/8: Liquidity custody verification...")
        checkpoint = self.ledger.checkpoint()
        data["lp_lock"] = self._verify_lp_lock(
            token,
            data["dex_pairs"],
            creator_address=data["blockscout_address"].get("creator_address"),
        )
        claim_evidence["lp_lock"] = self.ledger.fact_ids_since(checkpoint)

        print(" Phase 5/8: Tax estimation...")
        data["tax_estimate"] = self._estimate_tax_from_reserves(token, data["dex_pairs"])

        print(" Phase 6/8: Deployer + holders + wash trading...")
        checkpoint = self.ledger.checkpoint()
        latest_block = pin_block
        data["transfer_activity"] = self._check_transfer_activity(token, latest_block)
        data["deployer"] = self._analyze_deployer_and_creation(token)
        data["blockscout_holders"] = self._analyze_holders_blockscout(
            token,
            verified_amm_addresses=data["dex_pairs"].get(
                "verified_amm_addresses", []
            ),
            total_supply_raw=data["basic_info"].get("total_supply_raw"),
        )
        data["wash_trading"] = self._detect_wash_trading(token, latest_block)
        claim_evidence["activity_and_holders"] = self.ledger.fact_ids_since(checkpoint)

        print(" Phase 7/8: Trend analysis (historical rings)...")
        data["trend"] = self._build_token_trend(token)

        # ── Phase 8: Analysis + scoring ────────────────────────────────────
        print(" Running analysis...")
        analysis = self._analyze(data)

        # ── Phase 9: PoQ + seal ────────────────────────────────────────────
        report = {
            "token_address": token,
            "token_name": data["basic_info"].get("name"),
            "token_symbol": data["basic_info"].get("symbol"),
            "chain_id": self.chain_id,
            "chain": "robinhood",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "explorer_url": f"{EXPLORER_TOKEN}{token}",
            "data": data,
            "analysis": analysis,
            "provenance": self.ledger.to_dict(),
            "claim_evidence": claim_evidence,
        }

        if not self.ledger.verify():
            raise RuntimeError("Provenance evidence verification failed; refusing to seal")

        poq_scores = self._self_evaluate(report)
        report["poq_scores"] = poq_scores

        self._seal_report(report)
        print()
        if full_report:
            self._print_report(report)
        else:
            self._print_summary(report)
        return report

    # ── DATA GATHERING ──────────────────────────────────────────────────────

    def _fetch_basic_info(self, token: str) -> dict:
        info = {
            "address": token,
            "name": self.rpc.erc20_name(token),
            "symbol": self.rpc.erc20_symbol(token),
            "decimals": self.rpc.erc20_decimals(token),
            "total_supply_raw": self.rpc.erc20_total_supply(token),
        }
        dec = info["decimals"]
        info["total_supply"] = info["total_supply_raw"] / (10 ** dec) if dec else info["total_supply_raw"]
        return info

    def _analyze_contract(self, token: str) -> dict:
        code = self.rpc.get_code(token)
        code_hex = code.replace("0x", "").lower()

        dangerous_found = [name for name, sel in HONEYPOT_SELECTORS.items() if sel in code_hex]
        try:
            owner = self.rpc.erc20_owner(token)
        except RPCError:
            owner = "0x" + "0" * 40  # revert = no owner function = effectively renounced
        is_zero_owner = (owner == "0x" + "0" * 40 or owner == "0x" + "dead".zfill(40))
        has_mint = "a0712d68" in code_hex or "40c10f19" in code_hex

        return {
            "bytecode_size": len(code_hex),
            "owner": owner,
            "ownership_renounced": is_zero_owner,
            "has_mint_function": has_mint,
            "dangerous_functions": dangerous_found,
        }

    def _analyze_dex_pairs(self, token: str, data: dict) -> dict:
        """Analyze DEX pairs using DexScreener data + on-chain reserves."""
        dexscreener = data.get("dexscreener", {})
        goplus = data.get("goplus_security", {})
        all_pairs = dexscreener.get("pairs", [])
        pairs = [
            pair for pair in all_pairs
            if str(pair.get("chainId", "")).lower() == DEXSCREENER_CHAIN_ID
        ]
        primary = _pick_primary_pair({"pairs": pairs})

        result = {
            "pair_count": len(pairs),
            "discarded_foreign_pair_count": len(all_pairs) - len(pairs),
            "primary_pair_address": primary.get("pairAddress"),
            "primary_dex": primary.get("dexId"),
            "primary_labels": primary.get("labels", []),
            "primary_amm_version": _amm_version(primary),
            # Only addresses supplied by a Robinhood-chain DEX market record
            # are eligible for structural AMM exclusion in holder analysis.
            # V4 pool ids are 32-byte identifiers, not holder addresses.
            "verified_amm_addresses": sorted({
                str(pair.get("pairAddress")).lower()
                for pair in pairs
                if ADDRESS_RE.fullmatch(str(pair.get("pairAddress") or ""))
            }),
        }

        # Market data from DexScreener (all pairs aggregated)
        total_liquidity_usd = sum(_safe_float(p.get("liquidity", {}).get("usd")) for p in pairs)
        total_volume_24h = sum(_safe_float(p.get("volume", {}).get("h24")) for p in pairs)
        result["total_liquidity_usd"] = total_liquidity_usd
        result["total_volume_24h"] = total_volume_24h
        result["primary_price_usd"] = _safe_float(primary.get("priceUsd"))
        result["primary_liquidity_usd"] = _safe_float(primary.get("liquidity", {}).get("usd"))
        result["market_cap"] = _safe_float(primary.get("marketCap") or primary.get("fdv"))
        result["fdv"] = _safe_float(primary.get("fdv"))

        # Transaction counts (from best pair)
        txns = primary.get("txns", {})
        result["txns_5m"] = txns.get("m5", {})
        result["txns_1h"] = txns.get("h1", {})
        result["txns_6h"] = txns.get("h6", {})
        result["txns_24h"] = txns.get("h24", {})

        # Price changes
        price_change = primary.get("priceChange", {})
        result["price_change_5m"] = _safe_float(price_change.get("m5"))
        result["price_change_1h"] = _safe_float(price_change.get("h1"))
        result["price_change_6h"] = _safe_float(price_change.get("h6"))
        result["price_change_24h"] = _safe_float(price_change.get("h24"))

        # Token age
        created_at = primary.get("pairCreatedAt")
        if created_at:
            age_ms = time.time() * 1000 - created_at
            age_days = age_ms / 86400000
            result["token_age_days"] = round(age_days, 1)
            if age_days < 0.5:
                result["token_age_label"] = "NEW (<12h)"
            elif age_days < 1:
                result["token_age_label"] = "NEW (<24h)"
            elif age_days < 7:
                result["token_age_label"] = f"{age_days:.1f} days"
            else:
                result["token_age_label"] = f"{age_days:.0f} days"
        else:
            result["token_age_days"] = None
            result["token_age_label"] = "Unknown"

        # DexScreener URL
        result["dexscreener_url"] = primary.get("url")

        # DexScreener boosts (sentiment signal)
        boosts = primary.get("boosts", {})
        result["boosts_active"] = boosts.get("active", 0) if boosts else 0

        # Socials from DexScreener
        info = primary.get("info", {})
        result["websites"] = info.get("websites", [])
        result["socials"] = info.get("socials", [])
        result["image_url"] = info.get("imageUrl")

        # On-chain reserves for primary pair
        pair_addr = primary.get("pairAddress")
        if (result["primary_amm_version"] == "v2"
                and ADDRESS_RE.fullmatch(str(pair_addr or ""))
                and pair_addr != ZERO_ADDRESS):
            try:
                reserves = self.rpc.pair_get_reserves(pair_addr)
                t0 = self.rpc.pair_token0(pair_addr)
                t1 = self.rpc.pair_token1(pair_addr)
                result["on_chain_reserves"] = {
                    "pair_address": pair_addr,
                    "token0": t0,
                    "token1": t1,
                    "reserve0_raw": reserves["reserve0"],
                    "reserve1_raw": reserves["reserve1"],
                }
                # Determine which reserve is ETH (quote) vs token (base)
                if t1.lower() == WETH_ADDRESS.lower():
                    result["on_chain_reserves"]["reserve_eth"] = reserves["reserve1"] / 1e18
                    result["on_chain_reserves"]["reserve_tokens_raw"] = reserves["reserve0"]
                elif t0.lower() == WETH_ADDRESS.lower():
                    result["on_chain_reserves"]["reserve_eth"] = reserves["reserve0"] / 1e18
                    result["on_chain_reserves"]["reserve_tokens_raw"] = reserves["reserve1"]
            except Exception:
                result["on_chain_reserves"] = None

        # LP data from GoPlus
        lp_holders = goplus.get("lp_holders", [])
        result["lp_holder_count"] = _safe_int(goplus.get("lp_holder_count"))
        result["lp_total_supply"] = _safe_float(goplus.get("lp_total_supply"))
        result["lp_locked"] = False
        for lp in lp_holders:
            if lp.get("is_locked") == 1:
                result["lp_locked"] = True
                result["lp_locked_address"] = lp.get("address", "")
                result["lp_locked_percent"] = _safe_float(lp.get("percent")) * 100
                break

        return result

    def _check_transfer_activity(self, token: str, latest_block: int) -> dict:
        from_block = max(0, latest_block - 1000)

        try:
            logs = self.rpc.get_logs(
                from_block=from_block, to_block=latest_block,
                address=token, topics=[ERC20_TRANSFER_TOPIC],
            )
        except RPCError as e:
            return {"error": str(e), "recent_transfers": 0, "sniping_score": 0}

        total_transfers = len(logs)
        transfers_by_block = {}
        buyers, sellers = set(), set()

        for log in logs:
            if not log or "topics" not in log:
                continue
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            bn = int(log["blockNumber"], 16)
            transfers_by_block[bn] = transfers_by_block.get(bn, 0) + 1

            from_addr = _decode_address("0x" + topics[1][-40:])
            to_addr = _decode_address("0x" + topics[2][-40:])
            zero = "0x" + "0" * 40
            if from_addr != zero:
                sellers.add(from_addr)
            if to_addr != zero:
                buyers.add(to_addr)

        # Sniping detection
        sniping_score = 0.0
        sorted_blocks = sorted(transfers_by_block.keys())
        if sorted_blocks and total_transfers > 0:
            span = sorted_blocks[-1] - sorted_blocks[0] + 1
            if span > 1:
                cutoff = sorted_blocks[0] + max(1, span // 10)
                early = sum(v for bn, v in transfers_by_block.items() if bn <= cutoff)
                sniping_score = (early / total_transfers * 100)

        return {
            "recent_transfers": total_transfers,
            "recent_buyers": len(buyers),
            "recent_sellers": len(sellers),
            "sniping_score": round(sniping_score, 1),
            "blocks_analyzed": len(transfers_by_block),
        }

    # ── NEW: Blockscout + Deployer ──────────────────────────────────────────

    def _analyze_deployer_and_creation(self, token: str) -> dict:
        """Fetch deployer address, creation tx, and serial deployer history from Blockscout."""
        addr_info = _fetch_blockscout_address(token, ledger=self.ledger)
        token_info = _fetch_blockscout_token(token, ledger=self.ledger)

        creator = addr_info.get("creator_address", "")
        creation_tx = addr_info.get("creation_tx_hash", "")

        result = {
            "creator_address": creator,
            "creation_tx_hash": creation_tx,
            "is_verified": addr_info.get("is_verified", False),
            "is_scam": addr_info.get("is_scam", False),
            "blockscout_holders_count": token_info.get("holders_count"),
        }

        # Serial deployer check
        if creator:
            serial = _fetch_serial_deployer(creator, ledger=self.ledger)
            result["serial_deployer"] = serial
            result["is_serial_deployer"] = serial.get("is_serial_deployer", False)
            result["total_deployer_creations"] = serial.get("total_creations", 0)

        return result

    def _analyze_holders_blockscout(
        self,
        token: str,
        *,
        verified_amm_addresses=None,
        total_supply_raw=None,
    ) -> dict:
        """Fetch and classify top holders using explicit market evidence.

        Only holder addresses independently identified as Robinhood-chain AMM
        markets are excluded. Other contracts, including EIP-7702 accounts,
        remain economic holders. Concentration uses total token supply when it
        is available; a top-20 sample is never silently labelled as supply.
        """
        holders = _fetch_blockscout_holders(token, limit=20, ledger=self.ledger)
        if not holders:
            return {"holders": [], "blockscout_available": False}

        verified_amm = {
            str(address).lower()
            for address in (verified_amm_addresses or [])
            if ADDRESS_RE.fullmatch(str(address or ""))
        }
        # Compute concentration excluding only verified pair/AMM addresses.
        total_raw = 0
        excluded_amm = set()
        eoa_count = 0
        contract_count = 0
        scam_flagged = []
        proxy_holders = []
        eip7702_count = 0
        unclassified_contracts = []

        for h in holders:
            bal = _safe_int(h.get("balance_raw", "0"))
            h["balance_parsed"] = bal
            total_raw += bal

            addr = h.get("address", "").lower()
            addr_info = h.get("address_info", {})

            # Dual-source identity: the address must be a Robinhood-chain
            # market in DexScreener and a contract in Blockscout.
            if addr in verified_amm and h.get("is_contract"):
                excluded_amm.add(addr)

            if h.get("is_contract"):
                contract_count += 1
                if addr not in verified_amm:
                    unclassified_contracts.append(addr)
                # Check for proxy contracts (suspicious obfuscation)
                proxy_type = addr_info.get("proxy_type")
                if proxy_type and proxy_type not in ("eip7702",):
                    # EIP-7702 is normal account abstraction on Robinhood Chain;
                    # only track suspicious proxy types (eip1167, eip1967, etc.)
                    proxy_holders.append({"address": addr, "proxy_type": proxy_type})
                elif proxy_type == "eip7702":
                    eip7702_count += 1
                # Check for scam-flagged holders
                if addr_info.get("is_scam"):
                    scam_flagged.append(addr)
            else:
                eoa_count += 1

        # Retain sample totals for diagnostics, but use actual token supply as
        # the concentration denominator whenever the pinned RPC read provides it.
        non_pair_total = sum(
            h["balance_parsed"] for h in holders
            if h.get("address", "").lower() not in excluded_amm
        )
        supply_raw = _safe_int(total_supply_raw)
        concentration_denominator = supply_raw if supply_raw > 0 else total_raw
        concentration_basis = (
            "total_supply" if supply_raw > 0 else "top_holders_sample"
        )
        result = {
            "holders": holders,
            "blockscout_available": True,
            "total_parsed": total_raw,
            "non_pair_total": non_pair_total,
            "total_supply_raw": supply_raw or None,
            "concentration_denominator_raw": concentration_denominator,
            "concentration_basis": concentration_basis,
            "concentration_complete": supply_raw > 0,
            "pair_contracts_excluded": sorted(excluded_amm),
            "verified_amm_addresses": sorted(verified_amm),
            "amm_verification_method": (
                "DexScreener Robinhood pair + Blockscout contract"
            ),
            "unclassified_contract_holders": unclassified_contracts,
            "eoa_holders_count": eoa_count,
            "contract_holders_count": contract_count,
            "scam_flagged_holders": scam_flagged,
            "proxy_holder_count": len(proxy_holders),
            "proxy_holders": proxy_holders,
            "eip7702_count": eip7702_count,
        }

        if concentration_denominator > 0 and holders:
            # Raw concentration (includes pair contracts)
            result["top_1_pct"] = round(
                holders[0].get("balance_parsed", 0)
                / concentration_denominator * 100,
                2,
            )
            top10 = sum(h.get("balance_parsed", 0) for h in holders[:10])
            result["top_10_pct"] = round(
                top10 / concentration_denominator * 100, 2
            )
            whales = [
                h for h in holders
                if h.get("balance_parsed", 0)
                / concentration_denominator > 0.05
            ]
            result["whale_count"] = len(whales)

            # Adjusted concentration (excludes pair contracts) — more accurate for scoring
            if non_pair_total > 0:
                non_pair_holders = [
                    h for h in holders
                    if h.get("address", "").lower() not in excluded_amm
                ]
                if non_pair_holders:
                    result["adj_top_1_pct"] = round(
                        non_pair_holders[0].get("balance_parsed", 0)
                        / concentration_denominator * 100,
                        2,
                    )
                    adj_top10 = sum(h.get("balance_parsed", 0) for h in non_pair_holders[:10])
                    result["adj_top_10_pct"] = round(
                        adj_top10 / concentration_denominator * 100, 2
                    )

        return result

    # ── NEW: LP Lock Deep Verification ───────────────────────────────────────

    def _detect_timelock(self, holder_address: str, pair_address: str, lp_supply: int) -> dict:
        """Probe a contract with known timelock selectors to identify locker platform.

        Tries calling timelock function selectors (Unicrypt, TeamFinance, PinkLock,
        TrustSwap) and checks if they return valid uint256 timestamps.
        """
        result = {"detected": False, "platform": None, "release_date": None,
                   "release_timestamp": None, "locked_pct": 0}

        if not holder_address or holder_address == "0x" + "0" * 40:
            return result

        for sel_name, sel_hex in TIMELOCK_SELECTORS.items():
            # Timestamp selectors: call with index 0 (padded to 64 hex chars)
            if "getReleaseTime" in sel_name or "releaseDate" in sel_name or "unlockDate" in sel_name or "releaseTime" in sel_name:
                padded_index = "0" * 63 + "0"  # uint256(0)
                call_data = sel_hex + padded_index
            else:
                call_data = sel_hex + "0" * 56  # no-arg call

            try:
                raw = self.rpc.call(holder_address, call_data)
                if not raw or raw == "0x" or len(raw) < 66:
                    continue
                value = int(raw, 16)
                # Check if it looks like a valid Unix timestamp (after 2020-01-01)
                if value > 1577836800 and value < 2000000000:
                    result["detected"] = True
                    result["platform"] = TIMELOCK_PLATFORM_NAMES.get(sel_name, "Unknown")
                    result["release_timestamp"] = value
                    result["release_date"] = datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    break  # Found a timelock
            except RPCError:
                continue  # Selector not supported by this contract

        # If timelock detected, check how much LP is locked there
        if result["detected"] and lp_supply > 0:
            try:
                locked_bal = self.rpc.erc20_balance_of(pair_address, holder_address)
                result["locked_pct"] = round(locked_bal / lp_supply * 100, 2)
                result["locked_amount"] = locked_bal
            except RPCError:
                pass

        return result

    def _inspect_v2_lp_token(self, token: str, dex_data: dict) -> dict:
        """Collect V2 LP-token burn, holder, and timelock evidence.

        Checks: (1) Burn to dead addresses, (2) Known timelock contracts
        (Unicrypt, TeamFinance, PinkLock, TrustSwap), (3) Top LP holder
        contract probing for timelock patterns.
        """
        pair_addr = dex_data.get("primary_pair_address")
        if not pair_addr or pair_addr == "0x" + "0" * 40:
            return {"locked": False, "method": "No pair found"}

        # The LP token IS the pair contract itself. Check who holds it.
        try:
            lp_total_supply_raw = self.rpc.erc20_total_supply(pair_addr)
        except RPCError:
            return {"locked": False, "method": "Pair does not expose totalSupply"}

        # Check known lock destinations
        dead_addr = "0x000000000000000000000000000000000000dead"
        null_addr = "0x0000000000000000000000000000000000000001"

        try:
            dead_bal = self.rpc.erc20_balance_of(pair_addr, dead_addr)
        except RPCError:
            dead_bal = 0
        try:
            null_bal = self.rpc.erc20_balance_of(pair_addr, null_addr)
        except RPCError:
            null_bal = 0

        result = {
            "pair_address": pair_addr,
            "lp_total_supply_raw": lp_total_supply_raw,
            "dead_balance": dead_bal,
            "null_balance": null_bal,
            "total_burned": dead_bal + null_bal,
        }

        if lp_total_supply_raw > 0:
            burn_pct = round((dead_bal + null_bal) / lp_total_supply_raw * 100, 2)
            result["burn_pct"] = burn_pct
            result["timelock"] = {"detected": False}

            if burn_pct > 95:
                result["locked"] = True
                result["method"] = f"Burned to dead address ({burn_pct}%)"
            elif burn_pct > 50:
                result["locked"] = True
                result["method"] = f"Partially burned ({burn_pct}%)"
            else:
                # Not burned — check for timelock on top LP holder
                result["method"] = "Not burned"
                result["locked"] = False

                # Find the top LP holder via Blockscout
                lp_holders = _fetch_blockscout_holders(
                    pair_addr, limit=5, ledger=getattr(self, "ledger", None)
                )
                for h in lp_holders:
                    addr = h.get("address", "")
                    if not addr or addr.lower() in [dead_addr.lower(), null_addr.lower()]:
                        continue
                    bal = _safe_int(h.get("balance_raw", "0"))
                    if bal <= 0:
                        continue
                    # Probe this holder for timelock patterns
                    timelock = self._detect_timelock(addr, pair_addr, lp_total_supply_raw)
                    if timelock.get("detected"):
                        result["timelock"] = timelock
                        tl_pct = timelock.get("locked_pct", 0)
                        if tl_pct > 80:
                            result["locked"] = True
                            result["method"] = f"{timelock['platform']} timelock ({tl_pct}% locked, unlocks {timelock.get('release_date', '?')})"
                        elif tl_pct > 50:
                            result["locked"] = True
                            result["method"] = f"Partial {timelock['platform']} lock ({tl_pct}%, unlocks {timelock.get('release_date', '?')})"
                        else:
                            result["method"] = f"{timelock['platform']} timelock ({tl_pct}% locked, unlock: {timelock.get('release_date', '?')})"
                        break  # Found timelock, stop checking
        else:
            result["burn_pct"] = 0
            result["locked"] = False
            result["method"] = "No LP supply (no pair or zero liquidity)"
            result["timelock"] = {"detected": False}

        return result

    # ── Liquidity custody classification ────────────────────────────────────
    def _verify_lp_lock(self, token: str, dex_data: dict,
                        creator_address: str = None) -> dict:
        """Classify liquidity custody without conflating unknown with unlocked.

        V2 pools expose fungible LP tokens and can be evaluated using burn,
        timelock, and holder balances. V3/V4 liquidity is position-based, so
        Chainseer reports custody as unverified until it can prove the position
        owner and withdrawal restrictions.
        """
        pair_addr = dex_data.get("primary_pair_address")
        amm_version = str(
            dex_data.get("primary_amm_version") or "unknown"
        ).lower()
        liquidity_usd = _safe_float(dex_data.get("primary_liquidity_usd"))
        base = {
            "state": "custody_unverified",
            "locked": False,
            "withdrawal_verified": False,
            "hard_stop_eligible": False,
            "amm_version": amm_version,
            "pair_address": pair_addr,
            "method": "Liquidity custody has not been verified",
        }

        if not pair_addr or pair_addr == ZERO_ADDRESS:
            if liquidity_usd <= 0:
                return {
                    **base,
                    "state": "not_applicable",
                    "method": "No funded primary liquidity pool found",
                }
            return {
                **base,
                "method": (
                    "Liquidity is reported, but no canonical pair identifier "
                    "is available"
                ),
            }

        if amm_version == "v4":
            return {
                **base,
                "method": (
                    "Uniswap V4 uses position custody and a pool id, not an "
                    "ERC-20 LP token; position ownership and withdrawal "
                    "restrictions are not yet verified"
                ),
            }
        if amm_version == "v3":
            return {
                **base,
                "method": (
                    "Uniswap V3 liquidity is position-based, not an ERC-20 LP "
                    "token; position ownership and withdrawal restrictions "
                    "are not yet verified"
                ),
            }
        if amm_version != "v2":
            return {
                **base,
                "method": (
                    "AMM version is unknown; Chainseer will not apply V2 LP "
                    "token assumptions"
                ),
            }
        if not ADDRESS_RE.fullmatch(str(pair_addr)):
            return {
                **base,
                "method": "V2 pair identifier is not a valid contract address",
            }

        v2_evidence = self._inspect_v2_lp_token(token, dex_data)
        result = {**base, **v2_evidence, "amm_version": "v2"}
        burn_pct = _safe_float(result.get("burn_pct"))
        timelock = result.get("timelock") or {}
        timelock_pct = _safe_float(timelock.get("locked_pct"))
        lp_supply = _safe_int(result.get("lp_total_supply_raw"))

        result.update({
            "state": "custody_unverified",
            "locked": False,
            "withdrawal_verified": False,
            "hard_stop_eligible": False,
        })
        if lp_supply <= 0:
            if liquidity_usd <= 0:
                result.update({
                    "state": "not_applicable",
                    "method": "V2 pair has no LP supply or funded liquidity",
                })
            else:
                result["method"] = (
                    "Dex market data reports liquidity, but V2 LP supply is "
                    "zero or unavailable; custody cannot be verified"
                )
            return result

        if burn_pct >= 95:
            result.update({
                "state": "locked",
                "locked": True,
                "method": f"Burned to dead address ({burn_pct}%)",
            })
            return result

        if timelock.get("detected") and timelock_pct >= 95:
            result.update({
                "state": "protocol_secured",
                "locked": True,
                "method": (
                    f"{timelock.get('platform', 'Recognized')} timelock "
                    f"({timelock_pct}% locked, unlocks "
                    f"{timelock.get('release_date', '?')})"
                ),
            })
            return result

        result["method"] = (
            f"Only {max(burn_pct, timelock_pct):.2f}% of V2 LP supply is "
            "verified as burned or timelocked; remaining custody is unverified"
        )
        holders = _fetch_blockscout_holders(
            pair_addr, limit=5, ledger=getattr(self, "ledger", None)
        )
        normalized_creator = str(creator_address or "").lower()
        if normalized_creator:
            for holder_info in holders:
                holder = str(holder_info.get("address") or "")
                balance = _safe_int(holder_info.get("balance_raw", "0"))
                holder_pct = round(balance / lp_supply * 100, 2)
                if (
                    holder.lower() == normalized_creator
                    and not holder_info.get("is_contract")
                    and holder_pct >= 50
                ):
                    result.update({
                        "state": "creator_withdrawable",
                        "withdrawal_verified": True,
                        "hard_stop_eligible": True,
                        "withdrawal_controller": holder,
                        "withdrawable_pct": holder_pct,
                        "method": (
                            f"Token creator directly controls {holder_pct}% "
                            "of V2 LP supply"
                        ),
                    })
                    break

        return result

    # ── Historical Trend Analysis (multi-dimensional) ───────────────────────

    # Key metrics to track across analyses (from sealed component_scores)
    _TREND_METRICS = ["security", "liquidity", "holder_distribution", "volume",
                      "wash_trading", "lp_lock"]

    def _build_token_trend(self, token_address: str) -> dict:
        """Query Timechain for past analyses of this token and build score trajectory.

        Uses M51 Underlying-Pattern Extraction to surface individual metric
        trajectories beyond overall score, and S110 Concentration-Distribution
        Sensing to detect structural shifts (holder concentration, volume decay).

        Returns ordered list of past analyses with scores, timestamps, and
        computed trend metrics (direction, delta, volatility, metric shifts).
        """
        token_lower = token_address.lower()
        try:
            all_rings = self.tc.load()
        except Exception:
            return {"available": False, "past_analyses": []}

        # Filter for token_analysis rings matching this address
        past = []
        for ring in all_rings:
            if ring.get("ring_type") != "token_analysis":
                continue
            payload = ring.get("payload", {})
            addr = payload.get("token_address", "")
            if addr.lower() == token_lower:
                past.append({
                    "ring_index": ring.get("index"),
                    "timestamp": payload.get("timestamp", ""),
                    "legitimacy_score": payload.get("legitimacy_score"),
                    "risk_level": payload.get("risk_level"),
                    "component_scores": payload.get("component_scores", {}),
                    "confidence": payload.get("confidence", ""),
                })

        if len(past) < 2:
            return {"available": False, "past_count": len(past), "past_analyses": past}

        # Sort by timestamp
        past.sort(key=lambda p: p["timestamp"])

        # Compute overall trend metrics
        scores = [p["legitimacy_score"] for p in past if p["legitimacy_score"] is not None]
        if len(scores) < 2:
            return {"available": False, "past_count": len(past), "past_analyses": past}

        first_score = scores[0]
        last_score = scores[-1]
        delta = round(last_score - first_score, 1)

        # Direction
        if delta > 5:
            direction = "improving"
        elif delta < -5:
            direction = "declining"
        else:
            direction = "stable"

        # Volatility (std of scores)
        if len(scores) >= 3:
            mean = sum(scores) / len(scores)
            variance = sum((s - mean) ** 2 for s in scores) / len(scores)
            volatility = round(variance ** 0.5, 1)
        else:
            volatility = abs(delta)

        # ── Multi-dimensional metric trend extraction (M51 + S110) ───────────
        metric_shifts = []
        for metric in self._TREND_METRICS:
            values = []
            for p in past:
                cs = p.get("component_scores", {})
                if metric in cs and cs[metric] is not None:
                    values.append(cs[metric])
            if len(values) >= 2:
                m_delta = round(values[-1] - values[0], 1)
                if abs(m_delta) >= 10:  # significant shift
                    metric_shifts.append({
                        "metric": metric,
                        "first": values[0],
                        "last": values[-1],
                        "delta": m_delta,
                        "direction": "improving" if m_delta > 0 else "declining",
                    })

        result = {
            "available": True,
            "past_count": len(past),
            "past_analyses": past,
            "first_score": first_score,
            "last_score": last_score,
            "delta": delta,
            "direction": direction,
            "volatility": volatility,
            "score_range": f"{min(scores):.1f} - {max(scores):.1f}",
            "metric_shifts": metric_shifts,
        }

        return result

    # ── NEW: Self-Contained Tax Estimation ──────────────────────────────────

    def _estimate_tax_from_reserves(self, token: str, dex_data: dict) -> dict:
        """Estimate buy/sell tax using constant-product formula on pair reserves.

        Compares AMM-expected output (no tax) with actual market price
        (from DexScreener) to detect token-imposed fees.

        When GoPlus has tax data, this is a cross-check. When GoPlus is
        unavailable, this is the primary tax estimation method.
        """
        pair_addr = dex_data.get("primary_pair_address")
        reserves = dex_data.get("on_chain_reserves")
        price_usd = dex_data.get("primary_price_usd", 0)

        result = {
            "buy_tax_estimate": None,
            "sell_tax_estimate": None,
            "method": "constant_product",
            "available": False,
        }

        if not reserves or not pair_addr:
            return result

        try:
            r_eth = _safe_float(reserves.get("reserve_eth", 0))
            r_token_raw = _safe_int(reserves.get("reserve_tokens_raw", 0))

            if r_eth <= 0 or r_token_raw <= 0:
                return result

            # Compute AMM-expected price (no fees): price = reserveEth / reserveTokens
            dec = _safe_int(dex_data.get("basic_info", {}).get("decimals", 18))
            # We need decimals from basic_info — but we may not have it here
            # Use the DexScreener price to compute the expected token amount
            # If DexScreener says price = $0.001, and ETH = $3000, then
            # 1 ETH buys 3000/0.001 = 3,000,000 tokens (at DexScreener price)
            # AMM says 1 ETH buys reserveToken/reserveETH tokens
            # The difference = tax

            # Get basic_info from the data dict is not available here directly,
            # but we can compute using priceUsd and priceNative from DexScreener
            primary_pair = _pick_primary_pair({"pairs": [dex_data.get("primary_pair_raw", {})]})
            if not primary_pair:
                primary_pair = {}

            price_native = _safe_float(primary_pair.get("priceNative", 0))
            dex_reserves = primary_pair.get("liquidity", {})
            dex_liq_eth = _safe_float(dex_reserves.get("quote", 0))  # ETH in pool from DexScreener
            dex_liq_tokens = _safe_float(dex_reserves.get("base", 0))

            if dex_liq_eth <= 0 or dex_liq_tokens <= 0:
                return result

            # AMM expected ratio (no fees): token_per_eth = reserveTokens / reserveETH
            amm_ratio = dex_liq_tokens / dex_liq_eth
            # Market ratio from DexScreener: tokens_per_eth = 1 / priceNative
            if price_native > 0:
                market_ratio = 1.0 / price_native
                # Tax = how much less tokens you get vs AMM expected
                # If market_ratio < amm_ratio, there's a sell/buy tax
                discrepancy = (amm_ratio - market_ratio) / amm_ratio * 100

                # The discrepancy is split between buy and sell (both sides pay)
                estimated_tax = discrepancy / 2

                if estimated_tax < 0:
                    # Market ratio > AMM = token is overpriced (unlikely for tax, maybe price stale)
                    estimated_tax = 0

                result["buy_tax_estimate"] = round(max(0, estimated_tax), 2)
                result["sell_tax_estimate"] = round(max(0, estimated_tax), 2)
                result["available"] = True

                # Sanity check: if DexScreener and AMM agree within 0.5%, tax is ~0
                if abs(discrepancy) < 0.5:
                    result["buy_tax_estimate"] = 0.0
                    result["sell_tax_estimate"] = 0.0
        except Exception:
            pass

        return result

    # ── NEW: Wash Trading Detection ──────────────────────────────────────────

    def _scan_wash_window(self, token: str, from_block: int, to_block: int) -> dict:
        """Core wash trading scan over a single block window.

        Returns raw counts: ping_pong_pairs, circular_transfers, same_block_coordination.
        """
        try:
            logs = self.rpc.get_logs(
                from_block=from_block, to_block=to_block,
                address=token, topics=[ERC20_TRANSFER_TOPIC],
            )
        except RPCError:
            return {"ping_pong_pairs": [], "circular_transfers": [], "same_block_coordination": []}

        if not logs or len(logs) < 5:
            return {"ping_pong_pairs": [], "circular_transfers": [], "same_block_coordination": []}

        all_edges = []
        transfers_by_block = {}
        zero = "0x" + "0" * 40

        for log in logs:
            if not log or "topics" not in log:
                continue
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            from_addr = _decode_address("0x" + topics[1][-40:])
            to_addr = _decode_address("0x" + topics[2][-40:])
            data = log.get("data", "0x")
            value = int(data, 16) if data and data != "0x" else 0
            bn = int(log.get("blockNumber", "0x0"), 16)

            if from_addr == zero or to_addr == zero:
                continue

            all_edges.append((from_addr, to_addr, value, bn))
            if bn not in transfers_by_block:
                transfers_by_block[bn] = []
            transfers_by_block[bn].append((from_addr, to_addr, value))

        result = {"ping_pong_pairs": [], "circular_transfers": [], "same_block_coordination": []}

        # 1. Ping-pong
        pair_counts = {}
        for from_addr, to_addr, value, bn in all_edges:
            pair = tuple(sorted([from_addr.lower(), to_addr.lower()]))
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
        for pair, count in pair_counts.items():
            if count >= 8:
                result["ping_pong_pairs"].append({"addresses": list(pair), "round_trips": count // 2})

        # 2. Same-block coordination
        for bn, txs in transfers_by_block.items():
            if len(txs) >= 3:
                recipients = {}
                for from_addr, to_addr, value in txs:
                    if to_addr not in recipients:
                        recipients[to_addr] = {"from_addrs": set()}
                    recipients[to_addr]["from_addrs"].add(from_addr)
                for to_addr, info in recipients.items():
                    if len(info["from_addrs"]) >= 3:
                        result["same_block_coordination"].append({"block": bn, "recipient": to_addr, "sender_count": len(info["from_addrs"])})

        # 3. Circular transfers with amount similarity check
        edge_value_map = {}
        for from_addr, to_addr, value, bn in all_edges:
            key = (from_addr.lower(), to_addr.lower())
            if key not in edge_value_map:
                edge_value_map[key] = []
            edge_value_map[key].append(value)

        edge_map = {}
        for (from_addr, to_addr), values in edge_value_map.items():
            if from_addr not in edge_map:
                edge_map[from_addr] = []
            edge_map[from_addr].append(to_addr)

        cycles = []
        checked = set()
        for start in edge_map:
            if start in checked:
                continue
            for mid in edge_map.get(start, []):
                if mid == start:
                    continue
                for end in edge_map.get(mid, []):
                    if end == start and mid not in [start, end]:
                        a_vals = edge_value_map.get((start, mid), [])
                        b_vals = edge_value_map.get((mid, end), [])
                        c_vals = edge_value_map.get((end, start), [])
                        if a_vals and b_vals and c_vals:
                            avg_a = sum(a_vals) / len(a_vals)
                            avg_b = sum(b_vals) / len(b_vals)
                            avg_c = sum(c_vals) / len(c_vals)
                            max_val = max(avg_a, avg_b, avg_c)
                            if max_val > 0:
                                min_val = min(avg_a, avg_b, avg_c)
                                if min_val / max_val < 0.5:
                                    continue
                        cycle = tuple(sorted([start, mid, end]))
                        if cycle not in cycles:
                            cycles.append(cycle)
                            checked.update(cycle)

        result["circular_transfers"] = [{"addresses": list(c)} for c in cycles]
        return result

    def _compute_wash_score(self, scan_result: dict) -> int:
        """Compute a wash trading score (0-100) from scan result counts."""
        score = 0
        score += len(scan_result.get("ping_pong_pairs", [])) * 20
        score += len(scan_result.get("same_block_coordination", [])) * 15
        score += len(scan_result.get("circular_transfers", [])) * 25
        return min(100, score)

    def _detect_wash_trading(self, token: str, latest_block: int) -> dict:
        """Multi-window wash trading detection with stabilized scoring.

        Runs 3 independent scans over different block windows (500/1000/1500)
        and averages the scores. A consistent pattern across windows indicates
        real wash trading; volatile scores suggest noise or organic activity.
        """
        windows = [500, 1000, 1500]
        window_results = []

        for scan_size in windows:
            if latest_block < scan_size:
                continue
            from_block = latest_block - scan_size
            scan = self._scan_wash_window(token, from_block, latest_block)
            score = self._compute_wash_score(scan)
            window_results.append({"blocks": scan_size, "score": score, "scan": scan})

        if not window_results:
            return {"available": False}

        scores = [w["score"] for w in window_results]
        avg_score = round(sum(scores) / len(scores))

        # Standard deviation — measures signal stability
        if len(scores) >= 2:
            mean = sum(scores) / len(scores)
            variance = sum((s - mean) ** 2 for s in scores) / len(scores)
            std_dev = round(variance ** 0.5, 1)
        else:
            std_dev = 0.0

        # Use the widest window's detailed results for the report
        best = max(window_results, key=lambda w: w["blocks"])
        best_scan = best["scan"]

        if avg_score >= 50:
            wash_risk = "High"
        elif avg_score >= 20:
            wash_risk = "Medium"
        else:
            wash_risk = "Low"

        return {
            "available": True,
            "wash_score": avg_score,
            "wash_score_std": std_dev,
            "wash_score_windows": [{"blocks": w["blocks"], "score": w["score"]} for w in window_results],
            "wash_stable": std_dev < 20,
            "ping_pong_pairs": best_scan["ping_pong_pairs"],
            "circular_transfers": best_scan["circular_transfers"],
            "same_block_coordination": best_scan["same_block_coordination"],
            "wash_risk": wash_risk,
            "windows_analyzed": len(window_results),
        }

    # ── ANALYSIS ENGINE ────────────────────────────────────────────────────

    def _analyze(self, data: dict) -> dict:
        """12-component analysis with trend, sentiment, and enrichment.

        Tracks uncertainty: when a data source is unavailable, the component
        is flagged uncertain rather than scored optimistically (Mizukara's
        'unknown ≠ safe' principle, applied via M41 Self-Consistency Mapping).
        """
        scores = {}
        flags = {"green": [], "red": [], "yellow": []}
        uncertain = {}  # component -> reason it's uncertain (not confidently scored)

        gp = data.get("goplus_security", {})
        contract = data.get("contract_audit", {})
        dex = data.get("dex_pairs", {})
        activity = data.get("transfer_activity", {})
        basic = data.get("basic_info", {})
        lp_lock = data.get("lp_lock", {})
        tax_est = data.get("tax_estimate", {})
        deployer = data.get("deployer", {})
        bs_holders = data.get("blockscout_holders", {})
        wash = data.get("wash_trading", {})
        bs_addr = data.get("blockscout_address", {})
        bs_token = data.get("blockscout_token", {})
        source = data.get("source_code", {})
        trend = data.get("trend", {})
        gp_open_source = None
        if gp and gp.get("is_open_source") not in (None, ""):
            gp_open_source = _safe_int(gp.get("is_open_source")) == 1

        # ── 1. Security (GoPlus-driven, 0-100) ──────────────────────────────
        security_score = 100

        if gp:
            # Honeypot
            if _safe_int(gp.get("is_honeypot")) == 1:
                security_score = 0
                flags["red"].append("HONEYPOT DETECTED (GoPlus: cannot sell)")
            elif _safe_int(gp.get("cannot_buy")) == 1:
                security_score -= 50
                flags["red"].append("Cannot buy this token (GoPlus)")
            elif _safe_int(gp.get("cannot_sell_all")) == 1:
                security_score -= 30
                flags["yellow"].append("Cannot sell all holdings at once")

            if security_score > 0:
                # Buy/sell tax
                buy_tax = _safe_float(gp.get("buy_tax")) * 100  # GoPlus returns decimal (0.01 = 1%)
                sell_tax = _safe_float(gp.get("sell_tax")) * 100
                if buy_tax > 50:
                    security_score -= 40
                    flags["red"].append(f"Extreme buy tax: {buy_tax:.0f}%")
                elif buy_tax > 10:
                    security_score -= 20
                    flags["yellow"].append(f"High buy tax: {buy_tax:.1f}%")
                elif buy_tax > 3:
                    security_score -= 5
                    flags["yellow"].append(f"Moderate buy tax: {buy_tax:.1f}%")
                else:
                    flags["green"].append(f"Low buy tax: {buy_tax:.1f}%")

                if sell_tax > 50:
                    security_score -= 40
                    flags["red"].append(f"Extreme sell tax: {sell_tax:.0f}%")
                elif sell_tax > 10:
                    security_score -= 20
                    flags["yellow"].append(f"High sell tax: {sell_tax:.1f}%")
                elif sell_tax > 3:
                    security_score -= 5
                    flags["yellow"].append(f"Moderate sell tax: {sell_tax:.1f}%")
                else:
                    flags["green"].append(f"Low sell tax: {sell_tax:.1f}%")

                # Contract properties
                if _safe_int(gp.get("is_proxy")) == 1:
                    security_score -= 20
                    flags["red"].append("Proxy contract (logic can be swapped)")
                if _safe_int(gp.get("is_mintable")) == 1:
                    security_score -= 20
                    flags["red"].append("Mintable (supply can be inflated)")
                else:
                    flags["green"].append("Not mintable (fixed supply)")
                if _safe_int(gp.get("is_blacklisted")) == 1:
                    security_score -= 15
                    flags["red"].append("Blacklist function (can block sellers)")
                if _safe_int(gp.get("transfer_pausable")) == 1:
                    security_score -= 15
                    flags["red"].append("Transfers pausable (trading can be frozen)")
                if _safe_int(gp.get("hidden_owner")) == 1:
                    security_score -= 15
                    flags["red"].append("Hidden owner (obfuscated control)")
                if _safe_int(gp.get("selfdestruct")) == 1:
                    security_score -= 15
                    flags["red"].append("Self-destruct function present")
                if _safe_int(gp.get("can_take_back_ownership")) == 1:
                    security_score -= 15
                    flags["red"].append("Previous owner can reclaim ownership")
                if _safe_int(gp.get("slippage_modifiable")) == 1:
                    security_score -= 10
                    flags["yellow"].append("Owner can modify slippage")
                if _safe_int(gp.get("honeypot_with_same_creator")) == 1:
                    security_score -= 20
                    flags["red"].append("Same creator deployed other honeypots")

                # Owner check
                owner_addr = gp.get("owner_address", "0x" + "0" * 40)
                if owner_addr == "0x" + "0" * 40:
                    flags["green"].append("Ownership renounced")
                else:
                    security_score -= 10
                    flags["yellow"].append(f"Active owner: {owner_addr[:10]}...")
        else:
            # Fallback: use bytecode analysis
            if contract.get("ownership_renounced"):
                flags["green"].append("Ownership renounced")
                security_score -= 0
            else:
                flags["yellow"].append(f"Active owner (GoPlus unavailable)")
                security_score -= 10
            if contract.get("has_mint_function"):
                security_score -= 20
                flags["red"].append("Mint function detected")
            else:
                flags["green"].append("No mint function")
            for func in contract.get("dangerous_functions", []):
                security_score -= 10
                flags["red"].append(f"Dangerous function: {func}")

        scores["security"] = max(0, security_score)

        # Verified source improves inspectability, but is never proof of safety.
        if source.get("available") and source.get("is_verified"):
            scores["security"] = min(100, scores["security"] + 5)
            flags["green"].append("Contract source verified on Blockscout")
            if gp_open_source is False:
                flags["yellow"].append(
                    "Source-verification providers disagree: Blockscout is "
                    "verified, while GoPlus does not report open source"
                )
                uncertain["source_verification"] = (
                    "Blockscout and GoPlus disagree; Chainseer uses the "
                    "chain-specific Blockscout result"
                )
            if source.get("proxy_type"):
                if source.get("implementation_verified") is False:
                    scores["security"] = max(0, scores["security"] - 10)
                    flags["red"].append("Proxy implementation is not verified")
                else:
                    flags["yellow"].append(
                        f"Upgradeable proxy detected ({source.get('proxy_type')})"
                    )
        elif source.get("available"):
            scores["security"] = max(0, scores["security"] - 5)
            flags["yellow"].append("Contract source not verified on Blockscout")
            if gp_open_source is True:
                flags["yellow"].append(
                    "GoPlus reports open source, but Blockscout does not "
                    "verify this Robinhood Chain contract"
                )
                uncertain["source_verification"] = (
                    "Verification providers disagree; the chain-specific "
                    "Blockscout result takes precedence"
                )
        else:
            uncertain["source_verification"] = "Blockscout source metadata unavailable"
            if gp_open_source is True:
                flags["green"].append(
                    "GoPlus reports open source (Blockscout unavailable)"
                )
            elif gp_open_source is False:
                scores["security"] = max(0, scores["security"] - 10)
                flags["yellow"].append(
                    "GoPlus does not report the contract as open source"
                )

        # ── 2. Honeypot safety (separate component, 0-100) ──────────────────
        if gp:
            if _safe_int(gp.get("is_honeypot")) == 1 or _safe_int(gp.get("cannot_buy")) == 1:
                scores["honeypot_safety"] = 0
            elif (_safe_int(gp.get("cannot_sell_all")) == 1 or
                  _safe_int(gp.get("is_blacklisted")) == 1 or
                  _safe_int(gp.get("transfer_pausable")) == 1):
                scores["honeypot_safety"] = 30
            elif _safe_float(gp.get("sell_tax")) * 100 > 25:
                scores["honeypot_safety"] = 40
            else:
                scores["honeypot_safety"] = 95
        else:
            # GoPlus unavailable — cannot confirm honeypot status.
            # Mark uncertain (NOT "safe"): score is a neutral 50, not 70,
            # and flagged so the report says "unknown" rather than implying safety.
            scores["honeypot_safety"] = 50
            uncertain["honeypot_safety"] = "GoPlus unavailable — honeypot status unknown"

        # ── 3. Liquidity (0-100) ───────────────────────────────────────────
        liq_usd = dex.get("total_liquidity_usd", 0)
        if liq_usd <= 0:
            scores["liquidity"] = 10
            flags["red"].append("No liquidity found")
        elif liq_usd < 1000:
            scores["liquidity"] = 20
            flags["red"].append(f"Extremely low liquidity: ${liq_usd:,.0f}")
        elif liq_usd < 10000:
            scores["liquidity"] = 40
            flags["yellow"].append(f"Low liquidity: ${liq_usd:,.0f}")
        elif liq_usd < 50000:
            scores["liquidity"] = 60
            flags["yellow"].append(f"Moderate liquidity: ${liq_usd:,.0f}")
        else:
            scores["liquidity"] = min(95, 60 + liq_usd / 100000 * 35)
            flags["green"].append(f"Good liquidity: ${liq_usd:,.0f}")

        # LP locked check (GoPlus quick check — detailed verification in component 8)
        if dex.get("lp_locked"):
            pct = dex.get("lp_locked_percent", 0)
            scores["liquidity"] = min(100, scores["liquidity"] + 10)

        # ── 4. Holder distribution (0-100, age + concentration) ─────────────
        holder_count = _safe_int(gp.get("holder_count", 0))
        holder_count_source = "GoPlus"
        if holder_count <= 0:
            holder_count = _safe_int(
                bs_token.get("holders_count")
                or deployer.get("blockscout_holders_count")
            )
            holder_count_source = "Blockscout"

        holder_assessment = None
        if holder_count > 0:
            holder_assessment = _holder_base_score(
                holder_count, dex.get("token_age_days")
            )
            holder_assessment["source"] = holder_count_source
            scores["holder_distribution"] = holder_assessment["score"]
            ratio = holder_assessment["cohort_ratio"]
            cohort = holder_assessment["cohort"]
            target = holder_assessment["target_holders"]
            if holder_assessment["age_days"] is None:
                flags["yellow"].append(
                    "Token age unknown; holder adoption uses the conservative "
                    "90+ day benchmark"
                )
            if ratio >= 1.0 and holder_count >= 100:
                flags["green"].append(
                    f"Strong holder adoption for {cohort}: "
                    f"{holder_count:,}/{target:,} benchmark"
                )
            elif ratio >= 0.5 and holder_count >= 100:
                flags["green"].append(
                    f"Healthy holder adoption for {cohort}: "
                    f"{holder_count:,}/{target:,} benchmark"
                )
            elif ratio >= 0.2:
                flags["yellow"].append(
                    f"Below-cohort holder adoption: "
                    f"{holder_count:,}/{target:,} benchmark ({cohort})"
                )
            else:
                flags["red"].append(
                    f"Weak holder adoption for {cohort}: "
                    f"{holder_count:,}/{target:,} benchmark"
                )
        else:
            scores["holder_distribution"] = 50
            uncertain["holder_distribution"] = (
                "Holder count unavailable from GoPlus and Blockscout"
            )

        # Prefer total-supply concentration with only verified AMMs excluded.
        has_complete_blockscout_concentration = (
            bs_holders.get("blockscout_available")
            and bs_holders.get("adj_top_1_pct") is not None
            and bs_holders.get("concentration_complete", True)
        )
        if has_complete_blockscout_concentration:
            adj_top1 = bs_holders["adj_top_1_pct"]
            if holder_assessment is not None:
                holder_assessment["largest_non_amm_holder_pct"] = adj_top1
                holder_assessment["concentration_source"] = (
                    "Blockscout holders / pinned total supply"
                )
            if adj_top1 > 50:
                flags["red"].append(
                    f"Top non-AMM holder has {adj_top1:.1f}% of supply"
                )
                scores["holder_distribution"] = max(0, scores.get("holder_distribution", 50) - 30)
            elif adj_top1 > 20:
                flags["yellow"].append(
                    f"Top non-AMM holder has {adj_top1:.1f}% of supply"
                )
                scores["holder_distribution"] = max(0, scores.get("holder_distribution", 50) - 15)
            elif adj_top1 <= 10:
                flags["green"].append(
                    f"Low non-AMM concentration: largest holder "
                    f"{adj_top1:.1f}% of supply"
                )
        else:
            # GoPlus is a fallback only when supply-based Blockscout
            # concentration is unavailable. Ignore any independently verified
            # AMM address rather than treating the pool as a whale.
            verified_amms = {
                str(address).lower()
                for address in bs_holders.get("verified_amm_addresses", [])
            }
            holders_list = [
                holder for holder in gp.get("holders", [])
                if str(
                    holder.get("address")
                    or holder.get("holder_address")
                    or ""
                ).lower() not in verified_amms
            ]
            if holders_list:
                top1 = _safe_float(holders_list[0].get("percent")) * 100
                if holder_assessment is not None:
                    holder_assessment["largest_non_amm_holder_pct"] = top1
                    holder_assessment["concentration_source"] = "GoPlus fallback"
                if top1 > 50:
                    flags["red"].append(
                        f"Top holder has {top1:.1f}% of supply"
                    )
                    scores["holder_distribution"] = max(
                        0, scores["holder_distribution"] - 30
                    )
                elif top1 > 20:
                    flags["yellow"].append(
                        f"Top holder has {top1:.1f}% of supply"
                    )
                    scores["holder_distribution"] = max(
                        0, scores["holder_distribution"] - 15
                    )
            if (
                bs_holders.get("blockscout_available")
                and not bs_holders.get("concentration_complete", False)
            ):
                uncertain["holder_concentration"] = (
                    "Total supply denominator unavailable; top-holder sample "
                    "was not treated as a complete concentration measure"
                )

        # ── 5. Volume & activity (0-100) ─────────────────────────────────
        vol_24h = dex.get("total_volume_24h", 0)
        pair_count = dex.get("pair_count", 0)
        txns_24h = dex.get("txns_24h", {})
        total_txns_24h = _safe_int(txns_24h.get("buys")) + _safe_int(txns_24h.get("sells"))

        if vol_24h > 100000:
            scores["volume"] = 90
            flags["green"].append(f"Strong 24h volume: ${vol_24h:,.0f}")
        elif vol_24h > 10000:
            scores["volume"] = 70
            flags["green"].append(f"Good 24h volume: ${vol_24h:,.0f}")
        elif vol_24h > 1000:
            scores["volume"] = 50
            flags["yellow"].append(f"Low 24h volume: ${vol_24h:,.0f}")
        elif vol_24h > 0:
            scores["volume"] = 30
            flags["yellow"].append(f"Very low 24h volume: ${vol_24h:,.0f}")
        else:
            scores["volume"] = 10
            flags["red"].append("No trading volume detected")

        # Buy/sell ratio
        buys_24h = _safe_int(txns_24h.get("buys"))
        sells_24h = _safe_int(txns_24h.get("sells"))
        if buys_24h > 0 and sells_24h > 0:
            buy_sell_ratio = buys_24h / sells_24h
            if buy_sell_ratio > 3:
                flags["yellow"].append(f"Buy-heavy (buy/sell ratio: {buy_sell_ratio:.1f}x) -- possible dump incoming")
            elif buy_sell_ratio < 0.33:
                flags["yellow"].append(f"Sell-heavy (buy/sell ratio: {buy_sell_ratio:.1f}x) -- possible panic")

        # ── 6. Token maturity (0-100) ──────────────────────────────────────
        age_days = dex.get("token_age_days")
        if age_days is not None:
            if age_days > 30:
                scores["maturity"] = 90
                flags["green"].append(f"Established token ({age_days:.0f} days old)")
            elif age_days > 7:
                scores["maturity"] = 70
                flags["green"].append(f"1-4 weeks old ({age_days:.0f} days)")
            elif age_days > 1:
                scores["maturity"] = 50
                flags["yellow"].append(f"Very new ({age_days:.1f} days old)")
            else:
                scores["maturity"] = 20
                flags["red"].append(f"Brand new token ({dex.get('token_age_label', '<24h')}) -- high risk")
        else:
            scores["maturity"] = 50

        # Sniping check
        sniping = activity.get("sniping_score", 0)
        if sniping > 60:
            flags["red"].append(f"High sniping activity ({sniping:.0f}% early buys)")
        elif sniping > 30:
            flags["yellow"].append(f"Moderate sniping ({sniping:.0f}% early buys)")

        # ── 7. Creator risk (0-100, GoPlus) ────────────────────────────────
        creator = gp.get("creator_address", "")
        creator_balance = _safe_float(gp.get("creator_balance"))
        if creator and creator != "0x" + "0" * 40:
            creator_pct = _safe_float(gp.get("creator_percent"))
            if creator_pct > 5:
                scores["creator_risk"] = 20
                flags["red"].append(f"Creator holds {creator_pct:.1f}% of supply")
            elif creator_pct > 1:
                scores["creator_risk"] = 50
                flags["yellow"].append(f"Creator holds {creator_pct:.1f}% of supply")
            else:
                scores["creator_risk"] = 80
                flags["green"].append(f"Creator holds minimal supply ({creator_pct:.2f}%)")
        else:
            scores["creator_risk"] = 70

        # ── 8. LP Lock (0-100) ───────────────────────────────────────────────
        custody_state = lp_lock.get("state")
        if custody_state not in LIQUIDITY_CUSTODY_STATES:
            custody_state = (
                "locked" if lp_lock.get("locked") else "custody_unverified"
            )

        if custody_state in {"locked", "protocol_secured"}:
            scores["lp_lock"] = 95
            flags["green"].append(
                f"Liquidity custody {custody_state.replace('_', ' ')}: "
                f"{lp_lock.get('method', 'Unknown')}"
            )
        elif custody_state == "creator_withdrawable":
            scores["lp_lock"] = 15
            flags["red"].append(
                f"Creator-withdrawable liquidity: "
                f"{lp_lock.get('method', 'Unknown')}"
            )
        elif custody_state == "not_applicable":
            scores["lp_lock"] = 50
        else:
            scores["lp_lock"] = 50
            reason = lp_lock.get(
                "method", "Liquidity custody could not be verified"
            )
            uncertain["lp_lock"] = reason
            flags["yellow"].append(
                f"Liquidity custody unverified: {reason}"
            )

        # ── 9. Wash Trading (0-100) ────────────────────────────────────────
        if wash.get("available"):
            wash_score = wash.get("wash_score", 0)
            wash_risk = wash.get("wash_risk", "Low")
            wash_stable = wash.get("wash_stable", True)
            wash_std = wash.get("wash_score_std", 0)
            windows = wash.get("wash_score_windows", [])

            if wash_risk == "High":
                scores["wash_trading"] = max(0, 100 - wash_score)
                stability_note = "" if wash_stable else f" (volatile: std={wash_std})"
                flags["red"].append(f"High wash trading risk (score: {wash_score}{stability_note})")
                if not wash_stable:
                    flags["yellow"].append(f"Wash score unstable across windows: {[w['score'] for w in windows]}")
                if wash.get("ping_pong_pairs"):
                    flags["red"].append(f"Ping-pong: {len(wash['ping_pong_pairs'])} address pairs trading back and forth")
                if wash.get("same_block_coordination"):
                    flags["red"].append(f"Coordinated buys: {len(wash['same_block_coordination'])} blocks with 3+ wallets buying same token")
                if wash.get("circular_transfers"):
                    flags["red"].append(f"Circular transfers: {len(wash['circular_transfers'])} 3-way cycles detected")
            elif wash_risk == "Medium":
                scores["wash_trading"] = 60
                flags["yellow"].append(f"Moderate wash trading indicators (score: {wash_score})")
            else:
                scores["wash_trading"] = 90
                flags["green"].append("No wash trading patterns detected")
        else:
            scores["wash_trading"] = 70

        # ── 10. Deployer Risk (0-100) ────────────────────────────────────────
        deployer_score = 80  # default: known creator, not serial
        if deployer.get("is_serial_deployer"):
            deployer_score = 20
            flags["red"].append(f"Serial deployer: {deployer.get('creator_address', '?')[:10]}... created {deployer.get('total_deployer_creations')}+ contracts")
        elif deployer.get("creator_address"):
            creator_short = deployer["creator_address"][:10]
            flags["green"].append(f"Deployer: {creator_short}... (no serial deployment pattern)")
        else:
            deployer_score = 70

        # Deployer scam flag (from Blockscout deployer address check)
        if deployer.get("deployer_is_scam"):
            deployer_score = 0
            flags["red"].append("Deployer address flagged as SCAM on Blockscout")
        if deployer.get("deployer_reputation") == "bad":
            deployer_score = min(deployer_score, 30)
            flags["red"].append("Deployer has 'bad' reputation on Blockscout")

        # GoPlus: same creator deployed other honeypots
        if _safe_int(gp.get("honeypot_with_same_creator")) == 1:
            deployer_score = min(deployer_score, 10)
            flags["red"].append("Same creator deployed other known honeypots (GoPlus)")

        scores["deployer"] = deployer_score

        # Blockscout scam flag (token-level)
        if bs_addr.get("is_scam"):
            scores["security"] = max(0, scores["security"] - 50)
            flags["red"].append("Flagged as scam on Blockscout explorer")

        # Holder enrichment flags
        if bs_holders.get("scam_flagged_holders"):
            scam_count = len(bs_holders["scam_flagged_holders"])
            flags["red"].append(f"Scam-flagged wallet found among top holders ({scam_count} flagged)")
            scores["holder_distribution"] = max(0, scores.get("holder_distribution", 50) - 20)
        if bs_holders.get("proxy_holder_count", 0) > 2:
            flags["yellow"].append(f"Multiple proxy wallets holding tokens ({bs_holders['proxy_holder_count']} detected) -- possible obfuscation")

        # Self-contained tax estimate (cross-check or primary when GoPlus unavailable)
        if tax_est.get("available"):
            buy_tax_self = tax_est.get("buy_tax_estimate", 0)
            sell_tax_self = tax_est.get("sell_tax_estimate", 0)
            if buy_tax_self > 10:
                flags["yellow"].append(f"Self-estimated buy tax: {buy_tax_self:.1f}% (reserve-based)")
            if sell_tax_self > 10:
                flags["yellow"].append(f"Self-estimated sell tax: {sell_tax_self:.1f}% (reserve-based)")

        # ── 11. Trend (0-100, Timechain history + M51 metric shifts) ──────────
        if trend.get("available"):
            direction = trend.get("direction", "stable")
            delta = trend.get("delta", 0)
            volatility = trend.get("volatility", 0)

            if direction == "improving":
                scores["trend"] = 90
                flags["green"].append(f"Score improving (+{delta:.1f} across {trend.get('past_count')} analyses, range: {trend.get('score_range')})")
            elif direction == "declining":
                decline_penalty = min(40, abs(delta))
                scores["trend"] = max(10, 60 - decline_penalty)
                flags["red"].append(f"Score declining ({delta:.1f} across {trend.get('past_count')} analyses, range: {trend.get('score_range')})")
            else:
                scores["trend"] = 70
                flags["yellow"].append(f"Score stable ({trend.get('first_score')} -> {trend.get('last_score')})")

            # M51 Underlying-Pattern Extraction: flag significant metric-level shifts
            metric_shifts = trend.get("metric_shifts", [])
            declining_metrics = [m for m in metric_shifts if m["direction"] == "declining"]
            if declining_metrics:
                for m in declining_metrics:
                    flags["yellow"].append(f"Metric shift: {m['metric']} declining ({m['first']:.0f} -> {m['last']:.0f})")
            improving_metrics = [m for m in metric_shifts if m["direction"] == "improving"]
            if improving_metrics:
                for m in improving_metrics:
                    flags["green"].append(f"Metric shift: {m['metric']} improving ({m['first']:.0f} -> {m['last']:.0f})")
        else:
            scores["trend"] = 70  # neutral — no history

        # ── 12. Sentiment (0-100, DexScreener boosts) ─────────────────────────
        boosts_active = dex.get("boosts_active", 0)
        if boosts_active and boosts_active > 0:
            scores["sentiment"] = 85
            flags["green"].append(f"Active DexScreener boost ({boosts_active} points) — community attention")
        else:
            scores["sentiment"] = 60  # neutral — no boost data

        # ── OVERALL COMPOSITE ──────────────────────────────────────────────
        weights = {
            "security":            0.19,
            "honeypot_safety":      0.11,
            "liquidity":           0.14,
            "lp_lock":             0.09,
            "holder_distribution":  0.07,
            "volume":              0.07,
            "maturity":            0.06,
            "creator_risk":        0.08,
            "wash_trading":        0.07,
            "deployer":            0.07,
            "sentiment":            0.03,
            "trend":               0.02,
        }
        overall = sum(scores.get(k, 50) * w for k, w in weights.items())
        overall = round(overall, 1)
        scores["legitimacy"] = overall

        # Confidence: based on data completeness
        has_goplus = bool(gp)
        has_dexscreener = len(data.get("dexscreener", {}).get("pairs", [])) > 0
        has_reserves = dex.get("on_chain_reserves") is not None
        has_blockscout = bool(bs_addr.get("creator_address"))
        has_wash = wash.get("available", False)
        has_tax = tax_est.get("available", False)
        has_trend = trend.get("available", False)
        confidence_parts = []
        if has_goplus:
            confidence_parts.append("GoPlus")
        if has_dexscreener:
            confidence_parts.append("DexScreener")
        if has_reserves:
            confidence_parts.append("reserves")
        if has_blockscout:
            confidence_parts.append("Blockscout")
        if has_wash:
            confidence_parts.append("wash detect")
        if has_tax:
            confidence_parts.append("tax estimate")
        if has_trend:
            confidence_parts.append("trend")
        confidence_parts.append("contract+logs")
        confidence_label = f"{len(confidence_parts)}/8 sources ({', '.join(confidence_parts)})"

        if overall >= 80:
            model_risk_level = "Low"
        elif overall >= 60:
            model_risk_level = "Medium"
        elif overall >= 40:
            model_risk_level = "High"
        else:
            model_risk_level = "Critical"

        # Hard-stop layer: weighted averages must not hide loss-of-capital risks.
        hard_stops = []

        def hard_stop(code, severity, reason, action="HIGH-RISK SPECULATION"):
            hard_stops.append({
                "code": code,
                "severity": severity,
                "reason": reason,
                "action": action,
            })

        if (_safe_int(gp.get("is_honeypot")) == 1 or
                _safe_int(gp.get("cannot_buy")) == 1):
            hard_stop("HONEYPOT", "Critical", "Confirmed honeypot or buying is blocked", "AVOID")
        if _safe_int(gp.get("cannot_sell_all")) == 1:
            hard_stop("SELL_RESTRICTED", "Critical", "Selling all tokens is restricted", "AVOID")
        if deployer.get("deployer_is_scam") or bs_addr.get("is_scam"):
            hard_stop("SCAM_FLAG", "Critical", "Token or deployer is scam-flagged", "AVOID")

        liquidity_usd = _safe_float(dex.get("primary_liquidity_usd"))
        if (
            liquidity_usd > 0
            and custody_state == "creator_withdrawable"
            and lp_lock.get("withdrawal_verified")
            and lp_lock.get("hard_stop_eligible")
        ):
            withdrawable_pct = _safe_float(
                lp_lock.get("withdrawable_pct")
            )
            hard_stop(
                "UNLOCKED_LP", "High",
                f"The creator can withdraw {withdrawable_pct:.1f}% of the "
                f"primary V2 liquidity position (${liquidity_usd:,.0f})",
                "AVOID",
            )

        top_holder_pct = _safe_float(bs_holders.get("adj_top_1_pct"))
        if top_holder_pct >= 50:
            severity = "Critical" if top_holder_pct >= 80 else "High"
            hard_stop(
                "EXTREME_CONCENTRATION", severity,
                f"Top non-AMM holder controls {top_holder_pct:.1f}% of token supply",
                "AVOID" if severity == "Critical" else "HIGH-RISK SPECULATION",
            )

        if source.get("proxy_type") and source.get("implementation_verified") is False:
            hard_stop(
                "UNVERIFIED_PROXY", "High",
                "Upgradeable proxy implementation is not verified",
            )

        ranks = {"Low": 0, "Medium": 1, "High": 2, "Critical": 3}
        effective_rank = ranks[model_risk_level]
        for stop in hard_stops:
            effective_rank = max(effective_rank, ranks[stop["severity"]])
        # Multiple independent High hard stops compound into Critical risk.
        if sum(stop["severity"] == "High" for stop in hard_stops) >= 2:
            effective_rank = ranks["Critical"]
        risk_level = next(level for level, rank in ranks.items() if rank == effective_rank)

        if any(stop["action"] == "AVOID" for stop in hard_stops) or risk_level == "Critical":
            action_label = "AVOID"
        elif risk_level == "High":
            action_label = "HIGH-RISK SPECULATION"
        elif risk_level == "Medium":
            action_label = "WATCHLIST"
        else:
            action_label = "PASSES INITIAL CHECKS"

        major_unknowns = [
            reason for key, reason in uncertain.items()
            if key in {"honeypot_safety", "source_verification", "lp_lock"}
        ]
        if major_unknowns:
            confidence_grade = "LIMITED"
        elif len(confidence_parts) >= 7 and not uncertain:
            confidence_grade = "HIGH"
        elif len(confidence_parts) >= 5 and len(uncertain) <= 1:
            confidence_grade = "MODERATE"
        else:
            confidence_grade = "LIMITED"

        if action_label == "AVOID":
            rec = "Loss-of-capital risks outweigh the positive signals. Avoid until the hard-stop conditions are resolved and independently verified."
        elif action_label == "HIGH-RISK SPECULATION":
            rec = "Material risks remain. Treat this only as high-risk speculation and verify every flagged condition before exposure."
        elif action_label == "WATCHLIST":
            rec = "No automatic rejection, but important cautions remain. Keep on a watchlist until the red flags and unknowns improve."
        else:
            rec = "No hard-stop condition was detected and the token passes initial checks. This is not proof of safety; continue normal due diligence."

        return {
            "legitimacy_score": overall,
            "risk_level": risk_level,
            "model_risk_level": model_risk_level,
            "action_label": action_label,
            "hard_stop_overrides": hard_stops,
            "component_scores": scores,
            "weights": weights,
            "holder_assessment": holder_assessment,
            "green_flags": flags["green"],
            "red_flags": flags["red"],
            "yellow_flags": flags["yellow"],
            "uncertain_components": uncertain,
            "recommendation": rec,
            "confidence": confidence_label,
            "confidence_grade": confidence_grade,
            "confidence_limiters": major_unknowns,
        }

    # ── PoQ SELF-EVALUATION ────────────────────────────────────────────────

    def _self_evaluate(self, report: dict) -> dict:
        analysis = report.get("analysis", {})
        data = report.get("data", {})
        scores = {"coherence": 0, "relevance": 0, "novelty": 0,
                  "consistency": 0, "depth": 0, "covenant": 0}

        analysis = report.get("analysis", {})
        red_count = len(analysis.get("red_flags", []))
        contradictions = (
            analysis.get("risk_level") == "Low" and red_count > 0
        )
        scores["coherence"] = 180 if contradictions else 225
        scores["relevance"] = 230 if report.get("token_address") else 120

        # Novelty: GoPlus + DexScreener provide significantly more data
        has_goplus = bool(data.get("goplus_security"))
        has_dexscreener = len(data.get("dexscreener", {}).get("pairs", [])) > 0
        if has_goplus and has_dexscreener:
            scores["novelty"] = 210
        elif has_goplus or has_dexscreener:
            scores["novelty"] = 170
        else:
            scores["novelty"] = 120

        # Consistency
        if analysis.get("risk_level") == "Low" and not analysis.get("red_flags"):
            scores["consistency"] = 220
        elif analysis.get("risk_level") == "Critical" and analysis.get("red_flags"):
            scores["consistency"] = 220
        elif analysis.get("risk_level") == "Low" and analysis.get("red_flags"):
            scores["consistency"] = 160
        else:
            scores["consistency"] = 200

        # Depth: count filled data sources
        sources_filled = 0
        for key in ["goplus_security", "dexscreener", "basic_info", "contract_audit",
                    "dex_pairs", "transfer_activity", "lp_lock", "tax_estimate",
                    "deployer", "blockscout_holders", "wash_trading", "blockscout_address",
                    "trend"]:
            d = data.get(key, {})
            if d and not d.get("error") and (isinstance(d, dict) and len(d) > 0):
                sources_filled += 1
            elif isinstance(d, list) and len(d) > 0:
                sources_filled += 1
        depth_ratio = sources_filled / 13
        scores["depth"] = int(100 + depth_ratio * 120)

        # Honest uncertainty and traceable evidence are the covenant-facing signals.
        provenance_ok = bool(report.get("provenance", {}).get("fact_count"))
        scores["covenant"] = 235 if provenance_ok else 170
        return scores

    # ── TIMECHAIN SEALING ───────────────────────────────────────────────────

    def _seal_report(self, report: dict):
        cognition = self.cognitive_loop.prepare(report)
        poq = report.get("poq_scores", {})
        analysis = report["analysis"]
        candidate = (
            f"Token {report['token_address']} assessed as {analysis['risk_level']} risk "
            f"with legitimacy score {analysis['legitimacy_score']}/100. "
            f"Evidence contains {report['provenance']['fact_count']} provenance facts; "
            f"uncertain components: {len(analysis.get('uncertain_components', {}))}."
        )
        gate_context = (
            "Chainseer block-pinned on-chain token analysis evidence: "
            + json.dumps({
                "token_address": report["token_address"],
                "analysis": report.get("analysis", {}),
                "provenance": report.get("provenance", {}),
                "claim_evidence": report.get("claim_evidence", {}),
            }, sort_keys=True, default=str)
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            candidate,
            context=gate_context,
            ring_type="token_analysis",
            external_scores={
                "coherence": poq.get("coherence", 0),
                "relevance": poq.get("relevance", 0),
                "novelty": poq.get("novelty", 0),
                "consistency": poq.get("consistency", 0),
                "depth": poq.get("depth", 0),
                "covenant": poq.get("covenant", 0),
            },
            use_index=True,
            frame="assertion",
            evidence_texts=[
                json.dumps(report.get("analysis", {}), sort_keys=True, default=str),
                json.dumps(report.get("provenance", {}), sort_keys=True, default=str),
                json.dumps(report.get("claim_evidence", {}), sort_keys=True, default=str),
            ],
            extra_payload={
                "token_address": report["token_address"],
                "token_name": report.get("token_name"),
                "legitimacy_score": report["analysis"]["legitimacy_score"],
                "risk_level": report["analysis"]["risk_level"],
                "model_risk_level": report["analysis"].get("model_risk_level"),
                "action_label": report["analysis"].get("action_label"),
                "hard_stop_overrides": report["analysis"].get("hard_stop_overrides", []),
                "recommendation": report["analysis"]["recommendation"],
                "green_flags": report["analysis"]["green_flags"],
                "red_flags": report["analysis"]["red_flags"],
                "component_scores": report["analysis"]["component_scores"],
                "confidence": report["analysis"]["confidence"],
                "confidence_grade": report["analysis"].get("confidence_grade"),
                "timestamp": report["timestamp"],
                "block_pin": report["provenance"]["block_pin"],
                "provenance": report["provenance"],
                "claim_evidence": report.get("claim_evidence", {}),
                "uncertain_components": report["analysis"].get("uncertain_components", {}),
                "cognitive_loop": cognition,
            },
        )
        if ring is None:
            raise RuntimeError(
                f"PoQ refused token analysis seal ({verdict.get('decision')}): "
                f"{'; '.join(verdict.get('reasons', []))}"
            )
        report["analysis_ring"] = ring.get("index")
        report["analysis_ring_hash"] = ring.get("ring_hash")
        report["poq_verdict"] = verdict
        self.cognitive_loop.finalize(report, ring)
        print(f"  Sealed ring {ring.get('index', '?')} (hash: {ring.get('ring_hash', '?')[:16]}...)")
        print(f"  PoQ: coherence={poq['coherence']} relevance={poq['relevance']} "
              f"depth={poq['depth']} covenant={poq['covenant']}")

    @staticmethod
    def structured_output_instructions() -> str:
        """Schema text for any future LLM explanation layer."""
        return (
            "Return exactly one JSON object matching this schema. Do not add markdown: "
            + json.dumps(ANALYSIS_OUTPUT_SCHEMA, sort_keys=True)
        )

    def reflect_on_analysis(self, analysis_ring: int, outcomes: dict,
                            evidence_fact_ids: list = None,
                            observed_at: str = None) -> dict:
        """Append a real-world outcome without rewriting the original analysis.

        Security outcomes and market outcomes stay separate so price movement does
        not incorrectly train the agent to equate an unprofitable token with a rug.
        """
        if not isinstance(outcomes, dict) or not outcomes:
            raise ValueError("outcomes must be a non-empty mapping")
        rings = self.tc.load()
        original = next((r for r in rings if r.get("index") == analysis_ring), None)
        if original is None or original.get("ring_type") != "token_analysis":
            raise ValueError("analysis_ring must reference a token_analysis ring")

        security_keys = {
            "rug_pull", "liquidity_removed_pct", "honeypot_observed", "exploit",
            "owner_privilege_used", "tax_changed", "contract_upgraded",
        }
        market_keys = {"price_return_pct", "max_drawdown_pct", "volatility_pct"}
        security = {k: outcomes[k] for k in security_keys if k in outcomes}
        market = {k: outcomes[k] for k in market_keys if k in outcomes}
        other = {k: v for k, v in outcomes.items() if k not in security_keys | market_keys}
        token = original.get("payload", {}).get("token_address")
        original_risk = original.get("payload", {}).get("risk_level")
        adverse_security_event = any(bool(security.get(k)) for k in (
            "rug_pull", "honeypot_observed", "exploit", "owner_privilege_used"
        )) or _safe_float(security.get("liquidity_removed_pct")) >= 50
        calibration = {
            "original_risk_level": original_risk,
            "adverse_security_event": adverse_security_event,
            "dangerous_false_negative": adverse_security_event and original_risk in {"Low", "Medium"},
        }
        candidate = (
            f"Observed outcome for token {token}, linked to analysis ring {analysis_ring}. "
            f"Adverse security event: {adverse_security_event}. "
            "Market performance is recorded separately from security correctness."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            candidate,
            context="Real-world Chainseer outcome reflection",
            ring_type="analysis_outcome",
            external_scores={"coherence": 235, "relevance": 245, "novelty": 225,
                             "consistency": 235, "depth": 220, "covenant": 245},
            relevant_rings=[original],
            declared_evidence=1,
            extra_payload={
                "analysis_ring": analysis_ring,
                "analysis_ring_hash": original.get("ring_hash"),
                "token_address": token,
                "observed_at": observed_at or datetime.now(timezone.utc).isoformat(),
                "security_outcomes": security,
                "market_outcomes": market,
                "other_outcomes": other,
                "evidence_fact_ids": list(evidence_fact_ids or []),
                "calibration": calibration,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused outcome reflection: {verdict.get('decision')}")
        return {"ring": ring, "verdict": verdict, "calibration": calibration}

    # ── REPORT PRINTING ────────────────────────────────────────────────────

    def _format_summary(self, report: dict, fmt: str = "text") -> str:
        """Format an investor decision card, with hard stops above model score."""
        analysis = report["analysis"]
        data = report["data"]
        basic = data.get("basic_info", {})
        dex = data.get("dex_pairs", {})
        lp_lock = data.get("lp_lock", {})
        prov = report.get("provenance", {})

        name = basic.get("name", "Unknown")
        symbol = basic.get("symbol", "???")
        risk = analysis["risk_level"]
        score = analysis["legitimacy_score"]
        price = dex.get("primary_price_usd")
        mcap = dex.get("market_cap")
        liquidity = _safe_float(dex.get("total_liquidity_usd"))
        volume = _safe_float(dex.get("total_volume_24h"))
        age = dex.get("token_age_label", "Unknown")

        def money(value):
            value = _safe_float(value)
            if value >= 1_000_000_000:
                return f"${value / 1_000_000_000:.2f}B"
            if value >= 1_000_000:
                return f"${value / 1_000_000:.2f}M"
            if value >= 1_000:
                return f"${value / 1_000:.1f}K"
            return f"${value:,.0f}"

        def price_text(value):
            value = _safe_float(value)
            if value >= 1:
                return f"${value:,.2f}"
            if value >= 0.01:
                return f"${value:.4f}"
            return f"${value:.8f}"

        action = analysis.get("action_label", "WATCHLIST")
        action_icons = {
            "AVOID": "🚨", "HIGH-RISK SPECULATION": "🔴",
            "WATCHLIST": "⚠️", "PASSES INITIAL CHECKS": "✅",
        }
        lines = [f"CHAINSEER — {name} ({symbol})", ""]
        lines.append(f"{action_icons.get(action, '❓')} ACTION: {action}")
        lines.append(
            f"Risk: {risk.upper()}  ·  Model score: {score}/100  ·  "
            f"Confidence: {analysis.get('confidence_grade', 'UNKNOWN')}"
        )
        if analysis.get("model_risk_level") and analysis["model_risk_level"] != risk:
            lines.append(
                f"Risk override: weighted model said {analysis['model_risk_level']}, "
                f"hard-stop rules raised it to {risk}."
            )
        lines.append("")

        lines.append("MARKET")
        market_parts = []
        if price:
            market_parts.append(f"Price {price_text(price)}")
        if mcap:
            market_parts.append(f"MC {money(mcap)}")
        if liquidity:
            market_parts.append(f"Liquidity {money(liquidity)}")
        if volume:
            market_parts.append(f"24h volume {money(volume)}")
        market_parts.append(f"Age {age}")
        lines.append("  " + "  ·  ".join(market_parts))
        if mcap and liquidity:
            lines.append(f"  Market cap / liquidity: {_safe_float(mcap) / liquidity:.1f}x")
        lines.append("")

        custody_state = lp_lock.get("state", "custody_unverified")
        lines.append("LIQUIDITY CUSTODY")
        lines.append(
            f"  {custody_state.replace('_', ' ').upper()}  ·  "
            f"AMM {str(lp_lock.get('amm_version', 'unknown')).upper()}"
        )
        lines.append(f"  {lp_lock.get('method', 'Custody evidence unavailable')}")
        lines.append("")

        hard_stops = analysis.get("hard_stop_overrides", [])
        red_flags = analysis.get("red_flags", [])
        dealbreakers = [stop["reason"] for stop in hard_stops]
        stop_codes = {stop.get("code") for stop in hard_stops}
        covered_terms = set()
        if "UNLOCKED_LP" in stop_codes:
            covered_terms.update({"lp", "liquidity"})
        if "EXTREME_CONCENTRATION" in stop_codes:
            covered_terms.update({"holder", "concentrat"})
        if "HONEYPOT" in stop_codes or "SELL_RESTRICTED" in stop_codes:
            covered_terms.update({"honeypot", "sell", "buy"})
        if "SCAM_FLAG" in stop_codes:
            covered_terms.add("scam")
        for flag in red_flags:
            lower = flag.lower()
            if any(term in lower for term in covered_terms):
                continue
            if flag not in dealbreakers:
                dealbreakers.append(flag)
        if dealbreakers:
            lines.append("WHY THIS IS RISKY")
            for reason in dealbreakers[:3]:
                lines.append(f"  • {reason}")
            lines.append("")

        green_flags = analysis.get("green_flags", [])
        if "UNLOCKED_LP" in stop_codes:
            green_flags = [flag for flag in green_flags if "liquidity" not in flag.lower()]
        if "EXTREME_CONCENTRATION" in stop_codes:
            green_flags = [flag for flag in green_flags if "holder" not in flag.lower()]
        if green_flags:
            lines.append("POSITIVE SIGNALS")
            for signal in green_flags[:3]:
                lines.append(f"  • {signal}")
            lines.append("")

        uncertain = analysis.get("uncertain_components", {})
        if uncertain:
            lines.append("WHAT IS STILL UNKNOWN")
            for reason in list(uncertain.values())[:3]:
                lines.append(f"  • {reason}")
            lines.append("")

        lines.append("INVESTOR TAKE")
        lines.append(f"  {analysis['recommendation']}")
        lines.append("")
        pin = prov.get("block_pin", "?")
        pin_text = f"{pin:,}" if isinstance(pin, (int, float)) else str(pin)
        lines.append(f"Evidence: {prov.get('fact_count', 0)} facts  ·  Block {pin_text}")
        lines.append("Risk assessment only — not financial advice.")
        return "\n".join(lines)

    def _print_summary(self, report: dict):
        """Print the investor summary to stdout."""
        print(self._format_summary(report, fmt="text"))

    def _print_report(self, report: dict):
        analysis = report["analysis"]
        data = report["data"]
        basic = data.get("basic_info", {})
        contract = data.get("contract_audit", {})
        gp = data.get("goplus_security", {})
        dex = data.get("dex_pairs", {})
        activity = data.get("transfer_activity", {})
        lp_lock = data.get("lp_lock", {})
        deployer = data.get("deployer", {})
        wash = data.get("wash_trading", {})
        tax_est = data.get("tax_estimate", {})
        bs_addr = data.get("blockscout_address", {})
        bs_holders = data.get("blockscout_holders", {})

        print("=" * 72)
        print(" CHAINSEER ANALYSIS REPORT v7.0")
        print("=" * 72)

        # Token identity
        name = basic.get("name", "Unknown")
        symbol = basic.get("symbol", "???")
        price = dex.get("primary_price_usd", None)
        mcap = dex.get("market_cap", None)
        age = dex.get("token_age_label", "Unknown")

        header = f" {name} ({symbol})"
        if price:
            header += f"  |  ${price:.6f}"
        if mcap:
            header += f"  |  MC: ${mcap:,.0f}"
        header += f"  |  Age: {age}"
        print(header)
        print(f" Address: {report['token_address']}")
        print(f" Supply: {basic.get('total_supply', 'Unknown'):,.2f}")
        print(f" Explorer: {report.get('explorer_url', '')}")
        if dex.get("dexscreener_url"):
            print(f" DexScreener: {dex['dexscreener_url']}")
        if dex.get("websites"):
            for w in dex["websites"]:
                print(f" Website: {w.get('label', '')} - {w.get('url', '')}")
        if dex.get("socials"):
            for s in dex["socials"][:3]:
                print(f" {s.get('type', '').title()}: {s.get('url', '')}")
        print()

        # Executive investment decision — deliberately before technical detail.
        risk = analysis["risk_level"]
        score = analysis["legitimacy_score"]
        bar = self._risk_bar(score)
        action = analysis.get("action_label", "WATCHLIST")
        print(" EXECUTIVE DECISION")
        print(f"   ACTION:       {action}")
        print(f"   RISK:         {risk}")
        print(f"   MODEL SCORE:  {score}/100  {bar}")
        print(f"   CONFIDENCE:   {analysis.get('confidence_grade', 'Unknown')}")
        print(f"   DATA COVERAGE: {analysis.get('confidence', 'Unknown')}")
        model_risk = analysis.get("model_risk_level")
        if model_risk and model_risk != risk:
            print(f"   OVERRIDE:     Weighted model={model_risk}; hard-stop result={risk}")
        print()

        hard_stops = analysis.get("hard_stop_overrides", [])
        if hard_stops:
            print(" HARD-STOP RISKS")
            for stop in hard_stops:
                print(f"   [{stop['severity'].upper()}] {stop['reason']}")
            print()

        print(" INVESTOR TAKE")
        print(f"   {analysis.get('recommendation', '')}")
        print()

        # Component scores
        print(" TECHNICAL SCORECARD (12-factor weighted model)")
        weights = analysis.get("weights", {})
        for name, value in analysis.get("component_scores", {}).items():
            if name in ("legitimacy", "overall_rug_risk"):
                continue
            w = weights.get(name, 0)
            bar = self._risk_bar(value)
            uncertain_mark = " [?]" if name in analysis.get("uncertain_components", {}) else ""
            print(f"   {name:<25} {value:>5.1f}  {bar}  (weight: {w*100:.0f}%){uncertain_mark}")
        print()

        # Uncertain components (data gaps — NOT scored as safe)
        uncertain = analysis.get("uncertain_components", {})
        if uncertain:
            print(f" Uncertain Components ({len(uncertain)} — scored neutrally, not as safe):")
            for comp, reason in uncertain.items():
                print(f"   {comp}: {reason}")
            print()

        # Market data
        liq = dex.get("total_liquidity_usd", 0)
        vol = dex.get("total_volume_24h", 0)
        txns = dex.get("txns_24h", {})
        print(f" Market Data:")
        print(f"   Liquidity (all pairs):  ${liq:,.0f}")
        print(f"   24h Volume:             ${vol:,.0f}")
        print(f"   24h Buys/Sells:        {txns.get('buys', '?')}/{txns.get('sells', '?')}")
        pc = dex.get("price_change_24h")
        if pc is not None:
            arrow = "+" if pc >= 0 else ""
            print(f"   24h Price Change:       {arrow}{pc:.1f}%")
        print(f"   DEX Pairs:              {dex.get('pair_count', 0)}")
        boosts_active = dex.get("boosts_active", 0)
        if boosts_active and boosts_active > 0:
            print(f"   DexScreener Boosts:     ACTIVE ({boosts_active} pts)")
        else:
            print(f"   DexScreener Boosts:     None")
        print(
            f"   Primary AMM:            "
            f"{str(dex.get('primary_amm_version', 'unknown')).upper()}"
        )
        print(
            f"   Liquidity Custody:      "
            f"{str(lp_lock.get('state', 'custody_unverified')).upper()}"
        )
        print()

        # Security details
        print(f" Security (GoPlus):")
        if gp:
            buy_tax = _safe_float(gp.get("buy_tax")) * 100
            sell_tax = _safe_float(gp.get("sell_tax")) * 100
            print(f"   Buy Tax:    {buy_tax:.1f}%")
            print(f"   Sell Tax:   {sell_tax:.1f}%")
            print(f"   Honeypot:   {'YES' if _safe_int(gp.get('is_honeypot')) == 1 else 'No'}")
            print(f"   Can Buy:    {'No' if _safe_int(gp.get('cannot_buy')) == 1 else 'Yes'}")
            print(f"   Can Sell:   {'Blocked' if _safe_int(gp.get('cannot_sell_all')) == 1 else 'Yes'}")
            print(f"   Mintable:   {'Yes' if _safe_int(gp.get('is_mintable')) == 1 else 'No'}")
            print(f"   Proxy:      {'Yes' if _safe_int(gp.get('is_proxy')) == 1 else 'No'}")
            print(f"   Open Source:{'Yes' if _safe_int(gp.get('is_open_source')) == 1 else 'No'}")
            owner_addr = gp.get("owner_address", "")
            creator_addr = gp.get("creator_address", "")
            if owner_addr:
                print(f"   Owner:      {owner_addr}")
            if creator_addr:
                print(f"   Creator:    {creator_addr}")
            holder_ct = _safe_int(gp.get("holder_count", 0))
            if holder_ct:
                print(f"   Holders:    {holder_ct:,}")
        else:
            print(f"   (GoPlus data unavailable -- using bytecode analysis)")
            print(f"   Ownership:   {'Renounced' if contract.get('ownership_renounced') else 'ACTIVE'}")
            print(f"   Mint:        {'Yes' if contract.get('has_mint_function') else 'No'}")
        print()

        holder_assessment = analysis.get("holder_assessment") or {}
        if holder_assessment:
            print(" Holder Adoption Calibration:")
            print(
                f"   Observed: {holder_assessment.get('holder_count', 0):,} "
                f"via {holder_assessment.get('source', 'unknown')}"
            )
            print(
                f"   Age cohort: {holder_assessment.get('cohort', 'unknown')} "
                f"(benchmark {holder_assessment.get('target_holders', 0):,})"
            )
            print(
                f"   Adoption ratio: "
                f"{holder_assessment.get('cohort_ratio', 0) * 100:.1f}%"
            )
            print()

        # On-chain reserves (if available)
        reserves = dex.get("on_chain_reserves")
        if reserves:
            print(f" On-Chain Reserves (primary pair):")
            print(f"   Pair: {reserves.get('pair_address', '?')}")
            print(f"   ETH in pool: {reserves.get('reserve_eth', 0):.4f}")
        print()

        # Liquidity custody verification
        if lp_lock.get("pair_address"):
            print(f" Liquidity Custody Verification:")
            print(f"   Pair / pool id: {lp_lock.get('pair_address', '?')}")
            print(f"   AMM version: {str(lp_lock.get('amm_version', 'unknown')).upper()}")
            print(f"   State: {str(lp_lock.get('state', 'custody_unverified')).upper()}")
            print(f"   Status: {lp_lock.get('method', 'Unknown')}")
            if "burn_pct" in lp_lock:
                print(f"   LP burned: {lp_lock.get('burn_pct', 0):.1f}%")
            timelock = lp_lock.get("timelock") or {}
            if timelock.get("detected"):
                print(
                    f"   Timelock: {timelock.get('platform', 'Unknown')} "
                    f"({timelock.get('locked_pct', 0):.1f}%)"
                )
        print()

        # Blockscout holders (if available)
        if bs_holders.get("blockscout_available"):
            holders = bs_holders.get("holders", [])
            excluded = bs_holders.get("pair_contracts_excluded", [])
            print(
                f" Top Holders (Blockscout, {len(holders)} loaded, "
                f"{len(excluded)} verified AMM addresses excluded):"
            )
            if bs_holders.get("adj_top_1_pct") is not None:
                print(
                    f"   Top 1 non-AMM holder: "
                    f"{bs_holders['adj_top_1_pct']}% of supply"
                )
                print(
                    f"   Top 10 non-AMM holders: "
                    f"{bs_holders.get('adj_top_10_pct', '?')}% of supply"
                )
            else:
                print(f"   Top 1 concentration: {bs_holders.get('top_1_pct', '?')}%")
                print(f"   Top 10 concentration: {bs_holders.get('top_10_pct', '?')}%")
            print(
                f"   Concentration basis: "
                f"{bs_holders.get('concentration_basis', 'legacy')}"
            )
            print(f"   Whale count (>5%): {bs_holders.get('whale_count', 0)}")
            # Holder enrichment summary
            eoa_ct = bs_holders.get("eoa_holders_count", 0)
            contract_ct = bs_holders.get("contract_holders_count", 0)
            scam_ct = len(bs_holders.get("scam_flagged_holders", []))
            proxy_ct = bs_holders.get("proxy_holder_count", 0)
            print(f"   EOA/Contract split: {eoa_ct}/{contract_ct}")
            if scam_ct > 0:
                print(f"   Scam-flagged holders: {scam_ct} (!)")
            proxy_ct = bs_holders.get("proxy_holder_count", 0)
            eip7702_ct = bs_holders.get("eip7702_count", 0)
            if proxy_ct > 0:
                print(f"   Suspicious proxies (eip1167/eip1967): {proxy_ct}")
            if eip7702_ct > 0:
                print(f"   EIP-7702 accounts: {eip7702_ct} (normal AA)")
            for i, h in enumerate(holders[:10]):
                addr = h.get("address", "?")[:12]
                bal = h.get("balance_parsed", 0)
                is_contract = " [contract]" if h.get("is_contract") else ""
                name = f" ({h.get('name')})" if h.get("name") else ""
                denominator = bs_holders.get(
                    "concentration_denominator_raw",
                    bs_holders.get("total_parsed", 0),
                )
                if denominator > 0:
                    pct = bal / denominator * 100
                    print(f"   {i+1:>2}. {addr}...{is_contract}{name} -- {pct:.2f}%")
        print()

        # Deployer info
        if deployer.get("creator_address"):
            print(f" Deployer:")
            print(f"   Address: {deployer['creator_address']}")
            if deployer.get("creation_tx_hash"):
                print(f"   Creation TX: {EXPLORER}/tx/{deployer['creation_tx_hash']}")
            if deployer.get("is_serial_deployer"):
                print(f"   Serial deployer: YES ({deployer.get('total_deployer_creations')}+ contracts)")
            else:
                print(f"   Serial deployer: No")
            if bs_addr.get("is_verified"):
                print(f"   Source verified: Yes")
            if bs_addr.get("is_scam"):
                print(f"   Scam flagged: YES")
            if deployer.get("deployer_is_scam"):
                print(f"   Deployer address scam: YES")
            rep = deployer.get("deployer_reputation", "unknown")
            if rep != "unknown":
                print(f"   Deployer reputation: {rep}")
            tags = deployer.get("deployer_public_tags", [])
            if tags:
                print(f"   Public tags: {', '.join(tags)}")
        print()

        # Self-estimated tax
        if tax_est.get("available"):
            print(f" Tax Estimation (reserve-based cross-check):")
            bt = tax_est.get("buy_tax_estimate", 0)
            st = tax_est.get("sell_tax_estimate", 0)
            print(f"   Estimated buy tax:  {bt:.1f}%")
            print(f"   Estimated sell tax: {st:.1f}%")
            print()

        # Wash trading
        if wash.get("available"):
            ws = wash.get("wash_score", 0)
            wr = wash.get("wash_risk", "Low")
            print(f" Wash Trading Detection (3-window stabilization):")
            print(f"   Risk: {wr} (score: {ws})")
            wash_stable = wash.get("wash_stable", False)
            wash_std = wash.get("wash_score_std", 0)
            print(f"   Stable: {wash_stable} (std: {wash_std:.1f})")
            window_scores = wash.get("wash_score_windows", [])
            if window_scores:
                score_strs = [f"{w['score']:.0f}" if isinstance(w, dict) else f"{w:.0f}" for w in window_scores]
                print(f"   Window scores (500/1k/1.5k blocks): {', '.join(score_strs)}")
            if wash.get("ping_pong_pairs"):
                print(f"   Ping-pong pairs: {len(wash['ping_pong_pairs'])}")
            if wash.get("same_block_coordination"):
                print(f"   Coordinated blocks: {len(wash['same_block_coordination'])}")
            if wash.get("circular_transfers"):
                print(f"   Circular transfers: {len(wash['circular_transfers'])}")
        print()

        # Trend history (Timechain + M51 metric shifts)
        trend = data.get("trend", {})
        if trend.get("available"):
            print(f" Historical Trend (Timechain, {trend.get('past_count', 0)} prior analyses):")
            direction = trend.get("direction", "stable")
            print(f"   Direction: {direction}")
            delta = trend.get("delta", 0)
            print(f"   Score delta: {delta:+.1f}")
            score_range = trend.get("score_range", "?")
            print(f"   Score range: {score_range}")
            first_score = trend.get("first_score")
            last_score = trend.get("last_score")
            if first_score is not None and last_score is not None:
                print(f"   Trajectory: {first_score:.1f} -> {last_score:.1f}")
            metric_shifts = trend.get("metric_shifts", [])
            if metric_shifts:
                print(f"   Metric-level shifts:")
                for m in metric_shifts:
                    arrow = "+" if m["delta"] > 0 else ""
                    print(f"     {m['metric']:<25} {m['first']:>5.0f} -> {m['last']:>5.0f} ({arrow}{m['delta']:.0f})")
        print()

        # Flags
        for label, icon in [("green", "+"), ("yellow", "~"), ("red", "-")]:
            flist = analysis.get(f"{label}_flags", [])
            if flist:
                cap = label.capitalize()
                print(f" {cap} Flags ({len(flist)}):")
                for f in flist:
                    print(f"   {icon} {f}")
                print()

        # Provenance summary
        provenance = report.get("provenance", {})
        if provenance:
            print(f" Provenance ({provenance.get('fact_count', 0)} facts, pinned to block {provenance.get('block_pin', '?')}):")
            facts = provenance.get("facts", [])
            rpc_count = sum(1 for f in facts if f.get("source") == "rpc")
            http_count = sum(1 for f in facts if f.get("source") == "http")
            print(f"   RPC queries: {rpc_count} | HTTP queries: {http_count}")
            print(f"   Each fact cites its query + SHA-256 hash of the response.")
            print(f"   RPC reads use block {provenance.get('block_pin', '?')}; mutable HTTP responses are content-addressed locally.")
        print()

        print("=" * 72)
        print(f" RECOMMENDATION: {analysis['recommendation']}")
        print("=" * 72)

    @staticmethod
    def _risk_bar(score: float) -> str:
        if score >= 80:
            filled = int(score / 100 * 20)
            return "[" + "=" * filled + "-" * (20 - filled) + "] STRONG"
        elif score >= 60:
            return "[" + "=" * 12 + "~" * 8 + "] MODERATE"
        elif score >= 40:
            return "[" + "=" * 8 + "!" * 12 + "] WEAK"
        else:
            return "[" + "X" * 20 + "] DANGER"


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    ensure_utf8_runtime()

    if len(sys.argv) < 2:
        print("Chainseer v7.1 — Robinhood Chain On-Chain Intelligence")
        print()
        print("Usage: py chainseer.py <token_address> [--full] [rpc_url]")
        print()
        print("Options:")
        print("  --full    Show detailed 12-factor report (default: investor summary)")
        print()
        print("Examples:")
        print("  py chainseer.py 0x407470F85e0b342a52AaE2F191E135cEF2947777")
        print("  py chainseer.py 0x407470F85e0b342a52AaE2F191E135cEF2947777 --full")
        print()
        print("Robinhood Chain defaults:")
        print(f"  RPC:      {RPC_URL}")
        print(f"  Chain ID: {CHAIN_ID}")
        print(f"  Explorer: {EXPLORER}")
        print(f"  WETH:     {WETH_ADDRESS}")
        sys.exit(1)

    # Parse args: first positional = address, --full flag, optional rpc_url
    args = sys.argv[1:]
    full_report = "--full" in args
    args = [a for a in args if a != "--full"]
    token_address = args[0]
    rpc_url = args[1] if len(args) > 1 else RPC_URL

    agent = Chainseer(rpc_url=rpc_url)
    result = agent.analyze_token(token_address, full_report=full_report)

    out_file = f"chainseer_report_{token_address[:10]}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n Full report saved to: {out_file}")


if __name__ == "__main__":
    main()
