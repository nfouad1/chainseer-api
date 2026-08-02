"""
Chainseer Telegram Bot — Robinhood Chain on-chain analysis on demand.

Paste a token address, get an investor-grade risk verdict in seconds.
Uses Chainseer's provenance-tracked 12-factor analysis engine.

Setup:
    1. Create a bot via @BotFather on Telegram -> get BOT_TOKEN
    2. pip install python-telegram-bot
    3. Set token:  set CHAINSEER_BOT_TOKEN=<your_token>   (or edit below)
    4. Run:        py chainseer_bot.py

Commands:
    /start        — welcome + instructions
    /help         — usage help
    <address>     — analyze a token (0x... address)
    /full <addr>  — detailed 12-factor report (longer message)
    /about        — what Chainseer checks and how

Every analysis is:
    - Pinned to a specific block (reproducible)
    - Provenance-tracked (every claim cites its query + response hash)
    - Sealed to the Cypher Tempe Timechain (verifiable memory)
"""

"""
Chainseer Telegram Bot — Robinhood Chain on-chain analysis on demand.
Updated with UTF-8 fix for Windows and better error handling.
"""

import os
import re
import sys
import asyncio
import logging
import time
from collections import defaultdict
import locale

# Force UTF-8 globally (critical for Windows + Cypher Tempre)
#sys.stdout.reconfigure(encoding="utf-8")
#sys.stderr.reconfigure(encoding="utf-8")
#locale.setlocale(locale.LC_ALL, 'C.UTF-8')

# ── Force UTF-8 for Windows (critical for Cypher Tempre) ─────────────────────
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    from telegram import Update
    from telegram.ext import (
        ApplicationBuilder,
        CommandHandler,
        MessageHandler,
        ContextTypes,
        filters,
    )
except ImportError:
    print("Missing dependency. Run: pip install python-telegram-bot")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chainseer import Chainseer, ensure_utf8_runtime
import chainseer_solana
from chainseer_solana_public import SolanaPublicAnalyzer, SolanaMintError, validate_solana_mint

# Uses chainseer_solana's env lookup (process env, falling back to the
# persistent Windows user registry) rather than a bare os.environ.get().
# `setx` only writes the registry -- it does not retroactively appear in
# any shell/process that was already running before the setx call, so a
# plain os.environ.get() here would silently miss a value that was in fact
# set correctly, just not in this process's inherited environment.
BOT_TOKEN = chainseer_solana._environment_setting("CHAINSEER_BOT_TOKEN") or ""
# Same defaults chainseer_solana.py's own CLI uses -- if the autotrader and
# this bot run from the same working directory (the normal single-operator
# setup), Solana lookups transparently share its catalog and get a richer,
# already-computed answer for anything it has already discovered.
SOLANA_ROOT = os.environ.get("CHAINSEER_SOLANA_BOT_ROOT", "solana_learning")
SOLANA_CHAIN_ROOT = os.environ.get("CHAINSEER_SOLANA_BOT_CHAIN_ROOT", "solana_chain")
# The operator's own chat -- reflection-checkpoint pushes go here, and only
# this chat is allowed to run /reflection or /ack. Unset means those two
# commands are unreachable for everyone (fail closed), not merely hidden.
OWNER_CHAT_ID = (chainseer_solana._environment_setting("CHAINSEER_TELEGRAM_CHAT_ID") or "").strip()

# Rate limiting: 1 analysis per user per 60 seconds
USER_RATE_LIMIT = 60
user_last_analysis = defaultdict(float)
inflight_users = set()

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")
SOLANA_ADDR_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("chainseer_bot")

_agent = None
_analysis_lock = asyncio.Lock()

def get_agent() -> Chainseer:
    global _agent
    if _agent is None:
        import sys
        import locale
        
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass
            
        # Try to set a UTF-8 locale, but fall back to system default if it fails (like on Windows)
        try:
            locale.setlocale(locale.LC_ALL, 'C.UTF-8')
        except locale.Error:
            try:
                # Fallback to the system's default locale
                locale.setlocale(locale.LC_ALL, '')
            except locale.Error:
                pass # If it still fails, let Python use its default
                
        _agent = Chainseer()
    return _agent

def extract_address(text: str) -> str | None:
    m = ADDR_RE.search(text)
    return m.group(0) if m else None


def extract_solana_mint(text: str) -> str | None:
    """First base58-shaped candidate in text that actually decodes to a
    32-byte pubkey. The shape regex alone accepts strings that decode to the
    wrong byte length (not every base58-alphabet string of the right length
    is a real pubkey encoding), so each candidate is validated for real."""
    for match in SOLANA_ADDR_RE.finditer(text):
        try:
            return validate_solana_mint(match.group(0))
        except SolanaMintError:
            continue
    return None


def _is_owner(update: Update) -> bool:
    return bool(OWNER_CHAT_ID) and str(update.effective_chat.id) == OWNER_CHAT_ID


_solana_engine = None
_solana_public_analyzer = None
_solana_lock = asyncio.Lock()


def get_solana_engine() -> "chainseer_solana.SolanaPrototypeEngine":
    """The SAME engine class the standalone autotrader uses, constructed
    read-only-in-intent here: the bot only ever calls evaluate_candidate()
    with shadow_enter=False, so it never opens/closes a paper position or
    touches the live-execution boundary (which is hard-disabled anyway)."""
    global _solana_engine
    if _solana_engine is None:
        _solana_engine = chainseer_solana.SolanaPrototypeEngine(
            root=SOLANA_ROOT, chain_root=SOLANA_CHAIN_ROOT,
        )
    return _solana_engine


def get_solana_public_analyzer() -> SolanaPublicAnalyzer:
    """Fallback for a mint that isn't a Pump.fun launch (or is too old/deep
    in its own signature history for resolve_candidate's bounded on-demand
    scan to recover) -- handles any SPL mint, just without deployer/creator
    history since that requires verified Pump.fun launch provenance."""
    global _solana_public_analyzer
    if _solana_public_analyzer is None:
        _solana_public_analyzer = SolanaPublicAnalyzer(chainseer_solana.SOLANA_RPC_URL)
    return _solana_public_analyzer


def _format_solana_deep_summary(mint: str, result: dict) -> str:
    decision = result["decision"]
    icon = {"Low": "✅", "Medium": "⚠️", "High": "\U0001f534"}.get(
        decision["risk_level"], "❓"
    )
    lines = [
        f"{icon} *{decision['risk_level'].upper()}* -- score {decision['score']}/100",
        f"Pump.fun launch, verified on-chain. `{mint[:6]}...{mint[-6:]}`",
        f"Evidence: {decision['evidence_state']} | Admission: {decision['admission_state']}",
    ]
    if decision["hard_stops"]:
        lines.append("\n\U0001f6d1 Hard stops:")
        lines.extend(f"  - {item}" for item in decision["hard_stops"])
    if decision["warnings"]:
        lines.append("\n⚠️ Warnings:")
        lines.extend(f"  - {item}" for item in decision["warnings"])
    if not decision["hard_stops"] and not decision["warnings"]:
        lines.append("\nNo hard stops or warnings raised.")
    lines.append(
        "\nIncludes deployer/creator deployment-cadence history and wallet "
        "convergence -- Chainseer's Pump.fun-specific evidence, not just a "
        "generic SPL mint check."
    )
    return "\n".join(lines)


def _format_solana_public_summary(mint: str, result: dict) -> str:
    analysis = result["analysis"]
    red_flags = analysis.get("red_flags") or []
    yellow_flags = analysis.get("yellow_flags") or []
    risk_level = analysis.get("risk_level", "Unknown")
    icon = {"Low": "✅", "Medium": "⚠️", "High": "\U0001f534"}.get(risk_level, "❓")
    lines = [
        f"{icon} *{risk_level.upper()}* -- score {analysis.get('legitimacy_score')}/100 "
        "(general SPL mint check)",
        f"`{mint[:6]}...{mint[-6:]}`",
        "No verified Pump.fun launch provenance found for this mint -- "
        "deployer/creator history is unknown, not cleared.",
    ]
    if red_flags:
        lines.append("\n\U0001f6d1 Hard stops:")
        lines.extend(f"  - {item}" for item in red_flags)
    if yellow_flags:
        lines.append("\n⚠️ Warnings:")
        lines.extend(f"  - {item}" for item in yellow_flags)
    if not red_flags and not yellow_flags:
        lines.append("\nNo hard stops or warnings raised.")
    return "\n".join(lines)


def _run_solana_analysis(mint: str):
    """Run blocking RPC/Jupiter/DexScreener work outside the event loop."""
    engine = get_solana_engine()
    candidate = engine.observer.resolve_candidate(mint)
    if candidate is not None:
        result = engine.evaluate_candidate(candidate, shadow_enter=False)
        return "deep", _format_solana_deep_summary(mint, result)
    result = get_solana_public_analyzer().analyze_token(mint)
    return "public", _format_solana_public_summary(mint, result)


def _run_analysis(token: str, full: bool):
    """Run blocking network and Timechain work outside the event loop."""
    import io
    from contextlib import redirect_stdout

    agent = get_agent()
    buf = io.StringIO()
    with redirect_stdout(buf):
        report = agent.analyze_token(token, full_report=full)
    return agent, report, buf.getvalue()


def _message_chunks(text: str, limit: int = 3900):
    """Split plain-text reports below Telegram's message limit."""
    remaining = text.strip()
    while remaining:
        if len(remaining) <= limit:
            yield remaining
            return
        split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        yield remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()

async def send_analysis(update: Update, token: str, full: bool = False):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    # Rate limiting
    now = time.time()
    if user_id in inflight_users or now - user_last_analysis[user_id] < USER_RATE_LIMIT:
        await update.message.reply_text("⏳ Please wait a moment before requesting another analysis.")
        return
    inflight_users.add(user_id)

    await update.message.reply_text(
        f"🔍 Analyzing `{token[:10]}...{token[-6:]}` on Robinhood Chain...\n"
        "This takes ~15-30 seconds."
    )

    try:
        # Keep the event loop responsive while serializing writes to the shared
        # append-only Timechain and the singleton agent's scan context.
        async with _analysis_lock:
            agent, report, detailed_output = await asyncio.to_thread(
                _run_analysis, token, full
            )

        if "error" in report:
            await update.message.reply_text(f"❌ {report['error']}")
            return

        user_last_analysis[user_id] = time.time()

        # Plain text avoids unsafe Markdown interpolation from token metadata.
        summary = agent._format_summary(report, fmt="text")
        await update.message.reply_text(summary)

        if full:
            for chunk in _message_chunks(detailed_output):
                await update.message.reply_text(chunk)

    except Exception as e:
        logger.exception("Analysis failed")
        await update.message.reply_text(
            f"❌ Analysis failed: {str(e)[:150]}\n"
            "The RPC or an API may be temporarily unavailable. Try again later."
        )
    finally:
        inflight_users.discard(user_id)


async def send_solana_analysis(update: Update, mint: str):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    now = time.time()
    if user_id in inflight_users or now - user_last_analysis[user_id] < USER_RATE_LIMIT:
        await update.message.reply_text("⏳ Please wait a moment before requesting another analysis.")
        return
    inflight_users.add(user_id)

    await update.message.reply_text(
        f"🔍 Analyzing `{mint[:6]}...{mint[-6:]}` on Solana...\n"
        "This takes ~15-30 seconds."
    )

    try:
        async with _solana_lock:
            mode, summary = await asyncio.to_thread(_run_solana_analysis, mint)

        user_last_analysis[user_id] = time.time()
        await update.message.reply_text(summary)

    except SolanaMintError as e:
        await update.message.reply_text(f"❌ {e.message}")
    except chainseer_solana.ConfiguredSolanaRpcRequiredError:
        logger.exception("Solana RPC policy mismatch")
        await update.message.reply_text(
            "❌ Analysis temporarily unavailable (Solana RPC configuration). "
            "Try again later."
        )
    except Exception as e:
        logger.exception("Solana analysis failed")
        await update.message.reply_text(
            f"❌ Analysis failed: {str(e)[:150]}\n"
            "The RPC or an API may be temporarily unavailable. Try again later."
        )
    finally:
        inflight_users.discard(user_id)


# ── Commands ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🔍 *Chainseer — On-Chain Analysis*\n\n"
        "Paste a token contract address (0x... on Robinhood Chain, or a "
        "Solana mint) and I'll give you an investor-grade risk verdict in "
        "seconds.\n\n"
        "*Commands:*\n"
        "`<address>` — quick verdict\n"
        "`/full <0x address>` — detailed Robinhood Chain report\n"
        "`/help` — usage help\n"
        "`/about` — how it works\n\n"
        "Every analysis is provenance-tracked and sealed to the Timechain."
    )
    if _is_owner(update):
        text += (
            "\n\n*Owner commands:*\n"
            "`/reflection` — Solana learner's reflection-checkpoint status\n"
            "`/ack <applied|no_change> <summary>` — resume a paused learner"
        )
    await update.message.reply_text(text)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*Usage:*\n\n"
        "1. Paste a token address:\n"
        "   `0x407470F85e0b342a52AaE2F191E135cEF2947777` (Robinhood Chain)\n"
        "   or a Solana mint address\n"
        "2. Get a verdict: ✅ LOW / ⚠️ MEDIUM / 🔴 HIGH\n\n"
        "Use `/full <address>` for the full 12-factor Robinhood Chain "
        "breakdown.\n\n"
        "Tip: Copy addresses directly from DexScreener or the explorer."
    )


async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*About Chainseer*\n\n"
        "Robinhood Chain tokens are analyzed using multiple sources:\n"
        "• GoPlus Security API\n"
        "• DexScreener\n"
        "• Blockscout\n"
        "• Direct RPC calls\n\n"
        "Solana mints launched on Pump.fun get Chainseer's deeper "
        "verification: on-chain bonding-curve/mint decode, canonical-pool "
        "cross-check, deployer deployment-cadence history, and wallet "
        "convergence. Any other Solana mint falls back to a general SPL "
        "check (mint/freeze authority, liquidity, Jupiter route).\n\n"
        "Every claim is provenance-tracked (query + response hash) "
        "and sealed to the Cypher Tempre Timechain for verifiability."
    )


async def cmd_full(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Usage: `/full 0x...`")
        return
    addr = extract_address(parts[1])
    if not addr:
        await update.message.reply_text("❌ No valid 0x address found.")
        return
    await send_analysis(update, addr, full=True)


async def cmd_reflection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the Solana learner's reflection-checkpoint state -- same
    context a pause notification carries, available on demand so
    acknowledging is never done blind."""
    if not _is_owner(update):
        await update.message.reply_text("❌ This command is restricted to the bot owner.")
        return
    try:
        engine = await asyncio.to_thread(get_solana_engine)
        state = await asyncio.to_thread(engine.reflection_status)
    except Exception as e:
        logger.exception("reflection status failed")
        await update.message.reply_text(f"❌ {str(e)[:200]}")
        return

    if not state.get("pause_requested") and state.get("status") != "pending":
        lines = [
            "✅ No reflection checkpoint pending.",
            f"Analyses so far: {state.get('analysis_events')}",
            f"Next checkpoint in: {state.get('analyses_until_checkpoint')} analyses",
        ]
        if state.get("last_reflection_at"):
            lines.append(
                f"Last reflection ({state.get('last_reflection_outcome')}): "
                f"{state.get('last_reflection_summary')}"
            )
        await update.message.reply_text("\n".join(lines))
        return

    pending = state.get("pending_checkpoint") or {}
    text = await asyncio.to_thread(
        engine._reflection_notification_text, pending
    )
    await update.message.reply_text(text)


async def cmd_ack(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        await update.message.reply_text("❌ This command is restricted to the bot owner.")
        return
    text = update.message.text or ""
    parts = text.split(maxsplit=2)
    if len(parts) < 3 or parts[1] not in ("applied", "no_change"):
        await update.message.reply_text(
            "Usage: /ack <applied|no_change> <summary text>\n"
            "Run /reflection first if you need the details to write the summary."
        )
        return
    outcome, summary = parts[1], parts[2]
    try:
        engine = await asyncio.to_thread(get_solana_engine)
        await asyncio.to_thread(engine.acknowledge_reflection, outcome, summary)
    except ValueError as e:
        await update.message.reply_text(f"❌ {e}")
        return
    except Exception as e:
        logger.exception("reflection ack failed")
        await update.message.reply_text(f"❌ {str(e)[:200]}")
        return
    await update.message.reply_text(
        f"✅ Acknowledged ({outcome}). Learning resumes on the next cycle."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    addr = extract_address(text)
    if addr:
        await send_analysis(update, addr, full=False)
        return
    mint = extract_solana_mint(text)
    if mint:
        await send_solana_analysis(update, mint)
        return
    if not text.startswith("/"):
        await update.message.reply_text(
            "📋 Paste a token contract address (0x... or a Solana mint) to "
            "analyze it.\n"
            "Example: `0x407470F85e0b342a52AaE2F191E135cEF2947777`"
        )


def main():
    ensure_utf8_runtime()

    if not BOT_TOKEN:
        print("❌ No BOT_TOKEN found. Set CHAINSEER_BOT_TOKEN environment variable.")
        sys.exit(1)

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("about", cmd_about))
    app.add_handler(CommandHandler("full", cmd_full))
    app.add_handler(CommandHandler("reflection", cmd_reflection))
    app.add_handler(CommandHandler("ack", cmd_ack))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    if not OWNER_CHAT_ID:
        print(
            "⚠ CHAINSEER_TELEGRAM_CHAT_ID not set -- reflection-checkpoint "
            "push notifications and /reflection, /ack are disabled."
        )

    print("🚀 Chainseer Telegram Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
