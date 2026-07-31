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

import os
import re
import sys
import logging

# ── Force UTF-8 stdout (Windows defaults to cp1252 which corrupts Unicode) ──
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, Exception):
        pass

# ── Telegram dependency ──────────────────────────────────────────────────────
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
    print("Missing dependency. Install with:  pip install python-telegram-bot")
    sys.exit(1)

# ── Chainseer engine ─────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from chainseer import Chainseer, RPC_URL

# ── Config ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("CHAINSEER_BOT_TOKEN", "")

# Address regex: 0x + 40 hex chars (case-insensitive)
ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("chainseer_bot")

# Shared Chainseer instance (Timechain persists across messages)
_agent = None


def get_agent() -> Chainseer:
    global _agent
    if _agent is None:
        _agent = Chainseer(rpc_url=RPC_URL)
    return _agent


# ── Helpers ──────────────────────────────────────────────────────────────────

def extract_address(text: str) -> str | None:
    """Extract the first valid-looking 0x address from text."""
    m = ADDR_RE.search(text)
    return m.group(0) if m else None


def escape_md(text: str) -> str:
    """Escape MarkdownV2 special characters (used for Telegram messages)."""
    # For simplicity we use plain text (HTML parse mode) instead of MarkdownV2
    # to avoid escaping headaches with token addresses and special chars.
    return text


async def send_analysis(update: Update, token: str, full: bool = False):
    """Run Chainseer analysis and send the result."""
    chat_id = update.effective_chat.id

    # Acknowledge — analysis takes ~15-30s
    await update.message.reply_text(
        f"🔍 Analyzing `{token[:10]}...{token[-6:]}` on Robinhood Chain...\n"
        f"This takes ~20 seconds (12-factor analysis, provenance-tracked)."
    )

    try:
        agent = get_agent()
        # analyze_token prints to stdout during phases; we capture the report
        # and format it for Telegram. Suppress stdout to keep bot logs clean.
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            report = agent.analyze_token(token, full_report=False)

        if "error" in report:
            await update.message.reply_text(f"❌ {report['error']}")
            return

        # Send summary
        summary = agent._format_summary(report, fmt="telegram")
        await update.message.reply_text(summary)

        # If full requested, send component scores + flags
        if full:
            analysis = report["analysis"]
            lines = ["\n📋 *DETAILED COMPONENT SCORES:*"]
            weights = analysis.get("weights", {})
            for name, val in analysis.get("component_scores", {}).items():
                if name in ("legitimacy", "overall_rug_risk"):
                    continue
                w = weights.get(name, 0)
                uncertain = " [?]" if name in analysis.get("uncertain_components", {}) else ""
                lines.append(f"  {name}: {val:.0f}/100 (w:{w*100:.0f}%){uncertain}")
            await update.message.reply_text("\n".join(lines))

            # All flags
            for label, icon in [("green", "✓"), ("yellow", "⚠"), ("red", "✗")]:
                flist = analysis.get(f"{label}_flags", [])
                if flist:
                    cap = label.upper()
                    lines = [f"\n{cap} FLAGS ({len(flist)}):"]
                    for f in flist:
                        lines.append(f"  {icon} {f}")
                    await update.message.reply_text("\n".join(lines))

    except Exception as e:
        logger.exception("Analysis failed")
        await update.message.reply_text(
            f"❌ Analysis failed: {str(e)[:200]}\n"
            f"The RPC or an API may be temporarily unavailable. Try again."
        )


# ── Command handlers ─────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔍 *Chainseer — Robinhood Chain Analysis*\n\n"
        "Paste any token contract address (0x...) and I'll give you an "
        "investor-grade risk verdict in seconds.\n\n"
        "*What I check:*\n"
        "• Honeypot & buy/sell tax (GoPlus)\n"
        "• LP lock / burn verification\n"
        "• Holder concentration (whale detection)\n"
        "• Wash trading patterns (3-window)\n"
        "• Deployer history & rug risk\n"
        "• Liquidity, volume, age\n"
        "• 12-factor weighted scoring\n\n"
        "*Commands:*\n"
        "`<address>` — quick verdict\n"
        "`/full <address>` — detailed report\n"
        "`/about` — how it works\n\n"
        "Every analysis is provenance-tracked: each claim cites its exact "
        "query and response hash, pinned to a specific block.",
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*Usage:*\n\n"
        "1. Paste a token address: `0x407470F85e0b342a52AaE2F191E135cEF2947777`\n"
        "2. Get a verdict: ✅ LOW / ⚠️ MEDIUM / 🔴 HIGH / 🚨 CRITICAL\n"
        "3. Use `/full <address>` for the detailed 12-factor breakdown\n\n"
        "*Tips:*\n"
        "• Copy any address from Robinhood Chain explorer or DexScreener\n"
        "• Analysis takes ~20 seconds (multi-source verification)\n"
        "• Results are reproducible — pinned to a specific block",
    )


async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*About Chainseer*\n\n"
        "Chainseer is an on-chain intelligence agent for Robinhood Chain "
        "(chain ID 4663). It fuses 4 data sources:\n\n"
        "• *GoPlus Security API* — honeypot, tax, holder analysis\n"
        "• *DexScreener* — price, volume, liquidity, market data\n"
        "• *Blockscout* — deployer, holders, contract verification\n"
        "• *Direct RPC* — reserves, wash trading, timelock probing\n\n"
        "*Provenance:* every number in the report cites the exact query "
        "(RPC method or HTTP endpoint) and a SHA-256 hash of the response. "
        "Scans are pinned to a block number — re-running at the same block "
        "produces byte-identical results.\n\n"
        "*Memory:* every analysis is sealed to a Cypher Tempe Timechain — "
        "a tamper-evident, hash-chained ledger. Trend analysis uses this "
        "history to detect score trajectories over time.\n\n"
        "12-factor weighted model: security, honeypot safety, liquidity, "
        "LP lock, holder distribution, volume, maturity, creator risk, "
        "wash trading, deployer risk, sentiment, trend.",
    )


async def cmd_full(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /full <address> — detailed report."""
    text = update.message.text or ""
    # Remove the /full command
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text(
            "Usage: `/full 0x407470F85e0b342a52AaE2F191E135cEF2947777`"
        )
        return
    addr = extract_address(parts[1])
    if not addr:
        await update.message.reply_text("❌ No valid address found. Paste a 0x... address.")
        return
    await send_analysis(update, addr, full=True)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle any message — extract address and analyze."""
    text = update.message.text or ""
    addr = extract_address(text)
    if addr:
        await send_analysis(update, addr, full=False)
    elif not text.startswith("/"):
        await update.message.reply_text(
            "📋 Paste a token contract address (0x...) to analyze it.\n"
            "Example: `0x407470F85e0b342a52AaE2F191E135cEF2947777`\n\n"
            "Use /help for more options."
        )


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        print("=" * 60)
        print(" CHAINSEER TELEGRAM BOT")
        print("=" * 60)
        print()
        print(" No bot token found. Set it via environment variable:")
        print()
        print("   set CHAINSEER_BOT_TOKEN=<your_token_from_botfather>")
        print()
        print(" Or edit BOT_TOKEN in this file.")
        print()
        print(" To create a bot:")
        print("   1. Open @BotFather in Telegram")
        print("   2. Send /newbot")
        print("   3. Follow the prompts to get your token")
        print("=" * 60)
        sys.exit(1)

    print("=" * 60)
    print(" CHAINSEER TELEGRAM BOT — starting...")
    print("=" * 60)

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("about", cmd_about))
    app.add_handler(CommandHandler("full", cmd_full))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print(" Bot is running. Press Ctrl+C to stop.")
    print()
    app.run_polling()


if __name__ == "__main__":
    main()
