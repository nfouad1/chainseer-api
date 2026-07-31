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

BOT_TOKEN = os.environ.get("CHAINSEER_BOT_TOKEN", "")

# Rate limiting: 1 analysis per user per 60 seconds
USER_RATE_LIMIT = 60
user_last_analysis = defaultdict(float)
inflight_users = set()

ADDR_RE = re.compile(r"0x[a-fA-F0-9]{40}")

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


# ── Commands ─────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🔍 *Chainseer — Robinhood Chain Analysis*\n\n"
        "Paste any token contract address (0x...) and I'll give you an "
        "investor-grade risk verdict in seconds.\n\n"
        "*Commands:*\n"
        "`<address>` — quick verdict\n"
        "`/full <address>` — detailed report\n"
        "`/help` — usage help\n"
        "`/about` — how it works\n\n"
        "Every analysis is provenance-tracked and sealed to the Timechain."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*Usage:*\n\n"
        "1. Paste a token address: `0x407470F85e0b342a52AaE2F191E135cEF2947777`\n"
        "2. Get a verdict: ✅ LOW / ⚠️ MEDIUM / 🔴 HIGH\n\n"
        "Use `/full <address>` for the full 12-factor breakdown.\n\n"
        "Tip: Copy addresses directly from DexScreener or the explorer."
    )


async def cmd_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*About Chainseer*\n\n"
        "Chainseer analyzes Robinhood Chain tokens using multiple sources:\n"
        "• GoPlus Security API\n"
        "• DexScreener\n"
        "• Blockscout\n"
        "• Direct RPC calls\n\n"
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


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    addr = extract_address(text)
    if addr:
        await send_analysis(update, addr, full=False)
    elif not text.startswith("/"):
        await update.message.reply_text(
            "📋 Paste a token contract address (0x...) to analyze it.\n"
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🚀 Chainseer Telegram Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
