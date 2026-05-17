#!/usr/bin/env python3
"""
DerivBot Pro — Phase 1 (CLEAN DATA ENGINE)
==========================================
Purpose:
- Collect OHLC data from Deriv
- Build multi-timeframe structure memory
- Detect raw market events ONLY (NO alerts, NO decisions)
- Prepare data for Phase 2 & 3 engines
"""

import asyncio
import json
import logging
import os
import websockets
from collections import deque
from dataclasses import dataclass
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "PASTE_TOKEN")
DERIV_WS = "wss://ws.binaryws.com/websockets/v3?app_id=1089"

WATCHLIST = {
    "frxEURUSD": "EURUSD",
    "frxGBPUSD": "GBPUSD",
    "frxUSDJPY": "USDJPY",
    "frxXAUUSD": "GOLD",
    "R_75": "Volatility 75",
}

TIMEFRAMES = {
    "M15": 900,
    "H1": 3600,
    "H4": 14400,
    "D1": 86400,
}

# ============================================================
# DATA STRUCTURES (RAW ONLY)
# ============================================================

@dataclass
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float

@dataclass
class MarketEvent:
    symbol: str
    tf: str
    type: str
    price: float
    time: int

# ============================================================
# STORAGE ENGINE (NO SIGNAL LOGIC)
# ============================================================

class MarketState:
    def __init__(self):
        self.candles = {
            sym: {tf: deque(maxlen=300) for tf in TIMEFRAMES}
            for sym in WATCHLIST
        }

        self.events = {
            sym: {tf: deque(maxlen=200) for tf in TIMEFRAMES}
            for sym in WATCHLIST
        }

state = MarketState()

# ============================================================
# DERIV DATA STREAM
# ============================================================

async def stream(symbol, tf):
    granularity = TIMEFRAMES[tf]

    while True:
        try:
            async with websockets.connect(DERIV_WS) as ws:
                await ws.send(json.dumps({
                    "ticks_history": symbol,
                    "granularity": granularity,
                    "count": 50,
                    "end": "latest",
                    "style": "candles",
                    "subscribe": 1
                }))

                log.info(f"Connected: {symbol} {tf}")

                while True:
                    msg = json.loads(await ws.recv())

                    if "ohlc" in msg:
                        o = msg["ohlc"]

                        candle = Candle(
                            time=int(o["open_time"]),
                            open=float(o["open"]),
                            high=float(o["high"]),
                            low=float(o["low"]),
                            close=float(o["close"])
                        )

                        # store raw candle only
                        state.candles[symbol][tf].append(candle)

                        # OPTIONAL: raw event logging (NOT signal)
                        last_10 = list(state.candles[symbol][tf])[-10:]

                        # raw structure events only (NO alerts)
                        if len(last_10) > 5:
                            highest = max(c.high for c in last_10[:-1])
                            lowest = min(c.low for c in last_10[:-1])

                            if candle.high > highest:
                                state.events[symbol][tf].append(
                                    MarketEvent(symbol, tf, "RAW_HIGH_BREAK",
                                                candle.high, candle.time)
                                )

                            if candle.low < lowest:
                                state.events[symbol][tf].append(
                                    MarketEvent(symbol, tf, "RAW_LOW_BREAK",
                                                candle.low, candle.time)
                                )

        except Exception as e:
            log.error(f"Reconnect {symbol} {tf}: {e}")
            await asyncio.sleep(3)

# ============================================================
# ENGINE LAUNCHER
# ============================================================

async def start_engine():
    tasks = []
    for sym in WATCHLIST:
        for tf in TIMEFRAMES:
            tasks.append(stream(sym, tf))
    await asyncio.gather(*tasks)

# ============================================================
# TELEGRAM (MINIMAL CONTROL ONLY)
# ============================================================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "✅ Phase 1 Running\n"
        "📊 Data engine active\n"
        "❌ No alerts in Phase 1\n"
        "Next: Phase 2 (Confluence Layer)"
    )

def main():
    print("🚀 BOT BOOTING - IF YOU SEE THIS, CODE IS RUNNING")
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    loop = asyncio.get_event_loop()
    loop.create_task(start_engine())

    app.run_polling()

if __name__ == "__main__":
    main()
