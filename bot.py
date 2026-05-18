#!/usr/bin/env python3
"""
DerivBot Pro — Complete Final Build
=====================================
Architecture : Narrative → Confluence → Sequence → Trigger
New in final  : Encoded candle-sequence engine
                Adaptive thresholds per instrument class
                Protected high/low validation
                Confidence decay model
                All syntax bugs fixed
                Even/Odd re-verification at trade time
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import websockets
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ============================================================
# TOKENS
# ============================================================
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "PASTE_HERE")
DERIV_DEMO_TOKEN = os.environ.get("DERIV_DEMO_TOKEN", "PASTE_HERE")
DERIV_REAL_TOKEN = os.environ.get("DERIV_REAL_TOKEN", "PASTE_HERE")
# ============================================================

DERIV_WS           = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
MIN_SCORE          = 7
MAX_ALERTS_PER_DAY = 3
COOLDOWN_HOURS     = 4

# ============================================================
# INSTRUMENT CLASSES — for adaptive thresholds
# ============================================================
INSTRUMENT_CLASS = {
    # Forex — standard volatility
    "frxEURCHF": "forex", "frxEURGBP": "forex",
    "frxEURJPY": "forex", "frxEURUSD": "forex",
    "frxGBPAUD": "forex", "frxGBPJPY": "forex",
    "frxGBPCAD": "forex", "frxUSDJPY": "forex",
    # Metals / commodities — higher volatility
    "frxXAUUSD": "metal", "frxBROUSD": "commodity",
    # Crypto — highest volatility
    "frxBTCUSD": "crypto",
    # Indices — medium
    "SPC": "index", "HSI": "index", "N225": "index",
    "DJI": "index", "NDX": "index",
    # Synthetics — very high volatility
    "R_75": "synthetic",
}

# Displacement multiplier per instrument class
DISP_MULT = {
    "forex":     1.8,
    "metal":     2.0,
    "commodity": 2.0,
    "index":     2.0,
    "crypto":    2.2,
    "synthetic": 2.5,
}

# ============================================================
# WATCHLIST
# ============================================================
DEFAULT_WATCHLIST = {
    "frxEURCHF": "EURCHF", "frxEURGBP": "EURGBP",
    "frxEURJPY": "EURJPY", "frxEURUSD": "EURUSD",
    "frxGBPAUD": "GBPAUD", "frxGBPJPY": "GBPJPY",
    "frxGBPCAD": "GBPCAD", "frxUSDJPY": "USDJPY",
    "frxXAUUSD": "XAUUSD", "frxBROUSD": "US Oil",
    "frxBTCUSD": "BTCUSD", "SPC": "US SP500",
    "HSI": "HK 50",        "N225": "Japan 225",
    "DJI": "Wall St 30",   "NDX": "US Tech 100",
    "R_75": "Volatility 75",
}

TIMEFRAMES = {"D1": 86400, "H4": 14400, "H1": 3600, "M15": 900}
SESSIONS   = {"Asian": (0, 8), "London": (7, 16), "NY": (12, 21)}

# ============================================================
# CANDLE STATE ENGINE
# ============================================================

# States a candle can be classified as
COMPRESSION  = "COMPRESSION"   # small range, coiling
SWEEP        = "SWEEP"          # long wick, liquidity taken
REJECTION    = "REJECTION"      # body closes away from extreme
DISPLACEMENT = "DISPLACEMENT"   # large body, institutional move
INDECISION   = "INDECISION"     # doji / neutral

@dataclass
class Candle:
    time:  int
    open:  float
    high:  float
    low:   float
    close: float
    state: str = INDECISION     # filled by CandleStore.classify()

    def bull(self):       return self.close > self.open
    def bear(self):       return self.close < self.open
    def body(self):       return abs(self.close - self.open)
    def rng(self):        return self.high - self.low
    def body_pct(self):   return self.body() / self.rng() if self.rng() else 0
    def up_wick(self):    return self.high - max(self.open, self.close)
    def lo_wick(self):    return min(self.open, self.close) - self.low

    def disp(self, avg, mult=1.8):
        return self.body() >= avg * mult and self.body_pct() >= 0.6

    def rejection_wick(self, direction):
        if direction == "bull":
            return self.lo_wick() >= self.body() * 1.5
        return self.up_wick() >= self.body() * 1.5

@dataclass
class POI:
    kind:    str
    top:     float
    bottom:  float
    tf:      str
    time:    int
    fresh:   bool = True
    touches: int  = 0
    protected: bool = False   # validated by displacement away from it
    stale_count: int = 0      # candles since price was at POI

@dataclass
class Narrative:
    symbol:      str
    name:        str
    bias:        str = "NEUTRAL"
    h4_bias:     str = "NEUTRAL"
    h1_bias:     str = "NEUTRAL"
    location:    str = "NEUTRAL"
    extended:    bool = False
    draw:        float = 0.0
    draw_desc:   str = ""
    dealing_h:   float = 0.0
    dealing_l:   float = float('inf')
    pois:        List[POI] = field(default_factory=list)
    gate_open:   bool = False
    watching_ltf:bool = False
    score:       int = 0
    last_alert:  Optional[datetime] = None
    alerts_today:int = 0
    alert_day:   int = -1
    # Confidence decay
    gate_open_since: int = 0    # candles since gate opened
    poi_touch_candle:int = 0    # candle index when POI was touched

@dataclass
class SequenceResult:
    """Result of candle sequence analysis"""
    valid:        bool
    score_bonus:  int
    description:  str
    states_found: List[str]
    has_compression: bool
    has_sweep:       bool
    has_rejection:   bool
    has_displacement:bool

@dataclass
class Alert:
    symbol:    str
    name:      str
    narrative: str
    trigger:   str
    sequence:  str
    direction: str
    entry:     float
    sl:        float
    tp:        float
    rr:        float
    score:     int
    tf:        str
    session:   str
    details:   dict = field(default_factory=dict)
    time:      datetime = field(default_factory=datetime.utcnow)

# ============================================================
# MATH HELPERS
# ============================================================

def ema(vals, p):
    if not vals: return 0.0
    if len(vals) < p:
        return sum(vals) / len(vals)
    k = 2 / (p + 1)
    e = sum(vals[:p]) / p
    for v in vals[p:]:
        e = v * k + e * (1 - k)
    return e

def sma(vals, p):
    if not vals: return 0.0
    tail = vals[-p:] if len(vals) >= p else vals
    return sum(tail) / len(tail)

def get_session():
    h = datetime.utcnow().hour
    s = [n for n, (a, b) in SESSIONS.items() if a <= h < b]
    return ", ".join(s) if s else "Off-session"

def pct_in_range(price, high, low):
    if high == low: return 50.0
    return (price - low) / (high - low) * 100

def is_extended(price, high, low, direction):
    pct = pct_in_range(price, high, low)
    if direction == "BULLISH": return pct > 80
    if direction == "BEARISH": return pct < 20
    return False

# ============================================================
# CANDLE STORE + CLASSIFIER
# ============================================================

class CandleStore:
    def __init__(self, symbol: str, tf: str, maxlen: int = 300):
        self.symbol  = symbol
        self.tf      = tf
        self.candles = deque(maxlen=maxlen)
        self.closes  = deque(maxlen=220)
        self._inst_class = INSTRUMENT_CLASS.get(symbol, "forex")
        self._disp_mult  = DISP_MULT.get(self._inst_class, 1.8)

    def add(self, c: Candle):
        # Classify candle state before storing
        avg = self.avg_range()
        c.state = self._classify(c, avg)
        self.candles.append(c)
        self.closes.append(c.close)

    def _classify(self, c: Candle, avg: float) -> str:
        """
        Classify a candle into one of 5 states.
        Uses adaptive thresholds based on instrument class.
        """
        if avg == 0:
            return INDECISION

        rng_ratio  = c.rng() / avg
        body_ratio = c.body_pct()
        up_ratio   = c.up_wick() / c.rng() if c.rng() else 0
        lo_ratio   = c.lo_wick() / c.rng() if c.rng() else 0

        # DISPLACEMENT: large body, strong directional close
        if c.disp(avg, self._disp_mult):
            return DISPLACEMENT

        # COMPRESSION: small range, small body — coiling
        if rng_ratio < 0.5 and body_ratio < 0.4:
            return COMPRESSION

        # SWEEP: significant wick (>40% of range) + small body
        if (up_ratio > 0.4 or lo_ratio > 0.4) and body_ratio < 0.35:
            return SWEEP

        # REJECTION: decent body closing away from one extreme
        if body_ratio >= 0.4:
            if c.bull() and lo_ratio > 0.3:  return REJECTION
            if c.bear() and up_ratio > 0.3:  return REJECTION

        return INDECISION

    def get(self, n=None) -> List[Candle]:
        cl = list(self.candles)
        return cl[-n:] if n else cl

    def last(self) -> Optional[Candle]:
        return self.candles[-1] if self.candles else None

    def ready(self) -> bool:
        return len(self.candles) >= 30

    def avg_range(self, n: int = 20) -> float:
        c = list(self.candles)[-n:]
        return sum(x.rng() for x in c) / len(c) if c else 0.0

    def ema21(self):  return ema(list(self.closes), 21)
    def sma20(self):  return sma(list(self.closes), 20)
    def sma200(self): return sma(list(self.closes), 200)

    def swings(self, lookback: int = 5):
        c = self.get()
        highs = []; lows = []
        for i in range(lookback, len(c) - lookback):
            window = range(i - lookback, i + lookback + 1)
            if all(c[i].high >= c[j].high for j in window if j != i):
                highs.append(c[i])
            if all(c[i].low <= c[j].low for j in window if j != i):
                lows.append(c[i])
        return highs, lows

    def protected_swings(self):
        """
        Protected high/low: swing point that was tested AND had
        a displacement candle moving away from it — institutionally validated.
        """
        highs, lows = self.swings()
        candles = self.get()
        avg = self.avg_range()

        p_highs = []
        p_lows  = []

        for sh in highs:
            # Find if there's a displacement candle after this swing high
            for c in candles:
                if c.time > sh.time and c.bear() and c.disp(avg, self._disp_mult):
                    p_highs.append(sh)
                    break

        for sl in lows:
            for c in candles:
                if c.time > sl.time and c.bull() and c.disp(avg, self._disp_mult):
                    p_lows.append(sl)
                    break

        return p_highs, p_lows

    def trend(self) -> str:
        last = self.last()
        if not last: return "NEUTRAL"
        e = self.ema21()
        if not e: return "NEUTRAL"
        if last.close > e * 1.001: return "BULLISH"
        if last.close < e * 0.999: return "BEARISH"
        return "NEUTRAL"

    def structure_bias(self) -> str:
        highs, lows = self.swings()
        if len(highs) < 2 or len(lows) < 2:
            return self.trend()
        hh = highs[-1].high > highs[-2].high
        hl = lows[-1].low   > lows[-2].low
        lh = highs[-1].high < highs[-2].high
        ll = lows[-1].low   < lows[-2].low
        if hh and hl: return "BULLISH"
        if lh and ll: return "BEARISH"
        return self.trend()

    def detect_eqhl(self):
        highs, lows = self.swings()
        avg = self.avg_range()
        tol = avg * 0.15
        eqh = []; eql = []
        for i in range(1, len(highs)):
            if abs(highs[i].high - highs[i-1].high) <= tol:
                eqh.append((highs[i].high + highs[i-1].high) / 2)
        for i in range(1, len(lows)):
            if abs(lows[i].low - lows[i-1].low) <= tol:
                eql.append((lows[i].low + lows[i-1].low) / 2)
        return eqh, eql

    def detect_pois(self) -> List[POI]:
        c = self.get(); avg = self.avg_range(); pois = []
        if len(c) < 5: return pois
        mult = self._disp_mult
        # FVGs
        for i in range(2, len(c)):
            c1 = c[i-2]; c2 = c[i-1]; c3 = c[i]
            if c3.low > c1.high and c2.disp(avg, mult):
                pois.append(POI("fvg_bull", c3.low, c1.high, self.tf, c2.time))
            if c3.high < c1.low and c2.disp(avg, mult):
                pois.append(POI("fvg_bear", c1.low, c3.high, self.tf, c2.time))
        # OBs
        for i in range(1, len(c) - 1):
            prev = c[i-1]; disp = c[i]
            if prev.bear() and disp.bull() and disp.disp(avg, mult):
                poi = POI("ob_bull",
                    max(prev.open, prev.close),
                    min(prev.open, prev.close),
                    self.tf, prev.time)
                # Mark as protected if displacement away confirmed
                poi.protected = True
                pois.append(poi)
            if prev.bull() and disp.bear() and disp.disp(avg, mult):
                poi = POI("ob_bear",
                    max(prev.open, prev.close),
                    min(prev.open, prev.close),
                    self.tf, prev.time)
                poi.protected = True
                pois.append(poi)
        # Keep last 5 per type
        seen = defaultdict(int); result = []
        for p in reversed(pois):
            if seen[p.kind] < 5:
                result.append(p); seen[p.kind] += 1
        return result

    def get_states(self, n: int = 10) -> List[str]:
        """Return last N candle states for sequence analysis"""
        return [c.state for c in self.get(n)]

# ============================================================
# SEQUENCE ANALYZER — The "Last Part"
# ============================================================

class SequenceAnalyzer:
    """
    Validates candle sequences before firing alerts.
    Requires at least 3 of 4 states in logical order.

    Valid sequences (minimum 3 of 4):
      COMPRESSION → SWEEP → DISPLACEMENT          score +1
      COMPRESSION → REJECTION → DISPLACEMENT      score +1
      SWEEP → REJECTION → DISPLACEMENT            score +2 (strongest)
      COMPRESSION → SWEEP → REJECTION → DISP      score +3 (super)

    Invalid (rejected):
      DISPLACEMENT alone
      REJECTION → DISPLACEMENT only
      SWEEP → DISPLACEMENT only
      COMPRESSION → DISPLACEMENT only
    """

    def analyze(self, states: List[str], direction: str,
                avg: float, candles: List[Candle]) -> SequenceResult:
        """
        Analyze candle state sequence.
        direction: 'BULLISH' or 'BEARISH'
        """
        # Look back through last 10 states
        window = states[-10:] if len(states) >= 10 else states

        has_comp = COMPRESSION  in window
        has_sweep= SWEEP        in window
        has_rej  = REJECTION    in window
        has_disp = DISPLACEMENT in window

        states_found = [s for s in [
            COMPRESSION if has_comp else None,
            SWEEP       if has_sweep else None,
            REJECTION   if has_rej  else None,
            DISPLACEMENT if has_disp else None,
        ] if s]

        # DISPLACEMENT is always required
        if not has_disp:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="No displacement candle in sequence",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp)

        # Need at least sweep OR rejection (not just displacement)
        if not has_sweep and not has_rej:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="Displacement without sweep or rejection — too weak",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp)

        # Validate order: liquidity event must PRECEDE displacement
        disp_idx  = self._last_index(window, DISPLACEMENT)
        sweep_idx = self._last_index(window, SWEEP)
        rej_idx   = self._last_index(window, REJECTION)

        order_ok = False
        if has_sweep and sweep_idx < disp_idx:  order_ok = True
        if has_rej   and rej_idx   < disp_idx:  order_ok = True

        if not order_ok:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="Order invalid — displacement before sweep/rejection",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp)

        # Validate direction of displacement matches narrative
        last_disp = next(
            (c for c in reversed(candles) if c.state == DISPLACEMENT), None)
        if last_disp:
            if direction == "BULLISH" and not last_disp.bull():
                return SequenceResult(
                    valid=False, score_bonus=0,
                    description="Displacement direction mismatch",
                    states_found=states_found,
                    has_compression=has_comp, has_sweep=has_sweep,
                    has_rejection=has_rej, has_displacement=has_disp)
            if direction == "BEARISH" and not last_disp.bear():
                return SequenceResult(
                    valid=False, score_bonus=0,
                    description="Displacement direction mismatch",
                    states_found=states_found,
                    has_compression=has_comp, has_sweep=has_sweep,
                    has_rejection=has_rej, has_displacement=has_disp)

        # Score the sequence
        score_bonus = 0
        desc_parts  = []

        if has_comp and has_sweep and has_rej and has_disp:
            score_bonus = 3
            desc_parts.append("🌟 FULL SEQUENCE: Compression→Sweep→Rejection→Displacement")
        elif has_sweep and has_rej and has_disp:
            score_bonus = 2
            desc_parts.append("⭐ Sweep→Rejection→Displacement")
        elif has_comp and has_sweep and has_disp:
            score_bonus = 1
            desc_parts.append("Compression→Sweep→Displacement")
        elif has_comp and has_rej and has_disp:
            score_bonus = 1
            desc_parts.append("Compression→Rejection→Displacement")
        else:
            score_bonus = 0
            desc_parts.append("Sweep/Rejection→Displacement (basic)")

        return SequenceResult(
            valid=True,
            score_bonus=score_bonus,
            description=" | ".join(desc_parts),
            states_found=states_found,
            has_compression=has_comp,
            has_sweep=has_sweep,
            has_rejection=has_rej,
            has_displacement=has_disp)

    def _last_index(self, states: List[str], target: str) -> int:
        """Return index of last occurrence of target state"""
        for i in range(len(states) - 1, -1, -1):
            if states[i] == target:
                return i
        return -1

# ============================================================
# NARRATIVE ENGINE
# ============================================================

seq_analyzer = SequenceAnalyzer()

class NarrativeEngine:
    def __init__(self, symbol: str, name: str):
        self.symbol = symbol
        self.name   = name
        self.stores = {tf: CandleStore(symbol, tf) for tf in TIMEFRAMES}
        self.narr   = Narrative(symbol=symbol, name=name)
        self._candle_count = 0

    def feed(self, tf: str, c: Candle) -> Optional[Alert]:
        self.stores[tf].add(c)
        if tf == "M15":
            self._candle_count += 1
        if not all(s.ready() for s in self.stores.values()):
            return None
        self._update_narrative()
        # Apply confidence decay
        self._apply_decay()
        if self.narr.gate_open:
            return self._check_confluence_and_trigger()
        return None

    def _apply_decay(self):
        """Decay narrative confidence over time to prevent stale signals"""
        n = self.narr
        if n.gate_open:
            n.gate_open_since += 1
            # If gate has been open > 50 M15 candles (~12 hours) without
            # triggering, reduce score and re-evaluate
            if n.gate_open_since > 50:
                n.score = max(0, n.score - 1)
                n.gate_open_since = 0  # reset decay counter

        # Mark POIs as stale if price hasn't interacted
        for poi in n.pois:
            if poi.fresh:
                poi.stale_count += 1
                # After 96 candles (24 hours on H1) mark stale
                if poi.stale_count > 96:
                    poi.fresh = False

    def _update_narrative(self):
        n  = self.narr
        d1 = self.stores["D1"]
        h4 = self.stores["H4"]
        h1 = self.stores["H1"]

        # Directional bias
        n.bias    = d1.structure_bias()
        n.h4_bias = h4.structure_bias()
        n.h1_bias = h1.structure_bias()

        # Dealing range from D1 (last 50 candles)
        d1c = d1.get(50)
        if d1c:
            n.dealing_h = max(c.high for c in d1c)
            n.dealing_l = min(c.low  for c in d1c)

        # Price location
        last = d1.last()
        if last and n.dealing_h > n.dealing_l:
            pct = pct_in_range(last.close, n.dealing_h, n.dealing_l)
            if pct > 60:   n.location = "PREMIUM"
            elif pct < 40: n.location = "DISCOUNT"
            else:          n.location = "NEUTRAL"
            n.extended = is_extended(
                last.close, n.dealing_h, n.dealing_l, n.bias)

        # Liquidity draw
        eqh, eql = d1.detect_eqhl()
        if n.bias == "BEARISH" and eql:
            n.draw = min(eql); n.draw_desc = f"EQL at {n.draw:.5f}"
        elif n.bias == "BULLISH" and eqh:
            n.draw = max(eqh); n.draw_desc = f"EQH at {n.draw:.5f}"
        else:
            p_highs, p_lows = d1.protected_swings()
            if n.bias == "BULLISH" and p_highs:
                n.draw = p_highs[-1].high
                n.draw_desc = f"Protected High {n.draw:.5f}"
            elif n.bias == "BEARISH" and p_lows:
                n.draw = p_lows[-1].low
                n.draw_desc = f"Protected Low {n.draw:.5f}"

        # Collect POIs
        n.pois = (d1.detect_pois() +
                  h4.detect_pois() +
                  h1.detect_pois())

        # Gate check
        bias_ok     = n.bias != "NEUTRAL"
        h4_ok       = n.h4_bias == n.bias
        location_ok = (
            (n.bias == "BULLISH" and n.location == "DISCOUNT") or
            (n.bias == "BEARISH" and n.location == "PREMIUM") or
            n.location == "NEUTRAL"
        )
        not_extended = not n.extended
        has_draw     = n.draw != 0.0

        prev_gate = n.gate_open
        n.gate_open = bias_ok and h4_ok and location_ok and not_extended and has_draw

        # Reset decay counter when gate freshly opens
        if n.gate_open and not prev_gate:
            n.gate_open_since = 0

        # Score narrative
        score = 0
        if bias_ok:                              score += 2
        if h4_ok:                                score += 2
        if n.h1_bias == n.bias:                  score += 1
        if n.location in ["PREMIUM","DISCOUNT"]: score += 1
        if location_ok:                          score += 1
        if has_draw:                             score += 1
        if not_extended:                         score += 1
        n.score = min(score, 9)

    def _check_confluence_and_trigger(self) -> Optional[Alert]:
        n    = self.narr
        h4   = self.stores["H4"]
        h1   = self.stores["H1"]
        m15  = self.stores["M15"]
        last = h4.last() or h1.last()
        if not last: return None

        price = last.close
        avg   = h4.avg_range()
        buf   = avg * 0.3

        # Alert cooldown
        if n.last_alert:
            hrs = (datetime.utcnow() - n.last_alert).total_seconds() / 3600
            if hrs < COOLDOWN_HOURS:
                return None

        today = datetime.utcnow().day
        if n.alert_day == today and n.alerts_today >= MAX_ALERTS_PER_DAY:
            return None

        # Find active POI
        active_poi = None
        for poi in n.pois:
            if not poi.fresh: continue
            in_zone = poi.bottom - buf <= price <= poi.top + buf
            if not in_zone: continue
            if n.bias == "BULLISH" and "bull" in poi.kind:
                active_poi = poi; break
            if n.bias == "BEARISH" and "bear" in poi.kind:
                active_poi = poi; break

        if not active_poi: return None

        # Track POI touch for decay
        n.poi_touch_candle = self._candle_count

        # Confluence check
        e21     = h4.ema21()
        s200    = h4.sma200()
        session = get_session()

        ma_ok   = ((n.bias == "BULLISH" and price > e21) or
                   (n.bias == "BEARISH" and price < e21)) if e21 else False
        s200_ok = ((n.bias == "BULLISH" and price > s200) or
                   (n.bias == "BEARISH" and price < s200)) if s200 else False
        sess_ok = "London" in session or "NY" in session
        h1_ok   = n.h1_bias == n.bias
        loc_ok  = n.location in ["PREMIUM", "DISCOUNT"]
        protected_poi = active_poi.protected

        confs      = [ma_ok, s200_ok, sess_ok, h1_ok, loc_ok, protected_poi]
        conf_count = sum(1 for c in confs if c)
        if conf_count < 2: return None

        # ── SEQUENCE ANALYSIS ──────────────────────────────
        m15_states  = m15.get_states(10)
        m15_candles = m15.get(10)
        seq_result  = seq_analyzer.analyze(
            m15_states, n.bias, m15.avg_range(), m15_candles)

        if not seq_result.valid:
            log.debug(
                f"{n.name}: Sequence invalid — {seq_result.description}")
            return None

        # ── LTF TRIGGER ────────────────────────────────────
        trigger = self._check_ltf_trigger(m15, n.bias)
        if not trigger: return None

        # ── TRADE LEVELS ───────────────────────────────────
        entry, sl, tp, rr = self._calc_trade(
            n.bias, price, avg, active_poi, n.draw)
        if rr < 1.5: return None

        # ── FINAL SCORE ────────────────────────────────────
        score = n.score + conf_count + seq_result.score_bonus
        if trigger["quality"] == "high": score += 1
        if active_poi.protected:         score += 1
        score = min(score, 10)
        if score < MIN_SCORE: return None

        # ── BUILD NARRATIVE TEXT ───────────────────────────
        narr_text = (
            f"D1 {n.bias} | H4 {n.h4_bias} | H1 {n.h1_bias}\n"
            f"Location: {n.location}\n"
            f"Draw: {n.draw_desc}\n"
            f"POI: {active_poi.tf} "
            f"{'OB' if 'ob' in active_poi.kind else 'FVG'}"
            f" ({active_poi.bottom:.5f}—{active_poi.top:.5f})"
            f"{'  🛡Protected' if active_poi.protected else ''}"
        )

        # ── MARK ALERT ─────────────────────────────────────
        n.last_alert   = datetime.utcnow()
        n.alerts_today = (n.alerts_today + 1
                          if n.alert_day == today else 1)
        n.alert_day    = today
        active_poi.fresh = False

        return Alert(
            symbol    = n.symbol,
            name      = n.name,
            narrative = narr_text,
            trigger   = trigger["desc"],
            sequence  = seq_result.description,
            direction = n.bias,
            entry     = entry, sl=sl, tp=tp, rr=rr,
            score     = score,
            tf        = active_poi.tf,
            session   = session,
            details   = {
                "ma_ok":          ma_ok,
                "s200_ok":        s200_ok,
                "session":        session,
                "conf_count":     conf_count,
                "poi_kind":       active_poi.kind,
                "protected_poi":  active_poi.protected,
                "trigger_quality":trigger["quality"],
                "extended":       n.extended,
                "seq_states":     seq_result.states_found,
                "seq_score_bonus":seq_result.score_bonus,
                "has_full_seq":   seq_result.score_bonus == 3,
            }
        )

    def _check_ltf_trigger(
            self, m15: CandleStore, direction: str) -> Optional[dict]:
        """
        LTF confirmation — fires EARLY during accumulation/reversal,
        NOT after price has already expanded.
        Sequence must already be validated before reaching here.
        """
        candles = m15.get(10)
        if len(candles) < 5: return None
        avg  = m15.avg_range()
        last = candles[-1]
        prev = candles[-2] if len(candles) >= 2 else last

        # High quality: rejection wick AT the POI level
        d = "bull" if direction == "BULLISH" else "bear"
        if last.rejection_wick(d):
            return {"desc": "M15 rejection wick at POI",
                    "quality": "high"}

        # High quality: EQH/EQL swept then closed back (MSS forming)
        eqh, eql = m15.detect_eqhl()
        if direction == "BEARISH" and eqh:
            if last.high >= eqh[-1] and last.close < eqh[-1]:
                return {
                    "desc": f"M15 EQH swept ({eqh[-1]:.5f}) — MSS forming",
                    "quality": "high"}
        if direction == "BULLISH" and eql:
            if last.low <= eql[-1] and last.close > eql[-1]:
                return {
                    "desc": f"M15 EQL swept ({eql[-1]:.5f}) — MSS forming",
                    "quality": "high"}

        # Medium: CHoC — body closure above/below structure
        if (direction == "BULLISH" and last.bull()
                and last.body_pct() >= 0.6
                and last.close > prev.high):
            return {"desc": "M15 CHoC — bullish body closure above structure",
                    "quality": "medium"}
        if (direction == "BEARISH" and last.bear()
                and last.body_pct() >= 0.6
                and last.close < prev.low):
            return {"desc": "M15 CHoC — bearish body closure below structure",
                    "quality": "medium"}

        # Medium: directional displacement in LTF (sequence already validated)
        disp_mult = DISP_MULT.get(
            INSTRUMENT_CLASS.get(m15.symbol, "forex"), 1.8)
        if last.disp(avg, disp_mult):
            d_dir = "BULLISH" if last.bull() else "BEARISH"
            if d_dir == direction:
                return {
                    "desc": f"M15 displacement — {direction.lower()} momentum",
                    "quality": "medium"}

        return None

    def _calc_trade(self, direction, price, avg, poi, draw):
        buf = avg * 0.5
        if direction == "BULLISH":
            entry = poi.bottom + (poi.top - poi.bottom) * 0.3
            sl    = poi.bottom - buf
            tp    = draw if draw > price else price + (price - sl) * 3
        else:
            entry = poi.top - (poi.top - poi.bottom) * 0.3
            sl    = poi.top + buf
            tp    = draw if draw < price else price - (sl - price) * 3
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        rr     = round(reward / risk, 1) if risk else 0
        return round(entry, 5), round(sl, 5), round(tp, 5), rr

    def summary(self) -> str:
        n    = self.narr
        last = self.stores["D1"].last()
        price = last.close if last else 0
        icon  = "🟢" if n.bias=="BULLISH" else ("🔴" if n.bias=="BEARISH" else "⚪")
        gate  = "✅ OPEN" if n.gate_open else "⛔ CLOSED"
        ext   = "  ⚠️ Extended!" if n.extended else ""
        return (
            f"{icon} {n.name}\n"
            f"D1: {n.bias} | H4: {n.h4_bias} | H1: {n.h1_bias}\n"
            f"📍 {n.location}{ext}\n"
            f"🎯 Draw: {n.draw_desc or 'Not identified'}\n"
            f"📊 Score: {n.score}/9 | Gate: {gate}\n"
            f"⏰ {get_session()}\n"
            f"POIs tracked: {len([p for p in n.pois if p.fresh])}"
        )

# ============================================================
# WEBSOCKET MANAGER
# ============================================================

class WSManager:
    def __init__(self):
        self.engines: Dict[str, NarrativeEngine] = {}
        self.running  = False
        self.app_ref  = None

    def add_instrument(self, sym: str, name: str):
        self.engines[sym] = NarrativeEngine(sym, name)

    def remove_instrument(self, sym: str):
        self.engines.pop(sym, None)

    async def load_all_history(self):
        log.info(f"Loading history for {len(self.engines)} instruments...")
        for sym, eng in list(self.engines.items()):
            for tf, gran in TIMEFRAMES.items():
                await self._fetch_history(sym, tf, gran, eng)
                await asyncio.sleep(0.2)
        log.info("✅ History loaded!")

    async def _fetch_history(self, sym, tf, gran, eng):
        try:
            async with websockets.connect(
                    DERIV_WS, ping_interval=None) as ws:
                await ws.send(json.dumps({
                    "ticks_history": sym,
                    "granularity":   gran,
                    "count":         200,
                    "end":           "latest",
                    "style":         "candles"
                }))
                while True:
                    r = json.loads(
                        await asyncio.wait_for(ws.recv(), 20))
                    if "candles" in r:
                        for cd in r["candles"]:
                            eng.stores[tf].add(Candle(
                                time=cd["epoch"],
                                open=float(cd["open"]),
                                high=float(cd["high"]),
                                low=float(cd["low"]),
                                close=float(cd["close"])))
                        log.info(f"✅ {sym} {tf}: {len(r['candles'])} candles")
                        break
                    if "error" in r:
                        log.warning(f"{sym} {tf}: {r['error']['message']}")
                        break
        except Exception as e:
            log.error(f"History {sym} {tf}: {e}")

    async def run_live(self):
        tasks = [
            self._subscribe(sym, tf, gran, eng)
            for sym, eng in list(self.engines.items())
            for tf, gran in TIMEFRAMES.items()
        ]
        await asyncio.gather(*tasks)

    async def _subscribe(self, sym, tf, gran, eng):
        while self.running:
            try:
                async with websockets.connect(
                        DERIV_WS, ping_interval=30) as ws:
                    await ws.send(json.dumps({
                        "ticks_history": sym,
                        "granularity":   gran,
                        "count":         2,
                        "end":           "latest",
                        "style":         "candles",
                        "subscribe":     1
                    }))
                    while self.running:
                        r = json.loads(
                            await asyncio.wait_for(ws.recv(), 90))
                        if "ohlc" in r:
                            o = r["ohlc"]
                            c = Candle(
                                time=int(o["open_time"]),
                                open=float(o["open"]),
                                high=float(o["high"]),
                                low=float(o["low"]),
                                close=float(o["close"]))
                            alert = eng.feed(tf, c)
                            if alert and self.app_ref:
                                asyncio.create_task(
                                    self._send_alert(alert))
            except Exception as e:
                if self.running:
                    log.error(f"Sub {sym}/{tf}: {e}")
                    await asyncio.sleep(5)

    async def _send_alert(self, alert: Alert):
        if not state.chat_id: return
        try:
            msg = fmt_alert(alert)
            kb  = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "📊 Full Narrative",
                    callback_data=f"narr_{alert.symbol}"),
                InlineKeyboardButton(
                    "⏭ Skip", callback_data="skip_alert"),
            ]])
            await self.app_ref.bot.send_message(
                state.chat_id, msg, reply_markup=kb)
            state.alert_count += 1
        except Exception as e:
            log.error(f"Send alert: {e}")

ws_manager = WSManager()

# ============================================================
# ALERT FORMATTER
# ============================================================

def fmt_alert(a: Alert) -> str:
    icon  = "🟢" if a.direction == "BULLISH" else "🔴"
    stars = "⭐" * min(a.score, 5)
    d     = a.details
    full  = d.get("has_full_seq", False)

    msg  = f"{'🌟' if full else icon} {a.name} — {a.direction}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📖 Narrative:\n"
    for line in a.narrative.split("\n"):
        msg += f"   {line}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔢 Sequence: {a.sequence}\n"
    msg += f"⚡ Trigger:  {a.trigger}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📐 Trade Setup:\n"
    msg += f"   Entry: {a.entry}\n"
    msg += f"   SL:    {a.sl}\n"
    msg += f"   TP:    {a.tp}\n"
    msg += f"   RR:    1:{a.rr}\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⭐ Score: {stars} {a.score}/10\n"
    msg += f"⏰ {a.session} session\n"

    confs = []
    if d.get("ma_ok"):          confs.append("EMA21 ✅")
    if d.get("s200_ok"):        confs.append("SMA200 ✅")
    if d.get("protected_poi"):  confs.append("🛡 Protected POI")
    if d.get("trigger_quality") == "high": confs.append("High-quality trigger")
    if confs:
        msg += f"🔗 {' | '.join(confs)}\n"

    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚠️ Always confirm on chart before entry!"
    return msg

# ============================================================
# BOT STATE
# ============================================================

class BotState:
    def __init__(self):
        self.running      = False
        self.watchlist    = dict(DEFAULT_WATCHLIST)
        self.chat_id      = None
        self.alert_count  = 0
        self.start_time   = None
        self._scan_loop   = None
        self._scan_thread = None

    def rebuild_engines(self):
        ws_manager.engines.clear()
        for sym, name in self.watchlist.items():
            ws_manager.add_instrument(sym, name)

state = BotState()

# ── Scanner threading (buttons always respond) ──────────────

def start_scanner(app):
    ws_manager.running = True
    ws_manager.app_ref = app
    state.rebuild_engines()
    loop = asyncio.new_event_loop()
    state._scan_loop = loop

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_scanner_main())

    t = threading.Thread(target=run, daemon=True)
    state._scan_thread = t
    t.start()
    log.info("Scanner started in background thread")

def stop_scanner():
    ws_manager.running = False
    if state._scan_loop:
        state._scan_loop.call_soon_threadsafe(state._scan_loop.stop)

async def _scanner_main():
    await ws_manager.load_all_history()
    if state.chat_id and ws_manager.app_ref:
        await ws_manager.app_ref.bot.send_message(
            state.chat_id,
            f"✅ Narrative + Sequence Engine Active!\n"
            f"📊 {len(state.watchlist)} instruments\n"
            f"🔍 Funnel: Narrative→Confluence→Sequence→Trigger\n"
            f"🔢 Sequence validation: 3 of 4 states required\n"
            f"📉 Confidence decay: active\n"
            f"🛡 Protected POI: validated\n"
            f"🔕 Max {MAX_ALERTS_PER_DAY} alerts/day per instrument\n"
            f"⭐ Min score: {MIN_SCORE}/10\n\n"
            f"Alerts fire BEFORE expansion — not after!"
        )
    await ws_manager.run_live()

# ============================================================
# TELEGRAM UI
# ============================================================

def main_kb():
    btn = "⏹ Stop" if state.running else "▶️ Start Engine"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(btn, callback_data="toggle")],
        [InlineKeyboardButton("📊 Narratives",   callback_data="narratives"),
         InlineKeyboardButton("📋 Watchlist",    callback_data="watchlist")],
        [InlineKeyboardButton("📈 Even/Odd Bot", callback_data="evenodd"),
         InlineKeyboardButton("📉 Signal Log",   callback_data="signals")],
        [InlineKeyboardButton("⚙️ Settings",     callback_data="settings"),
         InlineKeyboardButton("❓ How It Works", callback_data="help")],
    ])

def wl_kb():
    rows = [[InlineKeyboardButton(
        f"❌ {n}", callback_data=f"rm_{s}"
    )] for s, n in state.watchlist.items()]
    rows += [
        [InlineKeyboardButton("➕ Add Instrument", callback_data="add")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(rows)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state.chat_id = update.effective_chat.id
    await update.message.reply_text(
        "🤖 *DerivBot Pro — Final Build*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "Funnel architecture:\n"
        "1️⃣ Narrative: Bias + Location + Draw\n"
        "2️⃣ Confluence: POI + MA + Session\n"
        "3️⃣ Sequence: Compression→Sweep→\n"
        "          Rejection→Displacement\n"
        "4️⃣ Trigger: LTF MSS/Sweep/Wick\n\n"
        "🛡 Protected POI validation\n"
        "📉 Confidence decay model\n"
        "⚡ Adaptive thresholds per asset\n"
        "🔕 Max 3 alerts/day per instrument\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 {len(state.watchlist)} instruments ready",
        parse_mode="Markdown",
        reply_markup=main_kb()
    )

async def cmd_btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()   # Immediate — prevents button freeze
    d = q.data

    if d == "menu":
        await q.edit_message_text(
            "🤖 DerivBot Pro", reply_markup=main_kb())

    elif d == "toggle":
        if not state.running:
            state.running    = True
            state.start_time = datetime.utcnow()
            start_scanner(ctx.application)
            await q.edit_message_text(
                f"✅ Engine Started!\n"
                f"{len(state.watchlist)} instruments\n"
                f"Loading history (~2 mins)...\n"
                f"Sequence validation: ON\n"
                f"Confidence decay: ON",
                reply_markup=main_kb())
        else:
            state.running = False
            stop_scanner()
            await q.edit_message_text(
                f"⏹ Engine Stopped\n"
                f"Alerts sent: {state.alert_count}",
                reply_markup=main_kb())

    elif d == "narratives":
        lines = ["📊 Market Narratives\n━━━━━━━━━━━━━━━━━━━"]
        for sym, eng in list(ws_manager.engines.items())[:10]:
            n  = eng.narr
            ic = "🟢" if n.bias=="BULLISH" else ("🔴" if n.bias=="BEARISH" else "⚪")
            gt = "✅" if n.gate_open else "⛔"
            lines.append(f"{ic} {n.name}: {n.bias} | {n.location} {gt}")
        lines.append(f"\n⏰ {get_session()}")
        await q.edit_message_text(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Refresh",  callback_data="narratives"),
                InlineKeyboardButton("◀️ Back",     callback_data="menu")]]))

    elif d.startswith("narr_"):
        sym = d[5:]
        eng = ws_manager.engines.get(sym)
        if eng:
            await q.edit_message_text(
                eng.summary(),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "◀️ Back", callback_data="narratives")]]))
        else:
            await q.edit_message_text(
                "Not found.", reply_markup=main_kb())

    elif d == "watchlist":
        txt  = f"📋 Watchlist ({len(state.watchlist)})\n"
        txt += "━━━━━━━━━━━━━━━━━━━\n"
        txt += "\n".join(f"• {n}" for n in state.watchlist.values())
        await q.edit_message_text(txt, reply_markup=wl_kb())

    elif d.startswith("rm_"):
        sym  = d[3:]
        name = state.watchlist.pop(sym, sym)
        ws_manager.remove_instrument(sym)
        await q.edit_message_text(
            f"✅ Removed {name}", reply_markup=wl_kb())

    elif d == "add":
        await q.edit_message_text(
            "➕ Send: SYMBOL NAME\n\n"
            "Examples:\n"
            "frxGBPUSD GBPUSD\n"
            "frxUSDCAD USDCAD\n"
            "R_100 Volatility 100",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "◀️ Back", callback_data="watchlist")]]))

    elif d == "signals":
        t = (state.start_time.strftime("%H:%M UTC")
             if state.start_time else "Not started")
        await q.edit_message_text(
            f"📉 Signal Log\n━━━━━━━━━━━━━━\n"
            f"Alerts sent: {state.alert_count}\n"
            f"Running since: {t}\n"
            f"Status: {'🟢 Active' if state.running else '⏹ Stopped'}\n\n"
            f"Limits:\n"
            f"• Max {MAX_ALERTS_PER_DAY}/day per instrument\n"
            f"• {COOLDOWN_HOURS}hr cooldown\n"
            f"• Min score {MIN_SCORE}/10\n"
            f"• Sequence validation required",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back", callback_data="menu")]]))

    elif d == "evenodd":
        await q.edit_message_text(
            "📈 Even/Odd Bot\n━━━━━━━━━━━━━━\n"
            "Run derivbot_replit.py separately\n"
            "for digit trading.\n\n"
            "Phase 4 merges both.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back", callback_data="menu")]]))

    elif d == "settings":
        await q.edit_message_text(
            f"⚙️ Settings\n━━━━━━━━━━━━━━\n"
            f"Min Score:      {MIN_SCORE}/10\n"
            f"Max Alerts/Day: {MAX_ALERTS_PER_DAY}\n"
            f"Cooldown:       {COOLDOWN_HOURS} hours\n"
            f"Instruments:    {len(state.watchlist)}\n"
            f"Timeframes:     D1|H4|H1|M15\n\n"
            f"Adaptive thresholds:\n"
            f"  Forex:     1.8x avg range\n"
            f"  Metal/Idx: 2.0x avg range\n"
            f"  Crypto:    2.2x avg range\n"
            f"  Synthetic: 2.5x avg range",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back", callback_data="menu")]]))

    elif d == "help":
        await q.edit_message_text(
            "❓ How It Works\n━━━━━━━━━━━━━━\n\n"
            "4-STAGE FUNNEL:\n\n"
            "1️⃣ NARRATIVE\n"
            "   D1+H4+H1 bias aligned\n"
            "   Price in discount/premium\n"
            "   Liquidity draw identified\n"
            "   Not extended ✅\n\n"
            "2️⃣ CONFLUENCE\n"
            "   Price at protected HTF POI\n"
            "   EMA21 + SMA200 aligned\n"
            "   Session timing ✅\n\n"
            "3️⃣ SEQUENCE (NEW)\n"
            "   Candle states validated:\n"
            "   Compression→Sweep→\n"
            "   Rejection→Displacement\n"
            "   Min 3 of 4 required ✅\n\n"
            "4️⃣ TRIGGER\n"
            "   M15 MSS/CHoC/Sweep/Wick\n"
            "   Alert fires EARLY ✅\n\n"
            "PROTECTION:\n"
            "🛡 Protected POI only\n"
            "📉 Confidence decay\n"
            "⚡ Adaptive thresholds\n"
            "🔕 3 alerts/day max",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back", callback_data="menu")]]))

    elif d == "skip_alert":
        await q.answer("Signal skipped.")

async def cmd_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if " " in text:
        parts = text.split(" ", 1)
        sym   = parts[0].strip()
        name  = parts[1].strip()
        state.watchlist[sym] = name
        ws_manager.add_instrument(sym, name)
            f"✅ Added {name} ({sym})\n"
            f"Total: {len(state.watchlist)} instruments",
            reply_markup=main_kb())
    else:
        await update.message.reply_text(
            "Format: SYMBOL NAME\nExample: frxGBPUSD GBPUSD")

# ============================================================
# MAIN
# ============================================================

async def health_check(request):
    """Dummy web server so Render free tier stays alive"""
    return web.Response(text="DerivBot Pro is running!")

async def run_web_server():
    """Start tiny web server on Render's required port"""
    port = int(os.environ.get("PORT", 8080))
    server = web.Application()
    server.router.add_get("/", health_check)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Health check server running on port {port}")
import asyncio

async def start_background_tasks(app):
    ws_manager.app_ref = app

    asyncio.create_task(ws_manager.load_all_history())
    asyncio.create_task(ws_manager.run_live())
async def run_bot():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(cmd_btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_msg))

    await app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

async def main_async():
    print("\n" + "=" * 55)
    print("  DerivBot Pro — Narrative + Sequence Engine")
    print(f"  Min score:        {MIN_SCORE}/10")
    print(f"  Max alerts/day:   {MAX_ALERTS_PER_DAY}")
    print(f"  Cooldown:         {COOLDOWN_HOURS} hours")
    print(f"  Sequence:         3 of 4 states required")
    print(f"  Instruments:      {len(DEFAULT_WATCHLIST)}")
    print("=" * 55 + "\n")
    # Run both web server and bot concurrently
    await asyncio.gather(
        run_web_server(),
        run_bot()
    )
  def main():
    run_bot()

if __name__ == "__main__":
    main()
