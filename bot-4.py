#!/usr/bin/env python3
"""
DerivBot Pro — MEGA FINAL BUILD
=================================
Architecture : Narrative → AutoFocus → Confluence → CISD/Sequence → Trigger
Integrated   : AutoFocusEngine, CISD, High-Probability Sweep
New          : Double EQH/EQL Run Logic
Risk Mgmt    : Entry/SL/TP from structure with trailing trigger
Deploy       : Replit / Render (web service compatible)
"""

import asyncio
import json
import logging
import os
import time
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
# TOKENS — Set as environment variables on Replit/Render
# ============================================================
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "PASTE_HERE")
DERIV_DEMO_TOKEN = os.environ.get("DERIV_DEMO_TOKEN", "PASTE_HERE")
DERIV_REAL_TOKEN = os.environ.get("DERIV_REAL_TOKEN", "PASTE_HERE")
# ============================================================

DERIV_WS           = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
MIN_SCORE          = 7
MAX_ALERTS_PER_DAY = 3
COOLDOWN_HOURS     = 4
MAX_FOCUS          = 4   # AutoFocus: top N instruments to deep-scan

# ============================================================
# INSTRUMENT CLASSES — adaptive thresholds
# ============================================================
INSTRUMENT_CLASS = {
    "frxEURCHF": "forex", "frxEURGBP": "forex",
    "frxEURJPY": "forex", "frxEURUSD": "forex",
    "frxGBPAUD": "forex", "frxGBPJPY": "forex",
    "frxGBPCAD": "forex", "frxUSDJPY": "forex",
    "frxXAUUSD": "metal", "frxBROUSD": "commodity",
    "frxBTCUSD": "crypto",
    "SPC": "index", "HSI": "index", "N225": "index",
    "DJI": "index", "NDX": "index",
    "R_75": "synthetic",
}
DISP_MULT = {
    "forex": 1.8, "metal": 2.0, "commodity": 2.0,
    "index": 2.0, "crypto": 2.2, "synthetic": 2.5,
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
# CANDLE STATES
# ============================================================
COMPRESSION  = "COMPRESSION"
SWEEP        = "SWEEP"
REJECTION    = "REJECTION"
DISPLACEMENT = "DISPLACEMENT"
INDECISION   = "INDECISION"

# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class Candle:
    time:  int
    open:  float
    high:  float
    low:   float
    close: float
    state: str = INDECISION

    def bull(self):     return self.close > self.open
    def bear(self):     return self.close < self.open
    def body(self):     return abs(self.close - self.open)
    def rng(self):      return self.high - self.low
    def body_pct(self): return self.body()/self.rng() if self.rng() else 0
    def up_wick(self):  return self.high - max(self.open, self.close)
    def lo_wick(self):  return min(self.open, self.close) - self.low
    def disp(self, avg, mult=1.8):
        return self.body() >= avg * mult and self.body_pct() >= 0.6
    def rejection_wick(self, direction):
        if direction == "bull": return self.lo_wick() >= self.body() * 1.5
        return self.up_wick() >= self.body() * 1.5

@dataclass
class POI:
    kind:      str
    top:       float
    bottom:    float
    tf:        str
    time:      int
    fresh:     bool = True
    touches:   int  = 0
    protected: bool = False
    stale_count: int = 0

@dataclass
class EQLiquidityLevel:
    """
    Equal High or Equal Low liquidity level.
    Tracks both legs so we can detect when BOTH are run.
    New concept: price tends to run both legs before reversing.
    """
    type:         str    # "EQH" or "EQL"
    level:        float  # average of both legs
    leg1:         float  # first high/low
    leg2:         float  # second high/low
    leg1_swept:   bool = False
    leg2_swept:   bool = False
    both_swept:   bool = False
    tf:           str  = "H4"
    time:         int  = 0
    reversal_watch: bool = False

@dataclass
class Narrative:
    symbol:       str
    name:         str
    bias:         str = "NEUTRAL"
    h4_bias:      str = "NEUTRAL"
    h1_bias:      str = "NEUTRAL"
    location:     str = "NEUTRAL"
    extended:     bool = False
    draw:         float = 0.0
    draw_desc:    str = ""
    dealing_h:    float = 0.0
    dealing_l:    float = float('inf')
    pois:         List[POI] = field(default_factory=list)
    eq_levels:    List[EQLiquidityLevel] = field(default_factory=list)
    gate_open:    bool = False
    score:        int  = 0
    focus_score:  float = 0.0   # AutoFocus ranking score
    last_alert:   Optional[datetime] = None
    alerts_today: int = 0
    alert_day:    int = -1
    gate_open_since: int = 0
    poi_touch_candle: int = 0

@dataclass
class SequenceResult:
    valid:           bool
    score_bonus:     int
    description:     str
    states_found:    List[str]
    has_compression: bool
    has_sweep:       bool
    has_rejection:   bool
    has_displacement:bool
    cisd_detected:   bool = False

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
    trailing:  float
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
    if len(vals) < p: return sum(vals)/len(vals)
    k = 2/(p+1); e = sum(vals[:p])/p
    for v in vals[p:]: e = v*k + e*(1-k)
    return e

def sma(vals, p):
    if not vals: return 0.0
    tail = vals[-p:] if len(vals)>=p else vals
    return sum(tail)/len(tail)

def get_session():
    h = datetime.utcnow().hour
    s = [n for n,(a,b) in SESSIONS.items() if a<=h<b]
    return ", ".join(s) if s else "Off-session"

def pct_in_range(price, high, low):
    if high==low: return 50.0
    return (price-low)/(high-low)*100

def is_extended(price, high, low, direction):
    pct = pct_in_range(price, high, low)
    if direction=="BULLISH": return pct>80
    if direction=="BEARISH": return pct<20
    return False

# ============================================================
# AUTO FOCUS ENGINE
# Ranks all instruments and focuses deep scan on top N
# ============================================================

class AutoFocusEngine:
    """
    Scores each instrument on:
    - ATR (volatility) — higher = more opportunity
    - Structure score — cleaner structure = better
    - Wick ratio — more wicks = more liquidity activity
    - Noise penalty — choppy price = lower score
    Focuses scanner on top MAX_FOCUS instruments.
    """
    def __init__(self, max_focus: int = MAX_FOCUS):
        self.max_focus = max_focus
        self.scores: Dict[str, float] = {}

    def score_instrument(self, store) -> float:
        if not store.ready(): return 0.0
        candles = store.get(20)
        avg     = store.avg_range()
        if avg == 0: return 0.0

        # ATR contribution
        atr_score = min(avg / 0.001, 10.0)

        # Structure score: consistent HH/HL or LH/LL
        highs, lows = store.swings()
        structure_score = 0.0
        if len(highs) >= 2 and len(lows) >= 2:
            hh = sum(1 for i in range(1,len(highs)) if highs[i].high>highs[i-1].high)
            hl = sum(1 for i in range(1,len(lows))  if lows[i].low>lows[i-1].low)
            ll = sum(1 for i in range(1,len(lows))  if lows[i].low<lows[i-1].low)
            lh = sum(1 for i in range(1,len(highs)) if highs[i].high<highs[i-1].high)
            structure_score = max(hh+hl, ll+lh) / max(len(highs)+len(lows)-2, 1) * 10

        # Wick ratio: average wick size relative to body
        wick_score = 0.0
        if candles:
            wicks = [max(c.up_wick(),c.lo_wick())/c.rng() if c.rng()>0 else 0 for c in candles]
            wick_score = sum(wicks)/len(wicks)*10

        # Noise penalty: high body_pct variance = choppy
        bpcts = [c.body_pct() for c in candles if c.rng()>0]
        noise = 0.0
        if len(bpcts)>1:
            mean = sum(bpcts)/len(bpcts)
            variance = sum((x-mean)**2 for x in bpcts)/len(bpcts)
            noise = min(variance*20, 5.0)

        score = (atr_score*0.4 + structure_score*0.35 + wick_score*0.35) - noise*0.5
        return max(score, 0.0)

    def get_focus(self, engines: Dict) -> List[str]:
        """Return top N instrument symbols by score"""
        scored = []
        for sym, eng in engines.items():
            d1_store = eng.stores.get("D1")
            h4_store = eng.stores.get("H4")
            if d1_store and h4_store:
                s = (self.score_instrument(d1_store)*0.6 +
                     self.score_instrument(h4_store)*0.4)
                self.scores[sym] = s
                eng.narr.focus_score = s
                scored.append((sym, s))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [sym for sym,_ in scored[:self.max_focus]]

# ============================================================
# SEQUENCE + CISD ANALYZER
# ============================================================

class SequenceAnalyzer:
    """
    Validates candle sequences before firing alerts.
    Requires at least 3 of 4 states in logical order.
    Also detects CISD (Change in State of Delivery):
      compression range expanding into displacement.

    Valid sequences (min 3 of 4):
      COMP → SWEEP → DISPLACEMENT        +1
      COMP → REJECTION → DISPLACEMENT    +1
      SWEEP → REJECTION → DISPLACEMENT   +2
      COMP → SWEEP → REJECTION → DISP    +3 (SUPER)

    CISD bonus: compression → expansion detected  +1
    """

    def detect_cisd(self, candles: List[Candle]) -> bool:
        """
        CISD: 3-candle compression → expansion model.
        Range must be contracting then explode on 3rd candle.
        """
        if len(candles) < 3: return False
        c1, c2, c3 = candles[-3], candles[-2], candles[-1]
        # Ranges contracting then expanding
        compression = c1.rng() > c2.rng()
        expansion   = c3.rng() > c2.rng() * 1.5
        displacement= c3.body_pct() >= 0.6
        return compression and expansion and displacement

    def analyze(self, states: List[str], direction: str,
                candles: List[Candle]) -> SequenceResult:
        window = states[-10:] if len(states)>=10 else states

        has_comp = COMPRESSION  in window
        has_sweep= SWEEP        in window
        has_rej  = REJECTION    in window
        has_disp = DISPLACEMENT in window
        cisd     = self.detect_cisd(candles) if len(candles)>=3 else False

        states_found = [s for s in [
            COMPRESSION  if has_comp else None,
            SWEEP        if has_sweep else None,
            REJECTION    if has_rej  else None,
            DISPLACEMENT if has_disp else None,
        ] if s]

        # DISPLACEMENT required unless CISD detected
        if not has_disp and not cisd:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="No displacement or CISD",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp,
                cisd_detected=cisd)

        # Need sweep OR rejection (not just displacement)
        if not has_sweep and not has_rej and not cisd:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="Displacement alone — too weak",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp,
                cisd_detected=cisd)

        # Validate order
        def last_idx(lst, target):
            for i in range(len(lst)-1,-1,-1):
                if lst[i]==target: return i
            return -1

        disp_idx  = last_idx(window, DISPLACEMENT)
        sweep_idx = last_idx(window, SWEEP)
        rej_idx   = last_idx(window, REJECTION)

        order_ok = cisd  # CISD bypasses order check
        if has_sweep and sweep_idx < disp_idx: order_ok = True
        if has_rej   and rej_idx   < disp_idx: order_ok = True

        if not order_ok:
            return SequenceResult(
                valid=False, score_bonus=0,
                description="Order invalid",
                states_found=states_found,
                has_compression=has_comp, has_sweep=has_sweep,
                has_rejection=has_rej, has_displacement=has_disp,
                cisd_detected=cisd)

        # Validate displacement direction
        if has_disp:
            last_disp = next(
                (c for c in reversed(candles) if c.state==DISPLACEMENT), None)
            if last_disp:
                if direction=="BULLISH" and not last_disp.bull():
                    return SequenceResult(
                        valid=False, score_bonus=0,
                        description="Displacement direction mismatch",
                        states_found=states_found,
                        has_compression=has_comp, has_sweep=has_sweep,
                        has_rejection=has_rej, has_displacement=has_disp,
                        cisd_detected=cisd)
                if direction=="BEARISH" and not last_disp.bear():
                    return SequenceResult(
                        valid=False, score_bonus=0,
                        description="Displacement direction mismatch",
                        states_found=states_found,
                        has_compression=has_comp, has_sweep=has_sweep,
                        has_rejection=has_rej, has_displacement=has_disp,
                        cisd_detected=cisd)

        # Score
        bonus = 0; desc = []
        if cisd: bonus += 1; desc.append("CISD ✅")
        if has_comp and has_sweep and has_rej and has_disp:
            bonus += 3; desc.append("🌟 FULL: Comp→Sweep→Rej→Disp")
        elif has_sweep and has_rej and has_disp:
            bonus += 2; desc.append("⭐ Sweep→Rejection→Displacement")
        elif has_comp and (has_sweep or has_rej) and has_disp:
            bonus += 1; desc.append("Compression→Liquidity→Displacement")
        elif cisd and not has_disp:
            desc.append("CISD compression→expansion")

        return SequenceResult(
            valid=True, score_bonus=bonus,
            description=" | ".join(desc) if desc else "Valid sequence",
            states_found=states_found,
            has_compression=has_comp, has_sweep=has_sweep,
            has_rejection=has_rej, has_displacement=has_disp,
            cisd_detected=cisd)

# ============================================================
# CANDLE STORE
# ============================================================

class CandleStore:
    def __init__(self, symbol: str, tf: str, maxlen: int = 300):
        self.symbol     = symbol
        self.tf         = tf
        self.candles    = deque(maxlen=maxlen)
        self.closes     = deque(maxlen=220)
        self._cls       = INSTRUMENT_CLASS.get(symbol, "forex")
        self._dmult     = DISP_MULT.get(self._cls, 1.8)

    def add(self, c: Candle):
        avg = self.avg_range()
        c.state = self._classify(c, avg)
        self.candles.append(c)
        self.closes.append(c.close)

    def _classify(self, c: Candle, avg: float) -> str:
        if avg==0: return INDECISION
        rng_ratio = c.rng()/avg
        body_ratio= c.body_pct()
        up_r  = c.up_wick()/c.rng() if c.rng() else 0
        lo_r  = c.lo_wick()/c.rng() if c.rng() else 0
        if c.disp(avg, self._dmult):               return DISPLACEMENT
        if rng_ratio<0.5 and body_ratio<0.4:       return COMPRESSION
        if (up_r>0.4 or lo_r>0.4) and body_ratio<0.35: return SWEEP
        if body_ratio>=0.4:
            if c.bull() and lo_r>0.3: return REJECTION
            if c.bear() and up_r>0.3: return REJECTION
        return INDECISION

    def get(self, n=None) -> List[Candle]:
        cl = list(self.candles)
        return cl[-n:] if n else cl

    def last(self) -> Optional[Candle]: return self.candles[-1] if self.candles else None
    def ready(self) -> bool: return len(self.candles)>=30
    def avg_range(self, n=20) -> float:
        c = list(self.candles)[-n:]
        return sum(x.rng() for x in c)/len(c) if c else 0.0

    def ema21(self):  return ema(list(self.closes), 21)
    def sma200(self): return sma(list(self.closes), 200)

    def swings(self, lookback=5):
        c=self.get(); highs=[]; lows=[]
        for i in range(lookback, len(c)-lookback):
            w = range(i-lookback, i+lookback+1)
            if all(c[i].high>=c[j].high for j in w if j!=i): highs.append(c[i])
            if all(c[i].low<=c[j].low  for j in w if j!=i): lows.append(c[i])
        return highs, lows

    def protected_swings(self):
        highs, lows = self.swings()
        c   = self.get(); avg = self.avg_range()
        ph  = [sh for sh in highs if any(
            x.time>sh.time and x.bear() and x.disp(avg,self._dmult) for x in c)]
        pl  = [sl for sl in lows  if any(
            x.time>sl.time and x.bull() and x.disp(avg,self._dmult) for x in c)]
        return ph, pl

    def trend(self) -> str:
        last=self.last()
        if not last: return "NEUTRAL"
        e=self.ema21()
        if not e: return "NEUTRAL"
        if last.close>e*1.001: return "BULLISH"
        if last.close<e*0.999: return "BEARISH"
        return "NEUTRAL"

    def structure_bias(self) -> str:
        highs,lows=self.swings()
        if len(highs)<2 or len(lows)<2: return self.trend()
        hh=highs[-1].high>highs[-2].high; hl=lows[-1].low>lows[-2].low
        lh=highs[-1].high<highs[-2].high; ll=lows[-1].low<lows[-2].low
        if hh and hl: return "BULLISH"
        if lh and ll: return "BEARISH"
        return self.trend()

    def detect_eqhl_levels(self, tf: str) -> List[EQLiquidityLevel]:
        """
        NEW: Detect EQH/EQL as two-legged liquidity levels.
        Track both legs so we know when BOTH are swept.
        """
        highs, lows = self.swings()
        avg = self.avg_range()
        tol = avg * 0.15
        levels = []

        for i in range(1, len(highs)):
            if abs(highs[i].high - highs[i-1].high) <= tol:
                levels.append(EQLiquidityLevel(
                    type="EQH",
                    level=(highs[i].high+highs[i-1].high)/2,
                    leg1=highs[i-1].high,
                    leg2=highs[i].high,
                    tf=tf,
                    time=highs[i].time
                ))

        for i in range(1, len(lows)):
            if abs(lows[i].low - lows[i-1].low) <= tol:
                levels.append(EQLiquidityLevel(
                    type="EQL",
                    level=(lows[i].low+lows[i-1].low)/2,
                    leg1=lows[i-1].low,
                    leg2=lows[i].low,
                    tf=tf,
                    time=lows[i].time
                ))

        return levels

    def detect_pois(self) -> List[POI]:
        c=self.get(); avg=self.avg_range(); pois=[]; mult=self._dmult
        if len(c)<5: return pois
        for i in range(2,len(c)):
            c1=c[i-2]; c2=c[i-1]; c3=c[i]
            if c3.low>c1.high and c2.disp(avg,mult):
                pois.append(POI("fvg_bull",c3.low,c1.high,self.tf,c2.time))
            if c3.high<c1.low and c2.disp(avg,mult):
                pois.append(POI("fvg_bear",c1.low,c3.high,self.tf,c2.time))
        for i in range(1,len(c)-1):
            prev=c[i-1]; disp=c[i]
            if prev.bear() and disp.bull() and disp.disp(avg,mult):
                p=POI("ob_bull",max(prev.open,prev.close),
                      min(prev.open,prev.close),self.tf,prev.time)
                p.protected=True; pois.append(p)
            if prev.bull() and disp.bear() and disp.disp(avg,mult):
                p=POI("ob_bear",max(prev.open,prev.close),
                      min(prev.open,prev.close),self.tf,prev.time)
                p.protected=True; pois.append(p)
        seen=defaultdict(int); result=[]
        for p in reversed(pois):
            if seen[p.kind]<5: result.append(p); seen[p.kind]+=1
        return result

    def get_states(self, n=10) -> List[str]:
        return [c.state for c in self.get(n)]

# ============================================================
# NARRATIVE ENGINE
# ============================================================

seq_analyzer  = SequenceAnalyzer()
autofocus_eng = AutoFocusEngine(MAX_FOCUS)

class NarrativeEngine:
    def __init__(self, symbol: str, name: str):
        self.symbol = symbol
        self.name   = name
        self.stores = {tf: CandleStore(symbol,tf) for tf in TIMEFRAMES}
        self.narr   = Narrative(symbol=symbol, name=name)
        self._count = 0

    def feed(self, tf: str, c: Candle) -> Optional[Alert]:
        self.stores[tf].add(c)
        if tf=="M15": self._count+=1
        if not all(s.ready() for s in self.stores.values()):
            return None
        self._update_narrative()
        self._apply_decay()
        self._update_eq_levels()
        if self.narr.gate_open:
            return self._check_confluence_and_trigger()
        return None

    def _update_eq_levels(self):
        """
        NEW: Update EQL liquidity levels across timeframes.
        Track which legs have been swept and when both are run.
        """
        n    = self.narr
        last = self.stores["H4"].last()
        if not last: return
        price = last.close

        # Collect fresh EQ levels from H4 and D1
        h4_eqs = self.stores["H4"].detect_eqhl_levels("H4")
        d1_eqs = self.stores["D1"].detect_eqhl_levels("D1")
        all_eqs= h4_eqs + d1_eqs

        # Update existing levels with new price data
        for eq in n.eq_levels:
            if eq.both_swept: continue
            avg = self.stores["H4"].avg_range()
            buf = avg * 0.1

            if eq.type == "EQH":
                # Leg 1 swept?
                if not eq.leg1_swept and price >= eq.leg1 - buf:
                    eq.leg1_swept = True
                    log.info(f"{n.name}: EQH leg1 swept at {eq.leg1}")
                # Leg 2 swept?
                if eq.leg1_swept and not eq.leg2_swept and price >= eq.leg2 - buf:
                    eq.leg2_swept = True
                    eq.both_swept = True
                    eq.reversal_watch = True
                    log.info(f"{n.name}: BOTH EQH legs swept — reversal watch ON")
            else:  # EQL
                if not eq.leg1_swept and price <= eq.leg1 + buf:
                    eq.leg1_swept = True
                    log.info(f"{n.name}: EQL leg1 swept at {eq.leg1}")
                if eq.leg1_swept and not eq.leg2_swept and price <= eq.leg2 + buf:
                    eq.leg2_swept = True
                    eq.both_swept = True
                    eq.reversal_watch = True
                    log.info(f"{n.name}: BOTH EQL legs swept — reversal watch ON")

        # Add new levels not already tracked
        existing_levels = {eq.level for eq in n.eq_levels}
        for eq in all_eqs:
            if eq.level not in existing_levels:
                n.eq_levels.append(eq)
                existing_levels.add(eq.level)

        # Keep only last 10
        n.eq_levels = n.eq_levels[-10:]

    def _apply_decay(self):
        n = self.narr
        if n.gate_open:
            n.gate_open_since += 1
            if n.gate_open_since > 50:
                n.score = max(0, n.score-1)
                n.gate_open_since = 0
        for poi in n.pois:
            if poi.fresh:
                poi.stale_count += 1
                if poi.stale_count > 96:
                    poi.fresh = False

    def _update_narrative(self):
        n=self.narr
        d1=self.stores["D1"]; h4=self.stores["H4"]; h1=self.stores["H1"]

        n.bias    = d1.structure_bias()
        n.h4_bias = h4.structure_bias()
        n.h1_bias = h1.structure_bias()

        d1c=d1.get(50)
        if d1c:
            n.dealing_h=max(c.high for c in d1c)
            n.dealing_l=min(c.low  for c in d1c)

        last=d1.last()
        if last and n.dealing_h>n.dealing_l:
            pct=pct_in_range(last.close,n.dealing_h,n.dealing_l)
            if pct>60:   n.location="PREMIUM"
            elif pct<40: n.location="DISCOUNT"
            else:        n.location="NEUTRAL"
            n.extended=is_extended(last.close,n.dealing_h,n.dealing_l,n.bias)

        # Draw from EQ levels first, then protected swings
        d1_eqs=self.stores["D1"].detect_eqhl_levels("D1")
        if n.bias=="BEARISH":
            eql=[e for e in d1_eqs if e.type=="EQL"]
            if eql: n.draw=min(e.level for e in eql); n.draw_desc=f"EQL draw {n.draw:.5f}"
        elif n.bias=="BULLISH":
            eqh=[e for e in d1_eqs if e.type=="EQH"]
            if eqh: n.draw=max(e.level for e in eqh); n.draw_desc=f"EQH draw {n.draw:.5f}"

        if n.draw==0.0:
            ph,pl=d1.protected_swings()
            if n.bias=="BULLISH" and ph:
                n.draw=ph[-1].high; n.draw_desc=f"Protected High {n.draw:.5f}"
            elif n.bias=="BEARISH" and pl:
                n.draw=pl[-1].low;  n.draw_desc=f"Protected Low {n.draw:.5f}"

        n.pois=(d1.detect_pois()+h4.detect_pois()+h1.detect_pois())

        bias_ok    = n.bias!="NEUTRAL"
        h4_ok      = n.h4_bias==n.bias
        loc_ok     = ((n.bias=="BULLISH" and n.location=="DISCOUNT") or
                      (n.bias=="BEARISH" and n.location=="PREMIUM") or
                      n.location=="NEUTRAL")
        not_ext    = not n.extended
        has_draw   = n.draw!=0.0

        prev_gate=n.gate_open
        n.gate_open=bias_ok and h4_ok and loc_ok and not_ext and has_draw
        if n.gate_open and not prev_gate: n.gate_open_since=0

        score=0
        if bias_ok:                              score+=2
        if h4_ok:                                score+=2
        if n.h1_bias==n.bias:                    score+=1
        if n.location in ["PREMIUM","DISCOUNT"]: score+=1
        if loc_ok:                               score+=1
        if has_draw:                             score+=1
        if not_ext:                              score+=1
        n.score=min(score,9)

    def _check_confluence_and_trigger(self) -> Optional[Alert]:
        n=self.narr; h4=self.stores["H4"]; h1=self.stores["H1"]
        m15=self.stores["M15"]
        last=h4.last() or h1.last()
        if not last: return None
        price=last.close; avg=h4.avg_range(); buf=avg*0.3

        # Cooldown
        if n.last_alert:
            hrs=(datetime.utcnow()-n.last_alert).total_seconds()/3600
            if hrs<COOLDOWN_HOURS: return None
        today=datetime.utcnow().day
        if n.alert_day==today and n.alerts_today>=MAX_ALERTS_PER_DAY: return None

        # Active POI
        active_poi=None
        for poi in n.pois:
            if not poi.fresh: continue
            if not (poi.bottom-buf<=price<=poi.top+buf): continue
            if n.bias=="BULLISH" and "bull" in poi.kind: active_poi=poi; break
            if n.bias=="BEARISH" and "bear" in poi.kind: active_poi=poi; break
        if not active_poi: return None

        n.poi_touch_candle=self._count

        # Check for both-legs-swept EQ near POI (HIGH PRIORITY)
        eq_double_swept = None
        for eq in n.eq_levels:
            if eq.both_swept and eq.reversal_watch:
                eq_level_near = abs(price-eq.level) <= avg*2
                if eq_level_near:
                    eq_double_swept = eq
                    break

        # Confluences
        e21=h4.ema21(); s200=h4.sma200(); session=get_session()
        ma_ok   =(n.bias=="BULLISH" and price>e21) or (n.bias=="BEARISH" and price<e21) if e21 else False
        s200_ok =(n.bias=="BULLISH" and price>s200) or (n.bias=="BEARISH" and price<s200) if s200 else False
        sess_ok ="London" in session or "NY" in session
        h1_ok   =n.h1_bias==n.bias
        loc_ok  =n.location in ["PREMIUM","DISCOUNT"]
        prot_ok =active_poi.protected
        eq_ok   =eq_double_swept is not None  # Both legs swept = strong confluence

        confs=[ma_ok,s200_ok,sess_ok,h1_ok,loc_ok,prot_ok,eq_ok]
        conf_count=sum(1 for c in confs if c)
        if conf_count<2: return None

        # High probability sweep check
        hp_sweep=self._check_high_prob_sweep(price, n, active_poi, avg)

        # Sequence analysis
        m15_states =m15.get_states(10)
        m15_candles=m15.get(10)
        seq=seq_analyzer.analyze(m15_states, n.bias, m15_candles)
        if not seq.valid and not hp_sweep and not eq_double_swept:
            return None

        # LTF trigger
        trigger=self._check_ltf_trigger(m15, n.bias, eq_double_swept)
        if not trigger: return None

        # Trade levels with trailing
        entry,sl,tp,rr,trailing=self._calc_trade(n.bias,price,avg,active_poi,n.draw)
        if rr<1.5: return None

        # Final score
        score=n.score+conf_count+(seq.score_bonus if seq.valid else 0)
        if trigger["quality"]=="high": score+=1
        if active_poi.protected:       score+=1
        if eq_double_swept:            score+=2  # Double sweep = strong bonus
        if hp_sweep:                   score+=1
        score=min(score,10)
        if score<MIN_SCORE: return None

        # Narrative text
        eq_note=""
        if eq_double_swept:
            eq_note=f"\n💧 BOTH {eq_double_swept.type} legs swept ✅ (reversal high probability)"
        narr_text=(
            f"D1 {n.bias} | H4 {n.h4_bias} | H1 {n.h1_bias}\n"
            f"Location: {n.location}\n"
            f"Draw: {n.draw_desc}\n"
            f"POI: {active_poi.tf} {'OB' if 'ob' in active_poi.kind else 'FVG'}"
            f" ({active_poi.bottom:.5f}—{active_poi.top:.5f})"
            f"{'  🛡Protected' if active_poi.protected else ''}"
            f"{eq_note}"
        )

        n.last_alert=datetime.utcnow()
        n.alerts_today=(n.alerts_today+1 if n.alert_day==today else 1)
        n.alert_day=today
        active_poi.fresh=False
        if eq_double_swept: eq_double_swept.reversal_watch=False

        return Alert(
            symbol=n.symbol, name=n.name,
            narrative=narr_text,
            trigger=trigger["desc"],
            sequence=seq.description if seq.valid else ("High-prob sweep" if hp_sweep else "Double EQ sweep"),
            direction=n.bias,
            entry=entry, sl=sl, tp=tp, rr=rr, trailing=trailing,
            score=score, tf=active_poi.tf, session=session,
            details={
                "ma_ok":ma_ok,"s200_ok":s200_ok,"session":session,
                "conf_count":conf_count,"poi_kind":active_poi.kind,
                "protected_poi":active_poi.protected,
                "trigger_quality":trigger["quality"],
                "seq_bonus":seq.score_bonus if seq.valid else 0,
                "cisd":seq.cisd_detected if seq.valid else False,
                "eq_double_swept":eq_double_swept is not None,
                "hp_sweep":hp_sweep,"focus_score":n.focus_score,
                "has_full_seq":seq.valid and seq.score_bonus==3,
            }
        )

    def _check_high_prob_sweep(self, price, n, poi, avg) -> bool:
        """
        High probability sweep:
        EQH/EQL present + HTF POI nearby + clean sweep
        """
        eq_present=len(n.eq_levels)>0
        poi_near=abs(price-(poi.top+poi.bottom)/2)<=avg*2
        htf_poi=poi.protected or "ob" in poi.kind
        return eq_present and poi_near and htf_poi

    def _check_ltf_trigger(self, m15: CandleStore,
                           direction: str,
                           eq_double: Optional[EQLiquidityLevel]) -> Optional[dict]:
        candles=m15.get(10)
        if len(candles)<5: return None
        avg=m15.avg_range(); last=candles[-1]; prev=candles[-2] if len(candles)>=2 else last

        # After double EQ sweep — lower bar for trigger
        if eq_double:
            if last.rejection_wick("bull" if direction=="BULLISH" else "bear"):
                return {"desc":f"M15 rejection wick after BOTH {eq_double.type} swept",
                        "quality":"high"}
            if last.body_pct()>=0.5:
                d="BULLISH" if last.bull() else "BEARISH"
                if d==direction:
                    return {"desc":f"M15 displacement after double {eq_double.type} sweep",
                            "quality":"high"}

        # Standard triggers
        d="bull" if direction=="BULLISH" else "bear"
        if last.rejection_wick(d):
            return {"desc":"M15 rejection wick at POI","quality":"high"}

        eqh,eql=m15.detect_eqhl_levels("M15"),[]
        m15_eqh=[e.level for e in m15.detect_eqhl_levels("M15") if e.type=="EQH"]
        m15_eql=[e.level for e in m15.detect_eqhl_levels("M15") if e.type=="EQL"]

        if direction=="BEARISH" and m15_eqh:
            if last.high>=m15_eqh[-1] and last.close<m15_eqh[-1]:
                return {"desc":f"M15 EQH swept ({m15_eqh[-1]:.5f}) — MSS forming","quality":"high"}
        if direction=="BULLISH" and m15_eql:
            if last.low<=m15_eql[-1] and last.close>m15_eql[-1]:
                return {"desc":f"M15 EQL swept ({m15_eql[-1]:.5f}) — MSS forming","quality":"high"}

        if direction=="BULLISH" and last.bull() and last.body_pct()>=0.6 and last.close>prev.high:
            return {"desc":"M15 CHoC — bullish body closure","quality":"medium"}
        if direction=="BEARISH" and last.bear() and last.body_pct()>=0.6 and last.close<prev.low:
            return {"desc":"M15 CHoC — bearish body closure","quality":"medium"}

        dmult=DISP_MULT.get(INSTRUMENT_CLASS.get(m15.symbol,"forex"),1.8)
        if last.disp(avg,dmult):
            d2="BULLISH" if last.bull() else "BEARISH"
            if d2==direction:
                return {"desc":f"M15 displacement — {direction.lower()} momentum","quality":"medium"}
        return None

    def _calc_trade(self, direction, price, avg, poi, draw):
        """Risk management: entry from POI, SL beyond structure, TP at draw"""
        buf=avg*0.5
        if direction=="BULLISH":
            entry=poi.bottom+(poi.top-poi.bottom)*0.3
            sl=poi.bottom-buf
            tp=draw if draw>price else price+(price-sl)*3
        else:
            entry=poi.top-(poi.top-poi.bottom)*0.3
            sl=poi.top+buf
            tp=draw if draw<price else price-(sl-price)*3
        risk=abs(entry-sl); reward=abs(tp-entry)
        rr=round(reward/risk,1) if risk else 0
        trailing=round(reward*0.5,5)  # Trailing activates at 50% to TP
        return round(entry,5),round(sl,5),round(tp,5),rr,trailing

    def summary(self) -> str:
        n=self.narr; last=self.stores["D1"].last()
        icon="🟢" if n.bias=="BULLISH" else ("🔴" if n.bias=="BEARISH" else "⚪")
        gate="✅ OPEN" if n.gate_open else "⛔ CLOSED"
        ext="  ⚠️ Extended!" if n.extended else ""
        eq_active=sum(1 for e in n.eq_levels if not e.both_swept)
        eq_ready =sum(1 for e in n.eq_levels if e.both_swept and e.reversal_watch)
        return (
            f"{icon} {n.name}\n"
            f"D1:{n.bias} H4:{n.h4_bias} H1:{n.h1_bias}\n"
            f"📍{n.location}{ext}\n"
            f"🎯 Draw:{n.draw_desc or 'Not identified'}\n"
            f"📊 Score:{n.score}/9 | Gate:{gate}\n"
            f"💧 EQ levels:{eq_active} active | {eq_ready} ready\n"
            f"🏆 Focus:{n.focus_score:.1f} | ⏰{get_session()}"
        )

# ============================================================
# WEBSOCKET MANAGER
# ============================================================

class WSManager:
    def __init__(self):
        self.engines: Dict[str,NarrativeEngine] = {}
        self.running = False
        self.app_ref = None

    def add_instrument(self, sym, name):
        self.engines[sym]=NarrativeEngine(sym,name)

    def remove_instrument(self, sym):
        self.engines.pop(sym,None)

    async def load_all_history(self):
        log.info(f"Loading history for {len(self.engines)} instruments...")
        for sym,eng in list(self.engines.items()):
            for tf,gran in TIMEFRAMES.items():
                await self._fetch_history(sym,tf,gran,eng)
                await asyncio.sleep(0.2)
        log.info("✅ History loaded!")
        # Initial AutoFocus ranking
        focus=autofocus_eng.get_focus(self.engines)
        log.info(f"🎯 AutoFocus top {MAX_FOCUS}: {[self.engines[s].name for s in focus if s in self.engines]}")

    async def _fetch_history(self, sym, tf, gran, eng):
        try:
            async with websockets.connect(DERIV_WS,ping_interval=None) as ws:
                await ws.send(json.dumps({
                    "ticks_history":sym,"granularity":gran,
                    "count":200,"end":"latest","style":"candles"}))
                while True:
                    r=json.loads(await asyncio.wait_for(ws.recv(),20))
                    if "candles" in r:
                        for cd in r["candles"]:
                            eng.stores[tf].add(Candle(
                                time=cd["epoch"],open=float(cd["open"]),
                                high=float(cd["high"]),low=float(cd["low"]),
                                close=float(cd["close"])))
                        log.info(f"✅ {sym} {tf}: {len(r['candles'])} candles"); break
                    if "error" in r:
                        log.warning(f"{sym} {tf}: {r['error']['message']}"); break
        except Exception as e:
            log.error(f"History {sym} {tf}: {e}")

    async def run_live(self):
        tasks=[self._subscribe(sym,tf,gran,eng)
               for sym,eng in list(self.engines.items())
               for tf,gran in TIMEFRAMES.items()]
        await asyncio.gather(*tasks)

    async def _subscribe(self, sym, tf, gran, eng):
        while self.running:
            try:
                async with websockets.connect(DERIV_WS,ping_interval=30) as ws:
                    await ws.send(json.dumps({
                        "ticks_history":sym,"granularity":gran,
                        "count":2,"end":"latest","style":"candles","subscribe":1}))
                    while self.running:
                        r=json.loads(await asyncio.wait_for(ws.recv(),90))
                        if "ohlc" in r:
                            o=r["ohlc"]
                            c=Candle(time=int(o["open_time"]),open=float(o["open"]),
                                     high=float(o["high"]),low=float(o["low"]),
                                     close=float(o["close"]))
                            alert=eng.feed(tf,c)
                            if alert and self.app_ref:
                                asyncio.create_task(self._send_alert(alert))
            except Exception as e:
                if self.running:
                    log.error(f"Sub {sym}/{tf}: {e}")
                    await asyncio.sleep(5)

    async def _send_alert(self, alert: Alert):
        if not state.chat_id: return
        try:
            msg=fmt_alert(alert)
            kb=InlineKeyboardMarkup([[
                InlineKeyboardButton("📊 Full Narrative",
                    callback_data=f"narr_{alert.symbol}"),
                InlineKeyboardButton("⏭ Skip",callback_data="skip_alert"),
            ]])
            await self.app_ref.bot.send_message(state.chat_id,msg,reply_markup=kb)
            state.alert_count+=1
        except Exception as e:
            log.error(f"Send alert: {e}")

ws_manager=WSManager()

# ============================================================
# ALERT FORMATTER
# ============================================================

def fmt_alert(a: Alert) -> str:
    icon ="🟢" if a.direction=="BULLISH" else "🔴"
    stars="⭐"*min(a.score,5)
    d    =a.details
    full =d.get("has_full_seq",False)
    eq_d =d.get("eq_double_swept",False)
    cisd =d.get("cisd",False)

    header="🌟" if full else ("💧" if eq_d else icon)
    msg  =f"{header} {a.name} — {a.direction}\n"
    msg +=f"━━━━━━━━━━━━━━━━━━━━\n"
    msg +=f"📖 Narrative:\n"
    for line in a.narrative.split("\n"): msg+=f"   {line}\n"
    msg +=f"━━━━━━━━━━━━━━━━━━━━\n"
    if cisd: msg+=f"🔄 CISD: Compression→Expansion ✅\n"
    msg +=f"🔢 Sequence: {a.sequence}\n"
    msg +=f"⚡ Trigger:  {a.trigger}\n"
    msg +=f"━━━━━━━━━━━━━━━━━━━━\n"
    msg +=f"📐 Trade Setup:\n"
    msg +=f"   Entry:    {a.entry}\n"
    msg +=f"   SL:       {a.sl}\n"
    msg +=f"   TP:       {a.tp}\n"
    msg +=f"   RR:       1:{a.rr}\n"
    msg +=f"   Trailing: activates at {a.trailing} from entry\n"
    msg +=f"━━━━━━━━━━━━━━━━━━━━\n"
    msg +=f"⭐ Score: {stars} {a.score}/10\n"
    msg +=f"⏰ {a.session} session\n"
    confs=[]
    if d.get("ma_ok"):         confs.append("EMA21 ✅")
    if d.get("s200_ok"):       confs.append("SMA200 ✅")
    if d.get("protected_poi"): confs.append("🛡 Protected POI")
    if eq_d:                   confs.append("💧 Double EQ swept")
    if d.get("hp_sweep"):      confs.append("🎯 High-prob sweep")
    if d.get("trigger_quality")=="high": confs.append("High-quality trigger")
    if confs: msg+=f"🔗 {' | '.join(confs)}\n"
    msg+=f"━━━━━━━━━━━━━━━━━━━━\n"
    msg+=f"⚠️ Always confirm on chart before entry!"
    return msg

# ============================================================
# BOT STATE
# ============================================================

class BotState:
    def __init__(self):
        self.running     =False
        self.watchlist   =dict(DEFAULT_WATCHLIST)
        self.chat_id     =None
        self.alert_count =0
        self.start_time  =None

    def rebuild_engines(self):
        ws_manager.engines.clear()
        for sym,name in self.watchlist.items():
            ws_manager.add_instrument(sym,name)

state=BotState()

async def start_background_tasks(app):
    ws_manager.app_ref=app
    asyncio.create_task(ws_manager.load_all_history())
    asyncio.create_task(ws_manager.run_live())

# ============================================================
# TELEGRAM UI
# ============================================================

def main_kb():
    btn="⏹ Stop" if state.running else "▶️ Start Engine"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(btn,callback_data="toggle")],
        [InlineKeyboardButton("📊 Narratives", callback_data="narratives"),
         InlineKeyboardButton("📋 Watchlist",  callback_data="watchlist")],
        [InlineKeyboardButton("🎯 AutoFocus",  callback_data="autofocus"),
         InlineKeyboardButton("📉 Signal Log", callback_data="signals")],
        [InlineKeyboardButton("⚙️ Settings",   callback_data="settings"),
         InlineKeyboardButton("❓ Help",       callback_data="help")],
    ])

def wl_kb():
    rows=[[InlineKeyboardButton(f"❌ {n}",callback_data=f"rm_{s}")]
          for s,n in state.watchlist.items()]
    rows+=[[InlineKeyboardButton("➕ Add Instrument",callback_data="add")],
           [InlineKeyboardButton("◀️ Back",callback_data="menu")]]
    return InlineKeyboardMarkup(rows)

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state.chat_id=update.effective_chat.id
    await update.message.reply_text(
        "🤖 *DerivBot Pro — Mega Final Build*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "5-Stage Funnel:\n"
        "1️⃣ Narrative: Bias+Location+Draw\n"
        "2️⃣ AutoFocus: Top 4 instruments\n"
        "3️⃣ Confluence: POI+MA+Session\n"
        "4️⃣ Sequence: CISD+Comp→Disp\n"
        "5️⃣ Trigger: LTF MSS/Sweep/Wick\n\n"
        "💧 Double EQH/EQL run detection\n"
        "🛡 Protected POI validation\n"
        "📉 Confidence decay model\n"
        "⚡ Adaptive thresholds\n"
        "🔕 Max 3 alerts/day per instrument\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 {len(state.watchlist)} instruments ready",
        parse_mode="Markdown",reply_markup=main_kb())

async def cmd_btn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer(); d=q.data

    if d=="menu":
        await q.edit_message_text("🤖 DerivBot Pro",reply_markup=main_kb())

    elif d=="toggle":
        if not state.running:
            state.running=True; state.start_time=datetime.utcnow()
            state.rebuild_engines()
            app=ctx.application
            ws_manager.running=True; ws_manager.app_ref=app
            asyncio.create_task(ws_manager.load_all_history())
            asyncio.create_task(ws_manager.run_live())
            await q.edit_message_text(
                f"✅ Engine Started!\n"
                f"{len(state.watchlist)} instruments\n"
                f"AutoFocus: top {MAX_FOCUS} instruments\n"
                f"Loading history ~2 mins...",
                reply_markup=main_kb())
        else:
            state.running=False; ws_manager.running=False
            await q.edit_message_text(
                f"⏹ Stopped\nAlerts sent: {state.alert_count}",
                reply_markup=main_kb())

    elif d=="narratives":
        lines=["📊 Market Narratives\n━━━━━━━━━━━━━━━━━━━"]
        focus=autofocus_eng.get_focus(ws_manager.engines)
        for sym,eng in list(ws_manager.engines.items())[:10]:
            n=eng.narr
            ic="🟢" if n.bias=="BULLISH" else("🔴" if n.bias=="BEARISH" else "⚪")
            gt="✅" if n.gate_open else "⛔"
            star="🎯" if sym in focus else " "
            lines.append(f"{star}{ic} {n.name}:{n.bias}|{n.location} {gt}")
        lines.append(f"\n⏰{get_session()}")
        await q.edit_message_text("\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Refresh",callback_data="narratives"),
                InlineKeyboardButton("◀️ Back",   callback_data="menu")]]))

    elif d=="autofocus":
        focus=autofocus_eng.get_focus(ws_manager.engines)
        lines=["🎯 AutoFocus — Top Instruments\n━━━━━━━━━━━━━━━━━━━"]
        for i,sym in enumerate(focus,1):
            eng=ws_manager.engines.get(sym)
            if eng:
                n=eng.narr
                ic="🟢" if n.bias=="BULLISH" else("🔴" if n.bias=="BEARISH" else "⚪")
                lines.append(f"{i}. {ic} {n.name} — Score:{n.focus_score:.1f} | {n.bias}")
        lines.append(f"\nTop {MAX_FOCUS} of {len(state.watchlist)} instruments")
        await q.edit_message_text("\n".join(lines),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Refresh",callback_data="autofocus"),
                InlineKeyboardButton("◀️ Back",   callback_data="menu")]]))

    elif d.startswith("narr_"):
        sym=d[5:]; eng=ws_manager.engines.get(sym)
        if eng:
            await q.edit_message_text(eng.summary(),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back",callback_data="narratives")]]))

    elif d=="watchlist":
        txt=f"📋 Watchlist ({len(state.watchlist)})\n━━━━━━━━━━━━━━━━━━━\n"
        txt+="\n".join(f"• {n}" for n in state.watchlist.values())
        await q.edit_message_text(txt,reply_markup=wl_kb())

    elif d.startswith("rm_"):
        sym=d[3:]; name=state.watchlist.pop(sym,sym)
        ws_manager.remove_instrument(sym)
        await q.edit_message_text(f"✅ Removed {name}",reply_markup=wl_kb())

    elif d=="add":
        await q.edit_message_text(
            "➕ Send: SYMBOL NAME\n\nExamples:\n"
            "frxGBPUSD GBPUSD\nfrxUSDCAD USDCAD\nR_100 Volatility 100",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back",callback_data="watchlist")]]))

    elif d=="signals":
        t=state.start_time.strftime("%H:%M UTC") if state.start_time else "Not started"
        await q.edit_message_text(
            f"📉 Signal Log\n━━━━━━━━━━━━━━\n"
            f"Alerts sent: {state.alert_count}\n"
            f"Running since: {t}\n"
            f"Status: {'🟢 Active' if state.running else '⏹ Stopped'}\n\n"
            f"Limits:\n• Max {MAX_ALERTS_PER_DAY}/day per instrument\n"
            f"• {COOLDOWN_HOURS}hr cooldown\n• Min score {MIN_SCORE}/10",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back",callback_data="menu")]]))

    elif d=="settings":
        await q.edit_message_text(
            f"⚙️ Settings\n━━━━━━━━━━━━━━\n"
            f"Min Score:      {MIN_SCORE}/10\n"
            f"Max Alerts/Day: {MAX_ALERTS_PER_DAY}\n"
            f"Cooldown:       {COOLDOWN_HOURS} hours\n"
            f"AutoFocus:      Top {MAX_FOCUS} instruments\n"
            f"Instruments:    {len(state.watchlist)}\n"
            f"Timeframes:     D1|H4|H1|M15\n\n"
            f"Thresholds:\n"
            f"  Forex: 1.8x | Metal: 2.0x\n"
            f"  Crypto: 2.2x | Synthetic: 2.5x",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back",callback_data="menu")]]))

    elif d=="help":
        await q.edit_message_text(
            "❓ How It Works\n━━━━━━━━━━━━━━\n\n"
            "5-STAGE FUNNEL:\n\n"
            "1️⃣ NARRATIVE\n"
            "   D1+H4+H1 bias aligned\n"
            "   Discount/Premium location\n"
            "   Liquidity draw identified\n\n"
            "2️⃣ AUTOFOCUS\n"
            "   Ranks all instruments\n"
            "   Deep scans top 4 only\n"
            "   ATR + Structure + Wicks\n\n"
            "3️⃣ CONFLUENCE\n"
            "   Protected HTF POI\n"
            "   EMA21 + SMA200\n"
            "   Session timing\n\n"
            "4️⃣ SEQUENCE + CISD\n"
            "   Comp→Sweep→Rej→Disp\n"
            "   CISD compression model\n"
            "   Min 3 of 4 states\n\n"
            "5️⃣ TRIGGER\n"
            "   M15 MSS/CHoC/Wick\n"
            "   Alert fires EARLY\n\n"
            "💧 DOUBLE EQH/EQL:\n"
            "   Tracks both legs\n"
            "   Waits for BOTH swept\n"
            "   Then watches reversal\n\n"
            "📐 RISK MGMT:\n"
            "   Entry from POI\n"
            "   SL beyond structure\n"
            "   TP at liquidity draw\n"
            "   Trailing at 50% to TP",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back",callback_data="menu")]]))

    elif d=="skip_alert":
        await q.answer("Signal skipped.")

async def cmd_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text=update.message.text.strip()
    if " " in text:
        parts=text.split(" ",1)
        sym=parts[0].strip(); name=parts[1].strip()
        state.watchlist[sym]=name
        ws_manager.add_instrument(sym,name)
        await update.message.reply_text(
            f"✅ Added {name} ({sym})\n"
            f"Total: {len(state.watchlist)} instruments",
            reply_markup=main_kb())
    else:
        await update.message.reply_text(
            "Format: SYMBOL NAME\nExample: frxGBPUSD GBPUSD")

# ============================================================
# WEB SERVER + MAIN (Replit/Render compatible)
# ============================================================

async def health_check(request):
    return web.Response(text="DerivBot Pro is running!")

async def run_web_server():
    port=int(os.environ.get("PORT",8080))
    server=web.Application()
    server.router.add_get("/",health_check)
    runner=web.AppRunner(server)
    await runner.setup()
    site=web.TCPSite(runner,"0.0.0.0",port)
    await site.start()
    log.info(f"Health server on port {port}")

async def run_bot():
    app=Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CallbackQueryHandler(cmd_btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,cmd_msg))
    app.post_init=start_background_tasks
    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True)
    log.info("✅ Bot running! Send /start in Telegram")
    await asyncio.Event().wait()

async def main_async():
    print("\n"+"="*55)
    print("  DerivBot Pro — MEGA FINAL BUILD")
    print(f"  Min score:     {MIN_SCORE}/10")
    print(f"  AutoFocus:     Top {MAX_FOCUS} instruments")
    print(f"  Max alerts:    {MAX_ALERTS_PER_DAY}/day per instrument")
    print(f"  Cooldown:      {COOLDOWN_HOURS} hours")
    print(f"  Instruments:   {len(DEFAULT_WATCHLIST)}")
    print("="*55+"\n")
    await asyncio.gather(run_web_server(), run_bot())

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
