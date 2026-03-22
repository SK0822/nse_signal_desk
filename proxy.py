"""
NSE Signal Dashboard — Flask Proxy for Angel One SmartAPI
----------------------------------------------------------
Run:  python proxy.py
Then open index.html in your browser.

Requirements:
  pip install flask flask-cors smartapi-python pyotp logzero yfinance pandas requests apscheduler
"""

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
import requests
import threading
import json
import requests
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
CORS(app)

# ─── CONFIG — read from environment variables ─────────────────────────────────
import os
API_KEY      = os.environ.get("API_KEY",      "YOUR_API_KEY")
CLIENT_CODE  = os.environ.get("CLIENT_CODE",  "YOUR_CLIENT_CODE")
MPIN         = os.environ.get("MPIN",         "YOUR_MPIN")
TOTP_SECRET  = os.environ.get("TOTP_SECRET",  "YOUR_TOTP_SECRET")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")
GCP_NEWS_URL = "http://34.42.82.253:5000/api/news"
# ──────────────────────────────────────────────────────────────────────────────

# NSE stocks to scan — symbol as used by yfinance (.NS suffix)
WATCHLIST = [
    {"symbol": "RELIANCE",    "yf": "RELIANCE.NS",    "token": "2885",  "sector": "Energy"},
    {"symbol": "HDFCBANK",    "yf": "HDFCBANK.NS",    "token": "1333",  "sector": "Banking"},
    {"symbol": "INFY",        "yf": "INFY.NS",         "token": "1594",  "sector": "IT"},
    {"symbol": "TATASTEEL",   "yf": "TATASTEEL.NS",    "token": "3499",  "sector": "Metals"},
    {"symbol": "SBIN",        "yf": "SBIN.NS",         "token": "3045",  "sector": "Banking PSU"},
    {"symbol": "ONGC",        "yf": "ONGC.NS",         "token": "2475",  "sector": "Energy PSU"},
    {"symbol": "WIPRO",       "yf": "WIPRO.NS",        "token": "3787",  "sector": "IT"},
    {"symbol": "BAJFINANCE",  "yf": "BAJFINANCE.NS",   "token": "317",   "sector": "NBFC"},
    {"symbol": "BHARTIARTL",  "yf": "BHARTIARTL.NS",   "token": "10604", "sector": "Telecom"},
    {"symbol": "MARUTI",      "yf": "MARUTI.NS",       "token": "10999", "sector": "Auto"},
    {"symbol": "TCS",         "yf": "TCS.NS",          "token": "11536", "sector": "IT"},
    {"symbol": "ICICIBANK",   "yf": "ICICIBANK.NS",    "token": "4963",  "sector": "Banking"},
]

# ─── LIVE PRICE CACHE (populated by WebSocket) ────────────────────────────────
# token → {"ltp": float, "ts": datetime}
_live_prices = {}
_ws_connected = False
_token_to_symbol = {s["token"]: s["symbol"] for s in WATCHLIST}

def get_live_ltp(token: str):
    """Return cached live LTP if fresh (< 10s old), else None."""
    entry = _live_prices.get(token)
    if entry and (datetime.now() - entry["ts"]).seconds < 60:
        return entry["ltp"]
    return None

# ─── SESSION ──────────────────────────────────────────────────────────────────
_session = {}

def get_angel_session():
    global _session
    if _session.get("obj") and _session.get("expires_at") and datetime.now() < _session["expires_at"]:
        return _session["obj"]
    obj = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    data = obj.generateSession(CLIENT_CODE, MPIN, totp)
    if not data.get("status"):
        raise Exception(f"Angel One login failed: {data.get('message')}")
    auth_token  = data["data"]["jwtToken"]
    feed_token  = obj.getfeedToken()
    _session = {
        "obj":        obj,
        "auth_token": auth_token,
        "feed_token": feed_token,
        "expires_at": datetime.now() + timedelta(hours=8)
    }
    return obj

def get_ltp_angel(obj, symbol, token):
    """Fallback REST LTP — used only when WebSocket cache is stale."""
    # First try live WebSocket cache
    cached = get_live_ltp(token)
    if cached:
        return cached
    # Fall back to REST
    try:
        resp = obj.ltpData("NSE", symbol + "-EQ", token)
        if resp.get("status") and resp["data"]:
            return float(resp["data"]["ltp"])
    except Exception:
        pass
    return None

# ─── WEBSOCKET MANAGER ────────────────────────────────────────────────────────

def _start_websocket():
    """Start SmartWebSocketV2 in a background thread. Auto-reconnects on disconnect."""
    global _ws_connected

    def _connect():
        global _ws_connected
        try:
            sess = get_angel_session()
            auth_token = _session["auth_token"]
            feed_token = _session["feed_token"]

            sws = SmartWebSocketV2(
                auth_token, API_KEY, CLIENT_CODE, feed_token,
                max_retry_attempt=5,
                retry_strategy=1,
                retry_delay=5,
                retry_multiplier=2,
                retry_duration=120
            )

            def on_open(wsapp):
                global _ws_connected
                _ws_connected = True
                print(f"[{datetime.now().strftime('%H:%M:%S')}] WebSocket connected — subscribing to {len(WATCHLIST)} tokens")
                token_list = [{
                    "exchangeType": 1,   # NSE
                    "tokens": [s["token"] for s in WATCHLIST]
                }]
                sws.subscribe("nse_live", 1, token_list)   # mode 1 = LTP only

            def on_data(wsapp, message):
                try:
                    if isinstance(message, str):
                        message = json.loads(message)
                    token = str(message.get("token", ""))
                    ltp   = message.get("last_traded_price", 0)
                    # Debug — print first message received to confirm format
                    if not _live_prices:
                        print(f"[WS DEBUG] First tick: {message}")
                    if token and ltp:
                        # Angel One sends LTP in paise — divide by 100
                        _live_prices[token] = {
                            "ltp": round(float(ltp) / 100, 2),
                            "ts":  datetime.now()
                        }
                except Exception as e:
                    pass

            def on_error(wsapp, error):
                global _ws_connected
                _ws_connected = False
                print(f"WebSocket error: {error}")

            def on_close(wsapp, close_status_code=None, close_msg=None):
                global _ws_connected
                _ws_connected = False
                print(f"WebSocket closed — will reconnect")

            sws.on_open  = on_open
            sws.on_data  = on_data
            sws.on_error = on_error
            sws.on_close = on_close
            sws.connect()

        except Exception as e:
            _ws_connected = False
            print(f"WebSocket start failed: {e}")
            traceback.print_exc()

    t = threading.Thread(target=_connect, daemon=True)
    t.start()
    return t

def compute_signals(df):
    """
    Smarter signal engine — scores 7 indicators:
      RSI, MA trend, MACD crossover, Bollinger Bands,
      Volume spike, ATR volatility, Candle pattern
    Swing needs score >= 4 for BUY, <= -3 for SELL.
    Intraday uses VWAP + MACD + BB squeeze.
    """
    close  = df["Close"]
    high   = df["High"]
    low    = df["Low"]
    volume = df["Volume"]

    # ── Base indicators ───────────────────────────────────────────────────────

    # MA
    ma20 = close.rolling(20).mean()
    ma50 = close.rolling(50).mean()

    # RSI (14)
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    ema12     = close.ewm(span=12, adjust=False).mean()
    ema26     = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - signal_line          # positive = bullish momentum

    # Bollinger Bands (20, 2σ)
    bb_mid  = close.rolling(20).mean()
    bb_std  = close.rolling(20).std()
    bb_up   = bb_mid + 2 * bb_std
    bb_low  = bb_mid - 2 * bb_std
    bb_pct  = (close - bb_low) / (bb_up - bb_low)   # 0=bottom band, 1=top band

    # ATR (14)
    tr  = pd.concat([high - low,
                     (high - close.shift()).abs(),
                     (low  - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    # Volume
    avg_vol   = volume.rolling(20).mean()
    vol_ratio = float((volume.iloc[-1] / avg_vol.iloc[-1] - 1) * 100) if float(avg_vol.iloc[-1]) > 0 else 0

    # Candle pattern
    if len(df) >= 2:
        p_o = float(df["Open"].iloc[-2]); p_c = float(df["Close"].iloc[-2])
        c_o = float(df["Open"].iloc[-1]); c_c = float(df["Close"].iloc[-1])
        bullish_engulf = c_c > p_o and c_o < p_c and c_c > c_o
        bearish_engulf = c_c < p_o and c_o > p_c and c_c < c_o
        bearish_wick   = float(high.iloc[-1] - c_c) > 1.5 * abs(c_c - c_o)
        bullish_hammer = (float(c_c) > float(c_o)) and ((float(c_o) - float(low.iloc[-1])) > 2 * abs(c_c - c_o))
    else:
        bullish_engulf = bearish_engulf = bearish_wick = bullish_hammer = False

    # Safe scalar reads
    def s(series): return float(series.iloc[-1]) if not pd.isna(series.iloc[-1]) else 0.0
    def s2(series): return float(series.iloc[-2]) if len(series) >= 2 and not pd.isna(series.iloc[-2]) else 0.0

    last_close   = s(close)
    last_rsi     = s(rsi) if s(rsi) != 0 else 50
    last_ma20    = s(ma20) if s(ma20) != 0 else last_close
    last_ma50    = s(ma50) if s(ma50) != 0 else last_close
    last_macd    = s(macd_line)
    last_signal  = s(signal_line)
    last_hist    = s(macd_hist)
    prev_hist    = s2(macd_hist)
    last_bb_pct  = s(bb_pct)
    last_bb_up   = s(bb_up)
    last_bb_low  = s(bb_low)
    last_atr     = s(atr) if s(atr) != 0 else last_close * 0.02

    # MACD crossover direction
    macd_cross_up   = prev_hist < 0 and last_hist > 0   # histogram crossed zero upward
    macd_cross_down = prev_hist > 0 and last_hist < 0   # histogram crossed zero downward
    macd_bullish    = last_macd > last_signal
    macd_bearish    = last_macd < last_signal

    # BB position
    bb_oversold    = last_bb_pct < 0.2   # near lower band
    bb_overbought  = last_bb_pct > 0.8   # near upper band
    bb_squeeze     = float(bb_std.iloc[-1]) < float(bb_std.rolling(20).mean().iloc[-1]) * 0.75

    # ── Swing scoring ─────────────────────────────────────────────────────────
    score = 0
    indicators = []

    # 1. RSI (weight 2)
    if last_rsi < 35:
        score += 2; indicators.append(f"RSI {last_rsi:.0f} — oversold")
    elif last_rsi < 50:
        score += 1; indicators.append(f"RSI {last_rsi:.0f}")
    elif last_rsi > 70:
        score -= 2; indicators.append(f"RSI {last_rsi:.0f} — overbought")
    elif last_rsi > 60:
        score -= 1; indicators.append(f"RSI {last_rsi:.0f}")
    else:
        indicators.append(f"RSI {last_rsi:.0f}")

    # 2. MA trend (weight 2)
    if last_close > last_ma20 > last_ma50:
        score += 2; indicators.append("MA20 > MA50 ✓")
    elif last_close > last_ma50:
        score += 1; indicators.append("Above MA50")
    elif last_close < last_ma20 < last_ma50:
        score -= 2; indicators.append("MA20 < MA50 ✗")
    elif last_close < last_ma50:
        score -= 1; indicators.append("Below MA50")

    # 3. MACD (weight 2)
    if macd_cross_up:
        score += 2; indicators.append("MACD cross ↑")
    elif macd_bullish and last_hist > 0:
        score += 1; indicators.append("MACD bullish")
    elif macd_cross_down:
        score -= 2; indicators.append("MACD cross ↓")
    elif macd_bearish and last_hist < 0:
        score -= 1; indicators.append("MACD bearish")
    else:
        indicators.append(f"MACD {last_hist:+.1f}")

    # 4. Bollinger Bands (weight 2)
    if bb_oversold:
        score += 2; indicators.append(f"BB lower band ({last_bb_pct:.0%})")
    elif bb_overbought:
        score -= 2; indicators.append(f"BB upper band ({last_bb_pct:.0%})")
    elif bb_squeeze:
        indicators.append("BB squeeze")
    else:
        indicators.append(f"BB mid ({last_bb_pct:.0%})")

    # 5. Volume (weight 1)
    if vol_ratio > 20:
        score += 1; indicators.append(f"Vol +{vol_ratio:.0f}%")
    elif vol_ratio < -20:
        score -= 1; indicators.append(f"Vol {vol_ratio:.0f}%")

    # 6. Candle pattern (weight 1)
    if bullish_engulf or bullish_hammer:
        score += 1
        indicators.append("Bullish candle" if bullish_engulf else "Hammer")
    elif bearish_engulf or bearish_wick:
        score -= 1
        indicators.append("Bearish candle" if bearish_engulf else "Bearish wick")

    # Signal threshold — require stronger consensus
    if score >= 4:
        swing_signal = "buy"
    elif score <= -3:
        swing_signal = "sell"
    else:
        swing_signal = "hold"

    # Confidence label
    if abs(score) >= 6:
        confidence = "high"
    elif abs(score) >= 4:
        confidence = "medium"
    else:
        confidence = "low"

    # Entry / target / SL (ATR-based)
    if swing_signal == "buy":
        entry  = round(last_close, 2)
        target = round(last_close + 2.5 * last_atr, 2)
        sl     = round(last_close - 1.0 * last_atr, 2)
    elif swing_signal == "sell":
        entry  = round(last_close, 2)
        target = round(last_close - 2.5 * last_atr, 2)
        sl     = round(last_close + 1.0 * last_atr, 2)
    else:
        entry = target = sl = None

    swing_reason = _swing_reason(swing_signal, last_rsi, last_close, last_ma20, last_ma50,
                                  vol_ratio, bullish_engulf, bullish_hammer,
                                  bearish_engulf, bearish_wick, macd_cross_up,
                                  macd_cross_down, macd_bullish, bb_oversold,
                                  bb_overbought, bb_squeeze, score)

    # ── Intraday signal ───────────────────────────────────────────────────────
    # VWAP (5-day approx)
    vwap_denom = float(volume.rolling(5).sum().iloc[-1])
    vwap       = float((close * volume).rolling(5).sum().iloc[-1] / vwap_denom) if vwap_denom > 0 else last_close
    above_vwap = last_close > vwap

    intra_score = 0
    if above_vwap:             intra_score += 1
    if macd_bullish:           intra_score += 1
    if last_rsi < 60:          intra_score += 1
    if not bb_overbought:      intra_score += 1
    if vol_ratio > 5:          intra_score += 1

    if intra_score >= 4:
        intra_signal = "buy"
    elif intra_score <= 1:
        intra_signal = "sell"
    else:
        intra_signal = "hold"

    intra_indicators = [
        f"VWAP {'above ✓' if above_vwap else 'below ✗'}",
        f"MACD {'↑' if macd_bullish else '↓'}",
        f"RSI {last_rsi:.0f}"
    ]

    intra_entry  = round(last_close, 2) if intra_signal != "hold" else None
    intra_target = round(last_close + last_atr * 0.8, 2) if intra_signal == "buy" else (round(last_close - last_atr * 0.8, 2) if intra_signal == "sell" else None)
    intra_sl     = round(last_close - last_atr * 0.4, 2) if intra_signal == "buy" else (round(last_close + last_atr * 0.4, 2) if intra_signal == "sell" else None)
    intra_reason = _intra_reason(intra_signal, above_vwap, last_rsi, vol_ratio, macd_bullish, bb_overbought, bb_oversold)

    return {
        "swing": {
            "signal": swing_signal,
            "score": score,
            "confidence": confidence,
            "indicators": indicators[:4],
            "entry": entry, "target": target, "sl": sl,
            "reason": swing_reason,
            "bb_upper": round(last_bb_up, 2),
            "bb_lower": round(last_bb_low, 2),
        },
        "intraday": {
            "signal": intra_signal,
            "score": intra_score,
            "confidence": "high" if abs(intra_score - 2.5) > 1.5 else "medium",
            "indicators": intra_indicators,
            "entry": intra_entry, "target": intra_target, "sl": intra_sl,
            "reason": intra_reason
        }
    }

def _swing_reason(signal, rsi, close, ma20, ma50, vol_ratio,
                  bull_eng, bull_ham, bear_eng, bear_wick,
                  macd_up, macd_down, macd_bull,
                  bb_os, bb_ob, bb_sq, score):
    if signal == "buy":
        parts = []
        if macd_up:   parts.append("MACD just crossed above signal line — fresh bullish momentum")
        elif macd_bull: parts.append("MACD is above signal line — trend is bullish")
        if bb_os:     parts.append("price near lower Bollinger Band — statistically oversold")
        if rsi < 40:  parts.append(f"RSI at {rsi:.0f} confirms oversold condition")
        if close > ma20 > ma50: parts.append("price above both MA20 and MA50 — uptrend intact")
        if bull_eng:  parts.append("bullish engulfing candle adds reversal confirmation")
        if bull_ham:  parts.append("hammer candle signals rejection of lower prices")
        if vol_ratio > 20: parts.append(f"volume {vol_ratio:.0f}% above average — institutional interest")
        if bb_sq:     parts.append("Bollinger Band squeeze suggests a breakout is brewing")
        return ". ".join(parts).capitalize() + f". (Score: {score}/10)" if parts else f"Multiple indicators align bullishly. (Score: {score}/10)"
    elif signal == "sell":
        parts = []
        if macd_down: parts.append("MACD crossed below signal line — momentum turning bearish")
        elif not macd_bull: parts.append("MACD below signal line — bearish pressure building")
        if bb_ob:     parts.append("price near upper Bollinger Band — statistically overbought")
        if rsi > 70:  parts.append(f"RSI at {rsi:.0f} confirms overbought condition")
        if close < ma20 < ma50: parts.append("price below both MAs — downtrend intact")
        if bear_eng:  parts.append("bearish engulfing candle confirms selling pressure")
        if bear_wick: parts.append("long upper wick shows rejection at highs")
        return ". ".join(parts).capitalize() + f". (Score: {score}/10)" if parts else f"Bearish confluence — avoid long entry. (Score: {score}/10)"
    else:
        if bb_sq: return f"Bollinger Bands are squeezing — a big move is likely soon but direction unclear. Wait for breakout confirmation. (Score: {score}/10)"
        return f"Indicators are mixed — no strong directional edge. Wait for MACD crossover or RSI to reach extremes before entering. (Score: {score}/10)"

def _intra_reason(signal, above_vwap, rsi, vol_ratio, macd_bull, bb_ob, bb_os):
    if signal == "buy":
        parts = []
        if above_vwap: parts.append("trading above VWAP — institutional buy side dominant")
        if macd_bull:  parts.append("MACD bullish on daily")
        if bb_os:      parts.append("near lower BB — bounce potential")
        if vol_ratio > 10: parts.append(f"volume {vol_ratio:.0f}% above average")
        return ". ".join(parts).capitalize() + ". Trail stop loss as price moves up." if parts else "Intraday conditions favour long entry. Use tight stop."
    elif signal == "sell":
        parts = []
        if not above_vwap: parts.append("below VWAP — sellers in control")
        if not macd_bull:  parts.append("MACD bearish")
        if bb_ob:          parts.append("near upper BB — fade the move")
        return ". ".join(parts).capitalize() + ". Keep stop tight above recent high." if parts else "Intraday conditions favour short entry."
    else:
        return "Mixed intraday signals — VWAP, MACD and BB not aligned. High risk of stop hunt. Better to wait for a cleaner setup."

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────

# Track already-alerted signals to avoid duplicate messages
# Key: "SYMBOL-swing/intraday" → last alerted signal string
_alerted = {}

def send_telegram(message: str):
    """Send a message to Telegram. Silently fails if not configured."""
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN" or TELEGRAM_CHAT_ID == "YOUR_CHAT_ID":
        print("⚠️  Telegram not configured — set TELEGRAM_TOKEN and TELEGRAM_CHAT_ID env vars on Render.")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.status_code == 200:
            print(f"✅ Telegram alert sent successfully.")
        else:
            print(f"❌ Telegram error: {resp.status_code} — {resp.text}")
    except Exception as e:
        print(f"Telegram send failed: {e}")

def format_alert(symbol, sector, price, change, mode, signal_data):
    """Format a clean Telegram alert message."""
    sig = signal_data["signal"].upper()
    score = signal_data.get("score", "?")
    conf  = signal_data.get("confidence", "").upper()
    entry  = signal_data.get("entry")
    target = signal_data.get("target")
    sl     = signal_data.get("sl")
    reason = signal_data.get("reason", "")[:200]  # truncate long reasons

    emoji = "🟢" if sig == "BUY" else "🔴" if sig == "SELL" else "🟡"
    mode_label = "SWING" if mode == "swing" else "INTRADAY"
    chg_sign = "+" if change >= 0 else ""

    lines = [
        f"{emoji} <b>{sig} — {symbol}</b> [{mode_label}]",
        f"📊 {sector} | ₹{price:,.2f} ({chg_sign}{change}%)",
        f"🎯 Confidence: <b>{conf}</b> (Score: {score:+d})",
    ]
    if entry and target and sl:
        rr = abs((target - entry) / (entry - sl))
        lines.append(f"📌 Entry: ₹{entry:,.2f} | Target: ₹{target:,.2f} | SL: ₹{sl:,.2f} | R:R {rr:.1f}x")
    lines.append(f"💡 {reason}")
    lines.append(f"🕐 {datetime.now().strftime('%d %b %Y %H:%M')}")
    return "\n".join(lines)

def _is_market_hours():
    """Returns True if current IST time is within NSE market hours."""
    now = datetime.now()
    h, m = now.hour, now.minute
    return (h > 9 or (h == 9 and m >= 15)) and (h < 15 or (h == 15 and m <= 30))

def scan_and_alert():
    """Background job — scans all stocks and sends Telegram alerts for HIGH confidence signals."""
    if not _is_market_hours():
        return

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running alert scan...")
    try:
        obj = get_angel_session()
    except Exception as e:
        print(f"Alert scan: Angel One login failed — {e}")
        return

    for stock in WATCHLIST:
        try:
            ltp = get_ltp_angel(obj, stock["symbol"], stock["token"])
            ticker = yf.Ticker(stock["yf"])
            df = ticker.history(period="90d", interval="1d", auto_adjust=True)
            if df.empty or len(df) < 26:
                continue
            df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]

            if ltp:
                prev_close = float(df["Close"].iloc[-1])
                change_pct = round((ltp - prev_close) / prev_close * 100, 2)
            else:
                ltp = float(df["Close"].iloc[-1])
                prev_close = float(df["Close"].iloc[-2]) if len(df) > 1 else ltp
                change_pct = round((ltp - prev_close) / prev_close * 100, 2)

            signals = compute_signals(df)

            for mode in ["swing", "intraday"]:
                sd = signals[mode]
                sig = sd["signal"]
                conf = sd.get("confidence", "low")

                # Only alert HIGH confidence BUY or SELL
                if sig not in ("buy", "sell") or conf != "high":
                    continue

                alert_key = f"{stock['symbol']}-{mode}"
                new_val = f"{sig}-{conf}"

                # Don't re-alert same signal unless it changed
                if _alerted.get(alert_key) == new_val:
                    continue

                _alerted[alert_key] = new_val
                msg = format_alert(
                    stock["symbol"], stock["sector"],
                    round(ltp, 2), change_pct,
                    mode, sd
                )
                send_telegram(msg)
                print(f"  → Alert sent: {stock['symbol']} {mode} {sig.upper()} (HIGH)")

        except Exception as e:
            print(f"Alert scan error for {stock['symbol']}: {e}")
            continue

# ─── INTRADAY CANDLES (Angel One getCandleData) ───────────────────────────────

def get_intraday_candles(obj, token, interval="FIVE_MINUTE", days=2):
    """
    Fetch intraday candles from Angel One getCandleData.
    Only runs during market hours — Angel One historical API
    is unreliable outside trading hours.
    """
    # Skip outside market hours — API times out when market is closed
    if not _is_market_hours():
        return None

    try:
        now      = datetime.now()
        fromdate = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
        todate   = now.strftime("%Y-%m-%d %H:%M")

        params = {
            "exchange":    "NSE",
            "symboltoken": token,
            "interval":    interval,
            "fromdate":    fromdate,
            "todate":      todate
        }
        resp = obj.getCandleData(params)
        if not resp.get("status") or not resp.get("data"):
            return None

        df = pd.DataFrame(resp["data"],
                          columns=["DateTime", "Open", "High", "Low", "Close", "Volume"])
        df["DateTime"] = pd.to_datetime(df["DateTime"])
        df.set_index("DateTime", inplace=True)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(inplace=True)
        return df if len(df) >= 15 else None

    except Exception as e:
        # Suppress timeout noise — will retry on next refresh
        if "timeout" in str(e).lower() or "Max retries" in str(e):
            return None
        print(f"getCandleData error (token {token}): {e}")
        return None


def compute_intraday_signals_5m(df5, daily_df, ltp):
    """
    Intraday signal engine using 5-min candles from Angel One.
    Indicators: VWAP, RSI(14), MACD, EMA9, EMA21, Bollinger Bands, Volume
    """
    close  = df5["Close"]
    high   = df5["High"]
    low    = df5["Low"]
    volume = df5["Volume"]

    # ── Indicators on 5-min candles ──────────────────────────────────────────

    # VWAP (cumulative for the day)
    typical = (high + low + close) / 3
    vwap    = (typical * volume).cumsum() / volume.cumsum()

    # RSI (14)
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))

    # EMA 9 and 21
    ema9  = close.ewm(span=9,  adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    # MACD (12, 26, 9) on 5-min
    ema12      = close.ewm(span=12, adjust=False).mean()
    ema26      = close.ewm(span=26, adjust=False).mean()
    macd_line  = ema12 - ema26
    sig_line   = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist  = macd_line - sig_line

    # Bollinger Bands (20, 2σ)
    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_up  = bb_mid + 2 * bb_std
    bb_low = bb_mid - 2 * bb_std
    bb_pct = (close - bb_low) / (bb_up - bb_low)

    # ATR (14) on 5-min
    tr  = pd.concat([high - low,
                     (high - close.shift()).abs(),
                     (low  - close.shift()).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()

    # Volume spike
    avg_vol   = volume.rolling(20).mean()
    vol_ratio = float((volume.iloc[-1] / avg_vol.iloc[-1] - 1) * 100) if float(avg_vol.iloc[-1]) > 0 else 0

    # Safe scalar reads
    def s(series): return float(series.iloc[-1]) if not pd.isna(series.iloc[-1]) else 0.0
    def s2(series): return float(series.iloc[-2]) if len(series) >= 2 and not pd.isna(series.iloc[-2]) else 0.0

    last_close  = ltp if ltp else s(close)
    last_vwap   = s(vwap)
    last_rsi    = s(rsi) if s(rsi) != 0 else 50
    last_ema9   = s(ema9)
    last_ema21  = s(ema21)
    last_hist   = s(macd_hist)
    prev_hist   = s2(macd_hist)
    last_macd   = s(macd_line)
    last_sig    = s(sig_line)
    last_bb_pct = s(bb_pct)
    last_atr    = s(atr) if s(atr) != 0 else last_close * 0.005

    above_vwap     = last_close > last_vwap
    ema_bullish    = last_ema9  > last_ema21
    macd_bullish   = last_macd  > last_sig
    macd_cross_up  = prev_hist  < 0 and last_hist > 0
    macd_cross_dn  = prev_hist  > 0 and last_hist < 0
    bb_oversold    = last_bb_pct < 0.2
    bb_overbought  = last_bb_pct > 0.8

    # ── Scoring ───────────────────────────────────────────────────────────────
    score = 0
    indicators = []

    # VWAP (weight 2)
    if above_vwap:
        score += 2; indicators.append("Above VWAP ✓")
    else:
        score -= 2; indicators.append("Below VWAP ✗")

    # EMA 9/21 crossover (weight 2)
    if ema_bullish:
        score += 2; indicators.append("EMA9 > EMA21 ✓")
    else:
        score -= 2; indicators.append("EMA9 < EMA21 ✗")

    # MACD (weight 2)
    if macd_cross_up:
        score += 2; indicators.append("MACD cross ↑")
    elif macd_bullish:
        score += 1; indicators.append("MACD bullish")
    elif macd_cross_dn:
        score -= 2; indicators.append("MACD cross ↓")
    elif not macd_bullish:
        score -= 1; indicators.append("MACD bearish")

    # RSI (weight 1)
    if last_rsi < 40:
        score += 1; indicators.append(f"RSI {last_rsi:.0f} oversold")
    elif last_rsi > 65:
        score -= 1; indicators.append(f"RSI {last_rsi:.0f} overbought")
    else:
        indicators.append(f"RSI {last_rsi:.0f}")

    # Bollinger Bands (weight 1)
    if bb_oversold:
        score += 1; indicators.append(f"BB low ({last_bb_pct:.0%})")
    elif bb_overbought:
        score -= 1; indicators.append(f"BB high ({last_bb_pct:.0%})")

    # Volume (weight 1)
    if vol_ratio > 20:
        score += 1; indicators.append(f"Vol +{vol_ratio:.0f}%")
    elif vol_ratio < -20:
        score -= 1; indicators.append(f"Vol {vol_ratio:.0f}%")

    # Signal — needs strong consensus for intraday
    if score >= 5:
        signal = "buy"
    elif score <= -4:
        signal = "sell"
    else:
        signal = "hold"

    confidence = "high" if abs(score) >= 6 else "medium" if abs(score) >= 4 else "low"

    # Entry / Target / SL using 5-min ATR
    if signal == "buy":
        entry  = round(last_close, 2)
        target = round(last_close + 2.0 * last_atr, 2)
        sl     = round(last_close - 1.0 * last_atr, 2)
    elif signal == "sell":
        entry  = round(last_close, 2)
        target = round(last_close - 2.0 * last_atr, 2)
        sl     = round(last_close + 1.0 * last_atr, 2)
    else:
        entry = target = sl = None

    # Reason
    parts = []
    if signal == "buy":
        if above_vwap:     parts.append("price above VWAP — buyers in control")
        if ema_bullish:    parts.append("EMA9 above EMA21 on 5-min — short-term trend up")
        if macd_cross_up:  parts.append("MACD just crossed up — fresh momentum")
        elif macd_bullish: parts.append("MACD bullish on 5-min")
        if bb_oversold:    parts.append("near lower BB — bounce likely")
        if vol_ratio > 20: parts.append(f"volume {vol_ratio:.0f}% above average")
        reason = ". ".join(parts).capitalize() + f". Trail SL as price moves. (Score: {score})" if parts else f"Intraday bullish setup. (Score: {score})"
    elif signal == "sell":
        if not above_vwap:  parts.append("price below VWAP — sellers dominant")
        if not ema_bullish: parts.append("EMA9 below EMA21 — short-term trend down")
        if macd_cross_dn:   parts.append("MACD just crossed down — momentum weakening")
        if bb_overbought:   parts.append("near upper BB — pullback likely")
        reason = ". ".join(parts).capitalize() + f". Keep stop tight. (Score: {score})" if parts else f"Intraday bearish setup. (Score: {score})"
    else:
        reason = f"5-min indicators not aligned — VWAP and EMAs giving mixed signals. Wait for clearer setup. (Score: {score})"

    candle_time = df5.index[-1].strftime("%H:%M") if len(df5) > 0 else "—"

    return {
        "signal":     signal,
        "score":      score,
        "confidence": confidence,
        "indicators": indicators[:4],
        "entry": entry, "target": target, "sl": sl,
        "reason":     reason,
        "candle_time": candle_time,   # last 5-min candle time
        "vwap":       round(last_vwap, 2),
        "rsi":        round(last_rsi, 1)
    }

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/signals")
def get_signals():
    try:
        obj = get_angel_session()
    except Exception as e:
        return jsonify({"error": f"Angel One login failed: {e}"}), 500

    results = []
    for stock in WATCHLIST:
        try:
            # Live LTP (WebSocket cache first, REST fallback)
            ltp = get_ltp_angel(obj, stock["symbol"], stock["token"])

            # Daily OHLCV from yfinance for swing signals
            ticker = yf.Ticker(stock["yf"])
            df_daily = ticker.history(period="90d", interval="1d", auto_adjust=True)
            if df_daily.empty or len(df_daily) < 20:
                continue
            df_daily.columns = [c if isinstance(c, str) else c[0] for c in df_daily.columns]

            if ltp:
                prev_close = float(df_daily["Close"].iloc[-1])
                change_pct = round((ltp - prev_close) / prev_close * 100, 2)
            else:
                ltp = float(df_daily["Close"].iloc[-1])
                prev_close = float(df_daily["Close"].iloc[-2]) if len(df_daily) > 1 else ltp
                change_pct = round((ltp - prev_close) / prev_close * 100, 2)

            # Swing signals from daily candles
            swing_signals = compute_signals(df_daily)["swing"]

            # ── Intraday signals from 5-min Angel One candles ──────────────
            df_5m = get_intraday_candles(obj, stock["token"], interval="FIVE_MINUTE", days=2)
            if df_5m is not None and len(df_5m) >= 15:
                intraday_signals = compute_intraday_signals_5m(df_5m, df_daily, ltp)
                intraday_source  = "5min"
            else:
                # Fallback to daily-based intraday if 5-min unavailable
                intraday_signals = compute_signals(df_daily)["intraday"]
                intraday_source  = "daily"

            # Last 30 days chart data
            chart_df     = df_daily.tail(30)
            chart_dates  = [d.strftime("%d %b") for d in chart_df.index]
            chart_close  = [round(float(v), 2) for v in chart_df["Close"].tolist()]
            chart_volume = [int(v) for v in chart_df["Volume"].tolist()]

            results.append({
                "symbol":   stock["symbol"],
                "sector":   stock["sector"],
                "price":    round(ltp, 2),
                "change":   change_pct,
                "swing":    swing_signals,
                "intraday": intraday_signals,
                "intraday_source": intraday_source,
                "updated":  datetime.now().strftime("%H:%M:%S"),
                "chart": {
                    "dates":  chart_dates,
                    "close":  chart_close,
                    "volume": chart_volume
                }
            })

        except Exception as e:
            print(f"Error processing {stock['symbol']}: {e}")
            traceback.print_exc()
            continue

    return jsonify({"status": "ok", "data": results, "count": len(results)})

@app.route("/api/indices")
def get_indices():
    INDICES = [
        {"name": "Nifty 50",     "yf": "^NSEI"},
        {"name": "Bank Nifty",   "yf": "^NSEBANK"},
        {"name": "Nifty IT",     "yf": "^CNXIT"},
        {"name": "Nifty Midcap", "yf": "^NSEMDCP50"},
    ]

    results = []
    for idx in INDICES:
        try:
            ticker = yf.Ticker(idx["yf"])
            df = ticker.history(period="10d", interval="1d", auto_adjust=True)
            if df.empty or len(df) < 2:
                continue

            # Ensure plain column names
            df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]

            last  = float(df["Close"].iloc[-1])
            prev  = float(df["Close"].iloc[-2])
            chg   = round((last - prev) / prev * 100, 2)
            high  = float(df["High"].iloc[-1])
            low   = float(df["Low"].iloc[-1])

            # 10-day sparkline
            spark = [round(float(v), 2) for v in df["Close"].tolist()]

            # Market mood
            if chg > 0.5:
                mood = "bullish"
            elif chg < -0.5:
                mood = "bearish"
            else:
                mood = "flat"

            results.append({
                "name":   idx["name"],
                "value":  round(last, 2),
                "change": chg,
                "high":   round(high, 2),
                "low":    round(low, 2),
                "mood":   mood,
                "spark":  spark
            })
        except Exception as e:
            print(f"Error fetching index {idx['name']}: {e}")
            traceback.print_exc()
            continue

    return jsonify({"status": "ok", "data": results})

@app.route("/api/test-alert")
def test_alert():
    """Send a test Telegram message to verify setup."""
    msg = (
        "✅ <b>NSE Signal Desk — Test Alert</b>\n"
        "Your Telegram alerts are working correctly!\n"
        f"🕐 {datetime.now().strftime('%d %b %Y %H:%M')}"
    )
    send_telegram(msg)
    return jsonify({"status": "ok", "message": "Test alert sent to Telegram"})

@app.route("/api/health")
def health():
    live_count = sum(1 for v in _live_prices.values() if (datetime.now() - v["ts"]).seconds < 60)
    return jsonify({
        "status": "ok",
        "time": datetime.now().isoformat(),
        "websocket": "connected" if _ws_connected else "disconnected",
        "live_prices": live_count
    })

@app.route('/api/news')
def proxy_news():
    try:
        r = requests.get(GCP_NEWS_URL, timeout=10)
        return jsonify(r.json())
    except Exception as e:
        return jsonify([])

if __name__ == "__main__":
    # Login first to get auth + feed tokens
    print("Logging in to Angel One...")
    try:
        get_angel_session()
        print(f"Login OK. Starting WebSocket for {len(WATCHLIST)} stocks...")
        _start_websocket()
    except Exception as e:
        print(f"Login failed: {e} — WebSocket will not start. Falling back to REST.")

    # Background alert scanner — every 3 minutes
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    scheduler.add_job(scan_and_alert, "interval", minutes=3, id="alert_scan")
    scheduler.start()

    port = int(os.environ.get("PORT", 5000))
    print("=" * 55)
    print(f"  NSE Signal Dashboard — Flask Proxy")
    print(f"  Running on port {port}")
    print(f"  WebSocket: live tick prices from Angel One")
    print(f"  Telegram alerts: every 3 min during market hours")
    print("=" * 55)
    try:
        app.run(host="0.0.0.0", debug=False, port=port)
    finally:
        scheduler.shutdown()
