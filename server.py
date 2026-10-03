# Gevent must patch the standard library BEFORE requests/urllib3 are imported,
# otherwise network calls can hang silently under gunicorn's gevent worker.
try:
    from gevent import monkey

    monkey.patch_all()
except ImportError:  # running without gevent (plain `python server.py`)
    pass

import csv
import json
import math
import os
import re
import statistics
import threading
import time
from collections import deque
from datetime import datetime

import requests
from flask import Flask, Response, jsonify, send_from_directory, stream_with_context

# --- Network hardening ---------------------------------------------------------
# 1) IPv4 only. Many hosts (including some containers) cannot route IPv6, and urllib3
#    otherwise waits out a full connect timeout on every IPv6 address before trying IPv4.
import socket
import urllib3.util.connection as _urllib3_connection

_urllib3_connection.allowed_gai_family = lambda: socket.AF_INET

# 2) Cache DNS answers for 5 minutes (and reuse a stale answer if a lookup fails), so
#    opening a connection does not depend on a slow or flaky resolver every time.
_orig_getaddrinfo = socket.getaddrinfo
_dns_cache = {}


def _cached_getaddrinfo(host, port, *args, **kwargs):
    key = (host, port, args, tuple(sorted(kwargs.items())))
    now = time.time()
    hit = _dns_cache.get(key)
    if hit and now - hit[0] < 300:
        return hit[1]
    try:
        result = _orig_getaddrinfo(host, port, *args, **kwargs)
    except Exception:
        if hit:
            return hit[1]
        raise
    _dns_cache[key] = (now, result)
    return result


socket.getaddrinfo = _cached_getaddrinfo

app = Flask(__name__, static_folder="static")
STARTED = time.time()

# Public Kalshi market-data API. No trading credentials are used.
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
SERIES = "KXBTC15M"

# --- Tunables (override with environment variables) ---------------------------
FEE_RATE = float(os.getenv("FEE_RATE", "0.07"))          # est. taker fee: rate * p * (1-p) per contract
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.07"))          # edge AFTER est. fees
MIN_PROB = float(os.getenv("MIN_PROB", "0.66"))
MIN_ASK_SIZE = float(os.getenv("MIN_ASK_SIZE", "5"))     # contracts available at the ask
LOG_DIR = os.path.abspath(os.getenv("LOG_DIR", "logs"))
LOG_EVERY = 5.0                                          # seconds between logged rows

# Trend from the last N hours of settled Kalshi results (yes = UP, no = DOWN).
TREND_HOURS = float(os.getenv("TREND_HOURS", "4"))
TREND_THRESHOLD = float(os.getenv("TREND_THRESHOLD", "0.65"))  # share of UP (or DOWN) results = a trend
TREND_MIN = int(os.getenv("TREND_MIN", "8"))                    # need at least this many results
TREND_WEIGHT = float(os.getenv("TREND_WEIGHT", "0.04"))        # how much the settled-results trend tilts the probability
TREND_MODE = os.getenv("TREND_MODE", "off").lower()             # "filter" also blocks the live BUY signal against the trend

# Chart indicators (EMA, MACD, RSI, Bollinger, VWAP on 5-minute bars) shift the expected final price.
TECH_Z = float(os.getenv("TECH_Z", "0.25"))  # max shift, in standard deviations, when the indicators fully agree

# Phone alerts: one check per market when ALERT_AT_SECONDS remain.
ALERT_AT = float(os.getenv("ALERT_AT_SECONDS", "300"))
ALERT_WINDOW = float(os.getenv("ALERT_WINDOW_SECONDS", "30"))  # alert fires in [AT-WINDOW, AT]
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "").strip()
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ALERT_ON_START = os.getenv("ALERT_ON_START", "1") == "1"

# Volatility is per-sqrt-second. BTC at ~50% annualised is about 9e-5.
SIGMA_FLOOR = 3.5e-5
SIGMA_ELEVATED = 9e-5

samples = deque(maxlen=2400)  # (ts, reference price), ~20 min
samples_lock = threading.Lock()

state = {"ok": False, "action": "WAIT", "error": "Starting..."}
state_lock = threading.Lock()
state_changed = threading.Condition(state_lock)

# Background pollers write here; compute() only reads. Slow upstreams never
# block the 0.5s signal loop.
feeds_lock = threading.Lock()
feeds = {
    "market": (0.0, None),
    "book": (0.0, None),
    "sentiment": (0.0, (0.0, [])),
    "history": (0.0, None),
    "candles": (0.0, None),
}
feed_errors = {}

POS = set(
    "surge rally rises rising bullish breakout gain gains strong stronger up upside "
    "approval adoption inflow inflows demand buy buying positive beat beats growth "
    "record high higher".split()
)
NEG = set(
    "crash falls falling bearish breakdown loss losses weak weaker down downside "
    "outflow outflows sell selling negative fear hack hacked fraud ban bans lawsuit "
    "risk risks liquidation liquidations".split()
)


# --- Helpers ------------------------------------------------------------------
# One shared session: reuses connections instead of redoing DNS/TLS on every call,
# which matters a lot on small, slow instances.
_session = requests.Session()
_session.mount("https://", requests.adapters.HTTPAdapter(pool_connections=16, pool_maxsize=16))
_session.headers.update({"User-Agent": "BTC15-Signal-Bot/1.2"})


def get(url, params=None):
    r = _session.get(url, params=params, timeout=(4.0, 8.0))
    r.raise_for_status()
    return r.json()


def iso(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def put(name, value):
    with feeds_lock:
        feeds[name] = (time.time(), value)


def read(name):
    with feeds_lock:
        return feeds.get(name, (0.0, None))


def fee(price):
    return FEE_RATE * price * (1.0 - price)


def valid_price(p):
    return p is not None and 0.0 < p < 1.0


def dollars(market_data, key):
    value = market_data.get(key + "_dollars")
    if value is None:
        value = market_data.get(key)
    try:
        value = float(value)
        if value > 1.0:  # older cent representation
            value /= 100.0
        return max(0.0, min(1.0, value))
    except Exception:
        return 0.0


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# --- Upstream fetchers --------------------------------------------------------
def fx_coinbase():
    return float(get(COINBASE)["price"])


def fx_kraken():
    result = get("https://api.kraken.com/0/public/Ticker", {"pair": "XBTUSD"})["result"]
    return float(next(iter(result.values()))["c"][0])


def fx_bitstamp():
    return float(get("https://www.bitstamp.net/api/v2/ticker/btcusd/")["last"])


def fx_gemini():
    return float(get("https://api.gemini.com/v1/pubticker/btcusd")["last"])


EXCHANGES = {"coinbase": fx_coinbase, "kraken": fx_kraken, "bitstamp": fx_bitstamp, "gemini": fx_gemini}
ENABLED = [n for n in os.getenv("PRICE_FEEDS", "coinbase,kraken,bitstamp,gemini").split(",") if n in EXCHANGES]


def reference():
    """Median of fresh exchange prices: a PROXY for CF Benchmarks BRTI, not BRTI itself."""
    now = time.time()
    vals, notes = [], []
    for name in ENABLED:
        ts, v = read("px_" + name)
        if v is not None and now - ts < 15:
            vals.append(v)
        else:
            notes.append(f"{name}: " + ("no data yet" if v is None else f"{now - ts:.0f}s old"))
    if not vals:
        raise RuntimeError("No BTC price feeds available (" + "; ".join(notes) + ")")
    return statistics.median(vals), len(vals)


def level(row):
    price, size = float(row[0]), float(row[1])
    if price > 1.0:
        price /= 100.0
    return price, size


def fetch_book(ticker):
    data = get(f"{KALSHI}/markets/{ticker}/orderbook", {"depth": 10})
    book = data.get("orderbook_fp") or data.get("orderbook") or {}
    yes = [level(r) for r in (book.get("yes_dollars") or book.get("yes") or []) if len(r) > 1]
    no = [level(r) for r in (book.get("no_dollars") or book.get("no") or []) if len(r) > 1]

    yes_size = sum(s for _, s in yes)
    no_size = sum(s for _, s in no)
    total = yes_size + no_size
    best_yes = max(yes) if yes else None  # best YES bid
    best_no = max(no) if no else None     # best NO bid

    return {
        "ticker": ticker,
        "imbalance": (yes_size - no_size) / total if total else 0.0,
        # Buying YES hits the best NO bid (and vice versa).
        "yes_ask_size": best_no[1] if best_no else None,
        "no_ask_size": best_yes[1] if best_yes else None,
    }


def poll_market():
    now = time.time()
    data = get(
        f"{KALSHI}/markets",
        {"series_ticker": SERIES, "status": "open", "limit": 100},
    )
    live = []
    for item in data.get("markets", []):
        close_ts = iso(item.get("close_time"))
        status = str(item.get("status", "")).lower()
        if close_ts is not None and close_ts > now and status in ("", "open", "active"):
            live.append(item)

    m = min(live, key=lambda item: iso(item["close_time"])) if live else None
    put("market", m)
    if m:
        put("book", fetch_book(m["ticker"]))


def poll_history():
    """Recent settled results for the series. Kalshi: status=settled pairs with min_settled_ts."""
    cutoff = time.time() - TREND_HOURS * 3600
    data = get(
        f"{KALSHI}/markets",
        {"series_ticker": SERIES, "status": "settled", "min_settled_ts": int(cutoff), "limit": 100},
    )
    rows = []
    for item in data.get("markets", []):
        close_ts = iso(item.get("close_time"))
        result = str(item.get("result", "")).lower()
        if close_ts is not None and close_ts >= cutoff and result in ("yes", "no"):
            rows.append((close_ts, result))
    rows.sort()
    put("history", rows)


def trend_summary(now):
    ts, rows = read("history")
    rows = [r for r in (rows or []) if now - r[0] <= TREND_HOURS * 3600]
    n = len(rows)
    ups = sum(1 for _, res in rows if res == "yes")
    if ts == 0.0 or now - ts > 600 or n < TREND_MIN:
        label = "Unknown"
    else:
        share = ups / n
        label = "UP" if share >= TREND_THRESHOLD else "DOWN" if share <= 1 - TREND_THRESHOLD else "Mixed"
    return {
        "label": label, "ups": ups, "downs": n - ups, "n": n,
        "recent": "".join("U" if res == "yes" else "D" for _, res in rows[-10:]),
    }


def poll_candles():
    """Last ~5 hours of 1-minute BTC-USD candles (Coinbase public API): [time, low, high, open, close, volume]."""
    rows = get("https://api.exchange.coinbase.com/products/BTC-USD/candles", {"granularity": 60})
    put("candles", sorted((int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])) for r in rows))


def ema_series(vals, n):
    k, e, out = 2.0 / (n + 1), None, []
    for v in vals:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def rsi_value(closes, n=14):
    if len(closes) < n + 1:
        return None
    gain = loss = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gain, loss = gain + max(d, 0), loss + max(-d, 0)
    gain, loss = gain / n, loss / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain, loss = (gain * (n - 1) + max(d, 0)) / n, (loss * (n - 1) + max(-d, 0)) / n
    return 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)


def aggregate(candles, secs):
    bars = {}
    for t, lo, hi, op, cl, vol in candles:
        b = t // secs * secs
        if b not in bars:
            bars[b] = [op, hi, lo, cl, vol]
        else:
            x = bars[b]
            x[1], x[2], x[3], x[4] = max(x[1], hi), min(x[2], lo), cl, x[4] + vol
    return [(b, *bars[b]) for b in sorted(bars)]  # (time, open, high, low, close, volume)


def chart_indicators(candles, btc):
    """Trend-following indicators vote with the trend; RSI/Bollinger only fade extremes. Each is -1..+1."""
    if not candles or len(candles) < 150:
        return None
    bars = aggregate(candles, 300)
    closes = [b[4] for b in bars]
    closes[-1] = btc
    if len(closes) < 36:
        return None

    e9, e21 = ema_series(closes, 9)[-1], ema_series(closes, 21)[-1]
    ema_s = math.tanh((e9 - e21) / e21 / 0.0015)

    macd_line = [a - b for a, b in zip(ema_series(closes, 12), ema_series(closes, 26))]
    hist = macd_line[-1] - ema_series(macd_line, 9)[-1]
    macd_s = math.tanh(hist / btc / 0.0004)

    r = rsi_value(closes, 14)
    rsi_s = clamp((r - 50) / 25, -1, 1) * 0.5
    if r >= 75:
        rsi_s = -0.3
    elif r <= 25:
        rsi_s = 0.3

    w = closes[-20:]
    mid = sum(w) / 20
    sd = (sum((x - mid) ** 2 for x in w) / 20) ** 0.5
    pctb = (btc - (mid - 2 * sd)) / (4 * sd) if sd > 0 else 0.5
    boll_s = -0.5 if pctb > 1 else 0.5 if pctb < 0 else 0.0

    vb = bars[-48:]
    vol = sum(b[5] for b in vb)
    vwap = sum((b[2] + b[3] + b[4]) / 3 * b[5] for b in vb) / vol if vol > 0 else btc
    vwap_s = math.tanh((btc - vwap) / vwap / 0.003)

    score = clamp(0.30 * ema_s + 0.25 * macd_s + 0.20 * vwap_s + 0.15 * rsi_s + 0.10 * boll_s, -1, 1)

    # Volatility regime from 1-minute returns: last 10 minutes vs last 4 hours (per sqrt-second).
    rets = [math.log(candles[i][4] / candles[i - 1][4]) for i in range(1, len(candles)) if candles[i - 1][4] > 0]
    def rms(x):
        return (sum(v * v for v in x) / len(x)) ** 0.5 / math.sqrt(60) if x else 0.0
    s_recent, s_long = rms(rets[-10:]), rms(rets[-240:])
    ratio = s_recent / s_long if s_long > 0 else 1.0
    return {
        "score": score, "ema": ema_s, "macd": macd_s, "vwap": vwap_s, "rsi": r, "pctb": pctb,
        "vol_ratio": ratio, "spike": ratio > 2.0, "sigma_recent": s_recent,
    }


def poll_sentiment():
    data = get(
        "https://api.gdeltproject.org/api/v2/doc/doc",
        {
            "query": "bitcoin BTC",
            "mode": "artlist",
            "format": "json",
            "maxrecords": 20,
            "timespan": "2h",
            "sort": "datedesc",
        },
    )
    headlines, scores = [], []
    for article in data.get("articles", [])[:12]:
        title = article.get("title", "")
        words = re.findall(r"[a-zA-Z]+", title.lower())
        score = clamp(sum(w in POS for w in words) - sum(w in NEG for w in words), -3, 3)
        scores.append(score)
        headlines.append({"title": title, "source": article.get("domain", ""), "score": score})

    average = (sum(scores) / len(scores)) / 3 if scores else 0.0
    put("sentiment", (average, headlines))


def call_with_deadline(fn, seconds):
    """Run fn, but never wait longer than `seconds`. A hang becomes a visible error."""
    box = {}

    def target():
        try:
            box["ok"] = fn()
        except Exception as exc:
            box["err"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(seconds)
    if t.is_alive():
        raise TimeoutError(f"request hung for more than {seconds:.0f}s")
    if "err" in box:
        raise box["err"]
    return box.get("ok")


def run_forever(name, fn, interval, retry=None):
    """Run fn on its own thread. On failure keep the last good value and retry sooner."""

    def loop():
        while True:
            delay = interval
            try:
                call_with_deadline(fn, 20)
                feed_errors.pop(name, None)
            except Exception as exc:
                msg = f"{type(exc).__name__}: {exc}"[:300]
                if feed_errors.get(name) != msg:
                    print(f"[{name}] {msg}", flush=True)  # shows up in the Render log
                feed_errors[name] = msg
                delay = retry or interval
            time.sleep(delay)

    threading.Thread(target=loop, name=name, daemon=True).start()


# --- CSV logging + outcome tracking -------------------------------------------
SIGNAL_FIELDS = [
    "ts", "ticker", "close_time", "strike", "btc", "seconds_left", "p_up",
    "yes_ask", "no_ask", "edge", "action", "entry_price", "ask_size",
    "ref_sources", "expected_avg", "sigma", "momentum", "book", "sentiment",
    "trend", "trend_ups", "trend_n", "trend_blocked",
    "tech_score", "rsi", "vol_ratio",
]
OUTCOME_FIELDS = ["ticker", "result", "settled_at", "expiration_value", "proxy_avg60", "gap"]
SIGNALS_CSV = os.path.join(LOG_DIR, "signals.csv")
OUTCOMES_CSV = os.path.join(LOG_DIR, "outcomes.csv")
ALERT_FIELDS = ["ts", "ticker", "action", "grade", "entry_price", "p_up", "edge", "seconds_left", "btc", "strike", "trend", "trend_ups", "trend_n", "tech_score"]
ALERTS_CSV = os.path.join(LOG_DIR, "alerts.csv")

log_lock = threading.Lock()
pending = {}  # ticker -> close_ts, waiting for settlement
_last_log = 0.0


def append_row(path, fields, row):
    with log_lock:
        os.makedirs(LOG_DIR, exist_ok=True)
        if os.path.exists(path):  # schema changed: keep the old file, start a new one
            with open(path, newline="") as f:
                header = f.readline().strip().split(",")
            if header != fields:
                os.replace(path, f"{path}.{int(time.time())}.old")
        new = not os.path.exists(path)
        with open(path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if new:
                writer.writeheader()
            writer.writerow(row)


def read_csv(path):
    try:
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def load_pending():
    settled = {r["ticker"] for r in read_csv(OUTCOMES_CSV)}
    for r in read_csv(SIGNALS_CSV):
        if r["ticker"] not in settled:
            try:
                pending[r["ticker"]] = float(r["close_time"])
            except (ValueError, KeyError):
                pass


def log_signal(s):
    global _last_log
    now = time.time()
    if not s.get("ok") or now - _last_log < LOG_EVERY:
        return
    _last_log = now

    action = s["action"]
    entry = s["yes_ask"] if action == "BUY UP" else s["no_ask"] if action == "BUY DOWN" else ""
    size = s["yes_ask_size"] if action == "BUY UP" else s["no_ask_size"] if action == "BUY DOWN" else ""
    append_row(SIGNALS_CSV, SIGNAL_FIELDS, {
        "ts": round(now, 2),
        "ticker": s["ticker"],
        "close_time": s["close_time"],
        "strike": s["strike"],
        "btc": s["btc"],
        "seconds_left": round(s["seconds_left"], 1),
        "p_up": round(s["p_up"], 4),
        "yes_ask": s["yes_ask"],
        "no_ask": s["no_ask"],
        "edge": "" if s["edge"] is None else round(s["edge"], 4),
        "action": action,
        "entry_price": entry,
        "ask_size": "" if size is None else size,
        "ref_sources": s["ref_sources"],
        "expected_avg": round(s["expected_avg"], 2),
        "sigma": s["sigma"],
        "momentum": round(s["momentum"], 4),
        "book": round(s["book"], 4),
        "sentiment": round(s["sent"], 4),
        "trend": s["trend"]["label"],
        "trend_ups": s["trend"]["ups"],
        "trend_n": s["trend"]["n"],
        "trend_blocked": s["trend_blocked"] or "",
        "tech_score": round(s["tech"]["score"], 3) if s["tech"] else "",
        "rsi": round(s["tech"]["rsi"], 1) if s["tech"] else "",
        "vol_ratio": round(s["tech"]["vol_ratio"], 2) if s["tech"] else "",
    })
    pending[s["ticker"]] = s["close_time"]


def window_average(close_ts):
    """Our proxy's average over the 60 seconds before close (what Kalshi averages)."""
    with samples_lock:
        pts = [p for t, p in samples if close_ts - 60 <= t <= close_ts]
    return sum(pts) / len(pts) if len(pts) >= 10 else None


alerted = set()


def load_alerted():
    alerted.update(r["ticker"] for r in read_csv(ALERTS_CSV))


def notify(title, body, priority="high"):
    """Push to ntfy.sh and/or Telegram, whichever is configured."""
    if NTFY_TOPIC:
        try:
            requests.post(
                f"https://ntfy.sh/{NTFY_TOPIC}",
                data=body.encode("utf-8"),
                headers={"Title": title, "Priority": priority},
                timeout=6,
            ).raise_for_status()
            feed_errors.pop("alert_ntfy", None)
        except Exception as exc:
            feed_errors["alert_ntfy"] = f"{type(exc).__name__}: {exc}"
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT, "text": f"{title}\n{body}"},
                timeout=6,
            ).raise_for_status()
            feed_errors.pop("alert_telegram", None)
        except Exception as exc:
            feed_errors["alert_telegram"] = type(exc).__name__  # never echo the token-bearing URL


def maybe_alert(s):
    """Once per market, when the 5:00 decision is made: log it, and push it if alerts are configured."""
    cp = s.get("checkpoint")
    if not s.get("ok") or not cp or s["ticker"] in alerted:
        return
    if len(alerted) > 500:
        alerted.clear()
    alerted.add(s["ticker"])

    price = cp["price"]
    append_row(ALERTS_CSV, ALERT_FIELDS, {
        "ts": round(time.time(), 2), "ticker": s["ticker"], "action": cp["action"], "grade": cp["grade"],
        "entry_price": "" if price is None else price, "p_up": round(s["p_up"], 4),
        "edge": "" if cp["edge"] is None else round(cp["edge"], 4),
        "seconds_left": round(cp["left"], 1), "btc": s["btc"], "strike": s["strike"],
        "trend": s["trend"]["label"], "trend_ups": s["trend"]["ups"], "trend_n": s["trend"]["n"],
        "tech_score": round(s["tech"]["score"], 3) if s["tech"] else "",
    })
    left = cp["left"]
    edge_txt = "n/a" if cp["edge"] is None else f"{cp['edge']*100:+.1f} pts after fees"
    price_txt = "n/a" if price is None else f"{price * 100:.0f}c"
    body = "\n".join([
        f"{cp['action']} ({cp['grade'].lower()}) at {int(left // 60)}:{int(left % 60):02d} left",
        f"Model {cp['prob'] * 100:.0f}% | ask {price_txt} | edge {edge_txt}",
        f"BTC (proxy) {s['btc']:,.0f} vs target {s['strike']:,.0f}",
        s["ticker"],
        "Snapshot only. Check the live price before acting.",
    ])
    threading.Thread(target=notify, args=(cp["action"], body), daemon=True).start()


checkpoints = {}  # ticker -> frozen 5:00 decision


def attach_checkpoint(s):
    """Freeze the decision from the first reading at the 5:00 mark; keep it until the market closes."""
    if not s.get("ok"):
        return
    t, left = s["ticker"], s["seconds_left"]
    if ALERT_AT - ALERT_WINDOW <= left <= ALERT_AT and t not in checkpoints:
        checkpoints[t] = {**s["decision"], "left": left}
    while len(checkpoints) > 20:
        checkpoints.pop(next(iter(checkpoints)))
    s["checkpoint"] = checkpoints.get(t)
    s["alert_at"], s["alert_window"] = ALERT_AT, ALERT_WINDOW


def poll_outcomes():
    for ticker, close_ts in list(pending.items()):
        if time.time() < close_ts + 30:
            continue
        m = get(f"{KALSHI}/markets/{ticker}").get("market", {})
        result = str(m.get("result", "")).lower()
        if result not in ("yes", "no"):
            continue
        try:
            exp = float(m.get("expiration_value"))
        except (TypeError, ValueError):
            exp = None
        proxy = window_average(close_ts)
        gap = proxy - exp if exp is not None and proxy is not None else None
        append_row(OUTCOMES_CSV, OUTCOME_FIELDS, {
            "ticker": ticker, "result": result, "settled_at": round(time.time(), 1),
            "expiration_value": "" if exp is None else exp,
            "proxy_avg60": "" if proxy is None else round(proxy, 2),
            "gap": "" if gap is None else round(gap, 2),
        })
        pending.pop(ticker, None)


def compute_stats():
    outcomes = {r["ticker"]: r["result"] for r in read_csv(OUTCOMES_CSV)}
    first_signal = {}
    for r in read_csv(ALERTS_CSV):
        first_signal.setdefault(r["ticker"], r)

    gaps = []
    for r in read_csv(OUTCOMES_CSV):
        try:
            gaps.append(float(r["gap"]))
        except (ValueError, KeyError, TypeError):
            pass

    rows = []
    for ticker, r in first_signal.items():
        result = outcomes.get(ticker)
        if result is None:
            continue
        won = (r["action"] == "BUY UP") == (result == "yes")
        try:
            price = float(r["entry_price"])
            pnl = (1.0 if won else 0.0) - price - fee(price)
        except (ValueError, TypeError):
            pnl = None
        rows.append((r.get("grade") or "STRONG", won, pnl))

    def summarize(sel):
        n = len(sel)
        wins = sum(1 for _, w, _ in sel if w)
        ps = [p for _, _, p in sel if p is not None]
        return {"n": n, "wins": wins, "win_rate": wins / n if n else None,
                "avg_pnl": sum(ps) / len(ps) if ps else None}

    overall = summarize(rows)
    return {
        "markets_resolved": len(outcomes),
        "ref_gap_samples": len(gaps),
        "ref_gap_mean": sum(gaps) / len(gaps) if gaps else None,
        "ref_gap_mean_abs": sum(abs(g) for g in gaps) / len(gaps) if gaps else None,
        "signals_resolved": overall["n"],
        "wins": overall["wins"],
        "win_rate": overall["win_rate"],
        "avg_pnl_per_contract": overall["avg_pnl"],
        "by_grade": {g: summarize([x for x in rows if x[0] == g]) for g in ("STRONG", "LEAN", "COIN FLIP")},
        "note": "5:00 decisions, priced at the ask, minus estimated fees.",
    }


# --- Signal computation -------------------------------------------------------
def offline(btc, error):
    return {"ok": False, "action": "WAIT", "btc": btc, "error": error}


def compute():
    btc, n_src = reference()
    now = time.time()
    with samples_lock:
        samples.append((now, btc))

    m_ts, m = read("market")
    if m_ts == 0.0:
        return offline(btc, "Waiting for Kalshi market data...")
    if now - m_ts > 20:
        return offline(btc, "Kalshi market data is stale.")
    if not m:
        return offline(btc, f"No active {SERIES} market found.")

    close_ts = iso(m.get("close_time"))
    if close_ts is None or close_ts <= now:
        return offline(btc, "Waiting for the next market to open.")

    strike_value = m.get("floor_strike", m.get("strike"))
    if strike_value is None:
        return offline(btc, "Active market has no strike price.")
    strike = float(strike_value)
    seconds_left = close_ts - now

    # Volatility: realised, per-sqrt-second.
    with samples_lock:
        points = list(samples)
    returns = []
    for (t0, p0), (t1, p1) in zip(points, points[1:]):
        if p0 > 0 and p1 > 0 and t1 > t0:
            returns.append((math.log(p1 / p0), t1 - t0))

    total_dt = sum(dt for _, dt in returns)
    if len(returns) >= 20 and total_dt > 0:
        sigma = math.sqrt(sum(r * r for r, _ in returns) / total_dt)
    else:
        sigma = 0.0
    sigma = max(sigma, SIGMA_FLOOR)

    # Chart indicators + volatility spike awareness (needs fresh candle data).
    c_ts, candles = read("candles")
    tech = chart_indicators(candles, btc) if candles and now - c_ts < 180 else None
    if tech:
        sigma = max(sigma, tech["sigma_recent"])  # a sudden burst of volatility widens the odds toward 50/50
    tech_score = tech["score"] if tech else 0.0

    # Kalshi settles on the AVERAGE of the index over the final 60s, not the last tick.
    # >60s left: variance of that average behaves like (seconds_left - 40) seconds of drift.
    # Inside the window: part of the average is already known, the rest is still random.
    if seconds_left > 60:
        expected, t_eff = btc, seconds_left - 40.0
    else:
        elapsed = 60.0 - seconds_left
        known = [p for t, p in points if close_ts - 60 <= t <= now]
        known_avg = sum(known) / len(known) if known else btc
        expected = (elapsed / 60.0) * known_avg + (seconds_left / 60.0) * btc
        t_eff = seconds_left ** 3 / 10800.0
    t_eff = max(t_eff, 0.25)
    z = math.log(expected / strike) / (sigma * math.sqrt(t_eff)) + TECH_Z * tech_score
    p_gauss = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

    # Prices. Fall back to the binary complement when one side is empty.
    yes_bid = dollars(m, "yes_bid")
    yes_ask = dollars(m, "yes_ask")
    no_bid = dollars(m, "no_bid")
    no_ask = dollars(m, "no_ask")
    if not valid_price(yes_ask) and valid_price(no_bid):
        yes_ask = round(1.0 - no_bid, 4)
    if not valid_price(no_ask) and valid_price(yes_bid):
        no_ask = round(1.0 - yes_bid, 4)
    yes_ask = yes_ask if valid_price(yes_ask) else None
    no_ask = no_ask if valid_price(no_ask) else None

    # Order book (only if it belongs to the current market and is fresh).
    b_ts, b = read("book")
    if b and b["ticker"] == m["ticker"] and now - b_ts < 15:
        book, yes_size, no_size = b["imbalance"], b["yes_ask_size"], b["no_ask_size"]
    else:
        book, yes_size, no_size = 0.0, None, None

    _, (sent, headlines) = read("sentiment")

    def past(seconds):
        old = [price for timestamp, price in points if now - timestamp >= seconds]
        return old[-1] if old else None

    p60, p180 = past(60), past(180)
    m60 = (btc / p60 - 1.0) if p60 else 0.0
    m180 = (btc / p180 - 1.0) if p180 else 0.0
    momentum = 0.6 * clamp(m60 / 0.002, -1, 1) + 0.4 * clamp(m180 / 0.004, -1, 1)

    trend = trend_summary(now)
    trend_score = (trend["ups"] - trend["downs"]) / trend["n"] if trend["label"] != "Unknown" and trend["n"] else 0.0
    technical = clamp(p_gauss + 0.07 * momentum + 0.04 * book + 0.03 * sent + TREND_WEIGHT * trend_score, 0.05, 0.95)

    # Blend in the market's own view only when it has real quotes.
    if yes_bid > 0 and yes_ask:
        mid = (yes_bid + yes_ask) / 2.0
    else:
        mid = yes_ask or (yes_bid if yes_bid > 0 else None)
    probability_up = clamp(0.70 * technical + 0.30 * mid, 0.05, 0.95) if mid is not None else technical
    probability_down = 1.0 - probability_up

    # Edge is net of estimated fees. A missing ask means no edge, never a free ticket.
    up_edge = probability_up - yes_ask - fee(yes_ask) if yes_ask else None
    down_edge = probability_down - no_ask - fee(no_ask) if no_ask else None

    action, edge, ask_size = "WAIT", None, None
    if up_edge is not None and up_edge >= MIN_EDGE and probability_up >= MIN_PROB:
        action, edge, ask_size = "BUY UP", up_edge, yes_size
    elif down_edge is not None and down_edge >= MIN_EDGE and probability_down >= MIN_PROB:
        action, edge, ask_size = "BUY DOWN", down_edge, no_size
    else:
        edges = [e for e in (up_edge, down_edge) if e is not None]
        edge = max(edges) if edges else None

    thin = action != "WAIT" and ask_size is not None and ask_size < MIN_ASK_SIZE
    if thin:
        action = "WAIT"

    # Trend filter: don't BUY against a clear 4-hour run of settled results.
    trend_blocked = None
    if TREND_MODE == "filter":
        if action == "BUY UP" and trend["label"] == "DOWN":
            trend_blocked = f"BUY UP blocked: {TREND_HOURS:g}h results lean DOWN"
        elif action == "BUY DOWN" and trend["label"] == "UP":
            trend_blocked = f"BUY DOWN blocked: {TREND_HOURS:g}h results lean UP"
        if trend_blocked:
            action = "WAIT"

    # The 5:00 decision is never WAIT: pick the side the model favors, and grade how strong it is.
    up_side = probability_up >= probability_down
    d_prob = probability_up if up_side else probability_down
    decision = {
        "action": "BUY UP" if up_side else "BUY DOWN",
        "grade": "STRONG" if action != "WAIT" else "LEAN" if d_prob >= 0.58 else "COIN FLIP",
        "prob": d_prob,
        "price": yes_ask if up_side else no_ask,
        "edge": up_edge if up_side else down_edge,
        "size": yes_size if up_side else no_size,
    }

    strongest = max(probability_up, probability_down)
    side = "UP" if probability_up >= probability_down else "DOWN"
    reasons = [
        {
            "type": "good" if strongest >= MIN_PROB else "neutral",
            "text": (
                f"Model probability is {strongest*100:.1f}% for {side}."
                if strongest >= MIN_PROB
                else "Probability threshold is not strong enough for a buy signal."
            ),
        },
        {
            "type": "good" if momentum > 0.15 else "bad" if momentum < -0.15 else "neutral",
            "text": f"Short-term momentum: {momentum*100:+.1f} normalized.",
        },
        {
            "type": "good" if book > 0.15 else "bad" if book < -0.15 else "neutral",
            "text": f"Order-book imbalance: {book*100:+.1f}%.",
        },
        {
            "type": "good" if sent > 0.15 else "bad" if sent < -0.15 else "neutral",
            "text": f"News sentiment: {sent*100:+.1f}%.",
        },
    ]
    reasons.append({
        "type": "good" if n_src >= 3 else "bad" if n_src < 2 else "neutral",
        "text": f"Price is a {n_src}-exchange median PROXY for Kalshi's reference index (CF BRTI), not the index itself.",
    })
    if yes_ask is None and no_ask is None:
        reasons.append({"type": "bad", "text": "No usable ask prices; signal disabled."})
    elif edge is not None:
        reasons.append({"type": "neutral", "text": f"Edge shown is after estimated fees ({FEE_RATE:.0%} x p x (1-p))."})
    if thin:
        reasons.append({"type": "bad", "text": f"Only {ask_size:g} contracts at the ask (minimum {MIN_ASK_SIZE:g}); signal blocked."})

    if tech:
        lean = "UP" if tech_score > 0.1 else "DOWN" if tech_score < -0.1 else "neither side"
        reasons.append({"type": "good" if abs(tech_score) > 0.35 else "neutral",
                        "text": f"Chart indicators lean {lean} (score {tech_score:+.2f})." + (" Volatility spike: odds pulled toward 50/50." if tech["spike"] else "")})
    else:
        reasons.append({"type": "neutral", "text": "Chart history still loading; indicators inactive."})
    if trend["label"] == "Unknown":
        reasons.append({"type": "neutral", "text": f"{TREND_HOURS:g}h result history not available yet; trend filter inactive."})
    else:
        reasons.append({
            "type": "bad" if trend_blocked else "neutral",
            "text": f"Last {trend['n']} settled markets: {trend['ups']} UP / {trend['downs']} DOWN ({trend['label']})."
                    + (f" {trend_blocked}." if trend_blocked else ""),
        })

    return {
        "ok": True,
        "btc": btc,
        "strike": strike,
        "seconds_left": seconds_left,
        "close_time": close_ts,
        "ticker": m.get("ticker"),
        "ref_sources": n_src,
        "expected_avg": expected,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "yes_ask_size": yes_size,
        "no_ask_size": no_size,
        "p_up": probability_up,
        "p_down": probability_down,
        "edge": edge,
        "action": action,
        "sigma": sigma,
        "trend": trend,
        "trend_blocked": trend_blocked,
        "decision": decision,
        "tech": tech,
        "momentum": momentum,
        "book": book,
        "sent": sent,
        "momentum_label": "Bullish" if momentum > 0.15 else "Bearish" if momentum < -0.15 else "Mixed",
        "volatility_label": "Spike" if tech and tech["spike"] else "Elevated" if sigma > SIGMA_ELEVATED else "Normal",
        "book_label": "Bid-heavy" if book > 0.15 else "Ask-heavy" if book < -0.15 else "Balanced",
        "sentiment_label": "Bullish" if sent > 0.15 else "Bearish" if sent < -0.15 else "Neutral",
        "reasons": reasons,
        "headlines": headlines,
        "error": "",
    }


def update_state():
    global state
    try:
        new_state = compute()
    except Exception as exc:
        new_state = {
            "ok": False,
            "action": "WAIT",
            "error": f"Data update failed: {type(exc).__name__}: {exc}",
        }

    try:
        attach_checkpoint(new_state)
    except Exception as exc:
        feed_errors["checkpoint"] = f"{type(exc).__name__}: {exc}"

    with state_changed:
        state = new_state
        state_changed.notify_all()

    try:
        log_signal(new_state)
        maybe_alert(new_state)
    except Exception as exc:  # logging must never break the signal
        feed_errors["log"] = f"{type(exc).__name__}: {exc}"
    return new_state


def loop():
    while True:
        update_state()
        time.sleep(0.5)


# --- Optional: save results to a private GitHub repo so they survive Render restarts ---------
# Inactive unless GITHUB_TOKEN and GITHUB_DATA_REPO are set. Saves only alerts.csv and
# outcomes.csv (a few rows per market). On startup it merges the saved copy back in.
import base64
import hashlib
import io

GH_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GH_REPO = os.getenv("GITHUB_DATA_REPO", "").strip()           # "owner/name"
GH_BRANCH = os.getenv("GITHUB_DATA_BRANCH", "main").strip()
GH_API = os.getenv("GITHUB_API_URL", "https://api.github.com").rstrip("/")
GH_ENABLED = bool(GH_TOKEN and GH_REPO)
GH_FILES = {"alerts.csv": (ALERTS_CSV, ALERT_FIELDS), "outcomes.csv": (OUTCOMES_CSV, OUTCOME_FIELDS)}
_gh_state = {n: {"ready": False, "sha": None, "remote_lines": 0, "pushed_hash": None} for n in GH_FILES}
_gh_lock = threading.Lock()


def _gh(method, name, accept="application/vnd.github+json", **kw):
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}", "Accept": accept,
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "BTC15-Signal-Bot",
    }
    return requests.request(method, f"{GH_API}/repos/{GH_REPO}/contents/{name}",
                            headers=headers, timeout=(5.0, 10.0), **kw)


def _merge_csv(path, fields, remote_rows):
    """Remote rows first, then any local rows the remote lacks; always written with the current columns."""
    with log_lock:
        local_rows = read_csv(path)
        seen = {r.get("ticker") for r in remote_rows}
        merged = remote_rows + [r for r in local_rows if r.get("ticker") not in seen]
        os.makedirs(LOG_DIR, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", restval="")
            w.writeheader()
            w.writerows(merged)
        os.replace(tmp, path)


def pending_from_alerts():
    """After a restart, markets decided but not yet settled still need their results recorded."""
    settled = {r["ticker"] for r in read_csv(OUTCOMES_CSV)}
    for r in read_csv(ALERTS_CSV):
        if r.get("ticker") and r["ticker"] not in settled:
            try:
                pending[r["ticker"]] = float(r["ts"]) + float(r["seconds_left"])
            except (ValueError, KeyError, TypeError):
                pass


def _gh_restore_file(name):
    """Pull the saved copy and merge it in. Returns True if local gained rows. Marks the file ready."""
    st, (path, fields) = _gh_state[name], GH_FILES[name]
    r = _gh("GET", name, params={"ref": GH_BRANCH})
    if r.status_code == 404:  # nothing saved yet
        st.update(ready=True, sha=None, remote_lines=0)
        return False
    r.raise_for_status()
    meta = r.json()
    st["sha"] = meta.get("sha")
    if meta.get("content"):
        raw = base64.b64decode(meta["content"])
    else:  # larger than 1 MB: ask for the raw bytes
        g = _gh("GET", name, accept="application/vnd.github.raw+json", params={"ref": GH_BRANCH})
        g.raise_for_status()
        raw = g.content
    st["remote_lines"] = raw.count(b"\n")
    remote_rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig", errors="replace"))))
    local_keys = {r.get("ticker") for r in read_csv(path)}
    changed = any(r.get("ticker") not in local_keys for r in remote_rows)
    if changed:
        _merge_csv(path, fields, remote_rows)
    st["ready"] = True
    return changed


def _gh_push_file(name):
    st, (path, _) = _gh_state[name], GH_FILES[name]
    if not st["ready"] or not os.path.exists(path):  # never write before we know what is saved
        return
    with open(path, "rb") as f:
        data = f.read()
    digest = hashlib.sha1(data).hexdigest()
    if digest == st["pushed_hash"]:
        return
    if data.count(b"\n") < st["remote_lines"]:
        print(f"[github] {name}: local copy has fewer rows than the saved one; not overwriting", flush=True)
        return
    body = {"message": f"data sync {name}", "content": base64.b64encode(data).decode(), "branch": GH_BRANCH}
    if st["sha"]:
        body["sha"] = st["sha"]
    r = _gh("PUT", name, json=body)
    if r.status_code in (409, 422):  # file changed underneath us: refresh its version and retry once
        g = _gh("GET", name, params={"ref": GH_BRANCH})
        if g.status_code == 200:
            body["sha"] = st["sha"] = g.json().get("sha")
        elif g.status_code == 404:
            body.pop("sha", None)
            st["sha"] = None
        r = _gh("PUT", name, json=body)
    r.raise_for_status()
    st.update(sha=r.json()["content"]["sha"], pushed_hash=digest, remote_lines=data.count(b"\n"))


def github_sync():
    if not _gh_lock.acquire(blocking=False):
        return
    try:
        gained = False
        for name in GH_FILES:
            if not _gh_state[name]["ready"]:
                gained = _gh_restore_file(name) or gained
        if gained:
            load_alerted()
            pending_from_alerts()
        for name in GH_FILES:
            _gh_push_file(name)
    finally:
        _gh_lock.release()


def restore_from_github():
    if not GH_ENABLED:
        return
    print(f"[github] saving results to {GH_REPO} ({GH_BRANCH})", flush=True)
    t = threading.Thread(target=github_sync, daemon=True)
    t.start()
    t.join(15)  # give the first restore a short head start; the loop below keeps retrying
    run_forever("github_sync", github_sync, 120, retry=60)


def start_price_feeds():
    for name in ENABLED:
        def work(name=name, fn=EXCHANGES[name]):
            put("px_" + name, fn())
        run_forever("px_" + name, work, 1.5, retry=2)


restore_from_github()  # does nothing unless GITHUB_TOKEN and GITHUB_DATA_REPO are set
load_pending()
load_alerted()
if ALERT_ON_START and (NTFY_TOPIC or (TG_TOKEN and TG_CHAT)):
    threading.Thread(
        target=notify,
        args=("Signal bot online", "Alerts are connected. You'll get a message when a BUY UP / BUY DOWN fires at about 5 minutes left.", "default"),
        daemon=True,
    ).start()
start_price_feeds()
print(f"started; price feeds: {ENABLED}", flush=True)
run_forever("market", poll_market, 2.0, retry=3)
run_forever("history", poll_history, 60, retry=15)
run_forever("candles", poll_candles, 30, retry=10)
run_forever("sentiment", poll_sentiment, 120, retry=20)  # failures retry fast, keep last headlines
run_forever("outcomes", poll_outcomes, 30)
threading.Thread(target=loop, name="signal-updater", daemon=True).start()


# --- Routes -------------------------------------------------------------------
def no_cache(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.get("/health")
def health():
    with state_lock:
        current = dict(state)
    return jsonify({
        "ok": bool(current.get("ok")),
        "error": current.get("error", ""),
        "feeds": feed_errors,
        "uptime_s": round(time.time() - STARTED),
        "price_feeds": ENABLED,
    })


_probe_last = 0.0


@app.get("/probe")
def probe():
    """Live connectivity test: fetches each upstream once, right now, and reports timing/errors."""
    global _probe_last
    if time.time() - _probe_last < 5:
        return jsonify({"error": "wait a few seconds between probes"}), 429
    _probe_last = time.time()

    targets = dict(EXCHANGES)
    targets["kalshi"] = lambda: len(get(f"{KALSHI}/markets", {"series_ticker": SERIES, "status": "open", "limit": 5}).get("markets", []))
    out = {}

    def run(name, fn):
        t0 = time.time()
        try:
            out[name] = {"result": fn(), "seconds": round(time.time() - t0, 2)}
        except Exception as exc:
            out[name] = {"error": f"{type(exc).__name__}: {exc}"[:200], "seconds": round(time.time() - t0, 2)}

    workers = [threading.Thread(target=run, args=(n, f), daemon=True) for n, f in targets.items()]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=9)
    for n in targets:
        out.setdefault(n, {"error": "no answer within 9 seconds (request is hanging)"})
    out["threads"] = sorted(t.name for t in threading.enumerate())
    out["feeds_have_data"] = {n: read("px_" + n)[1] is not None for n in ENABLED}
    return no_cache(jsonify(out))


@app.get("/api/state")
def api_state():
    with state_lock:
        current = dict(state)
    return no_cache(jsonify(current))


@app.get("/api/stats")
def api_stats():
    return no_cache(jsonify(compute_stats()))


@app.get("/api/log/<name>")
def api_log(name):
    if name not in ("signals.csv", "outcomes.csv", "alerts.csv"):
        return jsonify({"error": "not found"}), 404
    if not os.path.exists(os.path.join(LOG_DIR, name)):
        return jsonify({"error": "no data yet"}), 404
    return no_cache(send_from_directory(LOG_DIR, name, mimetype="text/csv", as_attachment=True))


@app.get("/api/stream")
def api_stream():
    @stream_with_context
    def events():
        with state_changed:
            payload = json.dumps(state, separators=(",", ":"))
        yield f"data: {payload}\n\n"

        while True:
            with state_changed:
                state_changed.wait(timeout=10)
                payload = json.dumps(state, separators=(",", ":"))
            yield f"data: {payload}\n\n"

    response = Response(events(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["Connection"] = "keep-alive"
    return response


@app.get("/")
def index():
    return no_cache(send_from_directory("static", "index.html", max_age=0))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), threaded=True)
