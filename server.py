import json
import math
import os
import re
import threading
import time
from collections import deque
from datetime import datetime

import requests
from flask import Flask, Response, jsonify, send_from_directory, stream_with_context

app = Flask(__name__, static_folder="static")

# Public Kalshi market-data API. No trading credentials are used.
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
SERIES = "KXBTC15M"

samples = deque(maxlen=900)

state = {"ok": False, "action": "WAIT", "error": "Starting..."}
state_lock = threading.Lock()
state_changed = threading.Condition(state_lock)

cache_lock = threading.Lock()
cache = {
    "btc": (0.0, None),
    "market": (0.0, None),
    "orderbook": {},
    "sentiment": (0.0, (0.0, [])),
}

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


def get(url, params=None):
    r = requests.get(
        url,
        params=params,
        timeout=(2.5, 4.0),
        headers={"User-Agent": "BTC15-Signal-Bot/1.0"},
    )
    r.raise_for_status()
    return r.json()


def iso(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def get_btc():
    now = time.time()
    with cache_lock:
        ts, value = cache["btc"]
        if value is not None and now - ts < 0.45:
            return value

    value = float(get(COINBASE)["price"])
    with cache_lock:
        cache["btc"] = (now, value)
    return value


def sentiment():
    now = time.time()
    with cache_lock:
        ts, value = cache["sentiment"]
        if now - ts < 120:
            return value

    try:
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
        articles = data.get("articles", [])[:12]
        headlines = []
        scores = []

        for article in articles:
            title = article.get("title", "")
            words = re.findall(r"[a-zA-Z]+", title.lower())
            score = sum(w in POS for w in words) - sum(w in NEG for w in words)
            score = max(-3, min(3, score))
            scores.append(score)
            headlines.append(
                {
                    "title": title,
                    "source": article.get("domain", ""),
                    "score": score,
                }
            )

        average = (sum(scores) / len(scores)) / 3 if scores else 0.0
        value = (average, headlines)
    except Exception:
        # News is supplemental. A news outage must not take the signal offline.
        value = (0.0, [])

    with cache_lock:
        cache["sentiment"] = (now, value)
    return value


def market():
    now = time.time()

    with cache_lock:
        ts, value = cache["market"]
        if value is not None and now - ts < 5:
            close_ts = iso(value.get("close_time"))
            if close_ts is not None and close_ts > now:
                return value

    data = get(
        f"{KALSHI}/markets",
        {"series_ticker": SERIES, "status": "open", "limit": 100},
    )
    markets = data.get("markets", [])

    live = []
    for item in markets:
        close_ts = iso(item.get("close_time"))
        status = str(item.get("status", "")).lower()
        if close_ts is not None and close_ts > now and status in ("", "open", "active"):
            live.append(item)

    value = min(live, key=lambda item: iso(item["close_time"])) if live else None

    with cache_lock:
        cache["market"] = (now, value)
    return value


def orderbook(ticker):
    now = time.time()
    with cache_lock:
        ts, value = cache["orderbook"].get(ticker, (0.0, None))
        if value is not None and now - ts < 0.45:
            return value

    try:
        data = get(f"{KALSHI}/markets/{ticker}/orderbook", {"depth": 10})
        book = data.get("orderbook_fp") or data.get("orderbook") or {}
        yes = book.get("yes_dollars") or book.get("yes") or []
        no = book.get("no_dollars") or book.get("no") or []

        yes_size = sum(float(row[1]) for row in yes[:10] if len(row) > 1)
        no_size = sum(float(row[1]) for row in no[:10] if len(row) > 1)
        value = (
            (yes_size - no_size) / (yes_size + no_size)
            if yes_size + no_size
            else 0.0
        )
    except Exception:
        value = 0.0

    with cache_lock:
        cache["orderbook"][ticker] = (now, value)
    return value


def dollars(market_data, key):
    value = market_data.get(key + "_dollars")
    if value is None:
        value = market_data.get(key)

    try:
        value = float(value)
        # Older API representations can be cents; current *_dollars fields
        # are already 0..1.
        if value > 1.0:
            value /= 100.0
        return max(0.0, min(1.0, value))
    except Exception:
        return 0.0


def compute():
    btc = get_btc()
    now = time.time()
    samples.append((now, btc))

    m = market()
    if not m:
        return {
            "ok": False,
            "action": "WAIT",
            "btc": btc,
            "error": f"No active {SERIES} market found.",
        }

    close_ts = iso(m.get("close_time"))
    strike_value = m.get("floor_strike", m.get("strike"))
    strike = float(strike_value) if strike_value is not None else None
    seconds_left = max(0.0, close_ts - now) if close_ts else 0.0

    points = list(samples)
    returns = []
    for (t0, p0), (t1, p1) in zip(points, points[1:]):
        if p0 > 0 and p1 > 0 and t1 > t0:
            returns.append((math.log(p1 / p0), t1 - t0))

    if len(returns) >= 20:
        mean = sum(x for x, _ in returns) / len(returns)
        variance = sum((x - mean) ** 2 for x, _ in returns) / (len(returns) - 1)
        elapsed = max(1.0, points[-1][0] - points[0][0])
        sigma = math.sqrt(max(0.0, variance) * len(returns) / elapsed)
    else:
        sigma = 0.0

    sigma = max(sigma, 0.000000035)

    if strike and seconds_left > 0:
        z = math.log(btc / strike) / (sigma * math.sqrt(seconds_left))
        p_up = max(0.001, min(0.999, 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))))
    else:
        z = 0.0
        p_up = 0.5

    yes_bid = dollars(m, "yes_bid")
    yes_ask = dollars(m, "yes_ask")
    no_bid = dollars(m, "no_bid")
    no_ask = dollars(m, "no_ask")

    if yes_ask == 0 and yes_bid > 0:
        yes_ask = yes_bid
    if no_ask == 0 and no_bid > 0:
        no_ask = no_bid

    book = orderbook(m["ticker"])
    sent, headlines = sentiment()

    def past(seconds):
        old = [price for timestamp, price in points if now - timestamp >= seconds]
        return old[-1] if old else None

    p60 = past(60)
    p180 = past(180)
    m60 = (btc / p60 - 1.0) if p60 else 0.0
    m180 = (btc / p180 - 1.0) if p180 else 0.0

    momentum = (
        0.6 * max(-1.0, min(1.0, m60 / 0.002))
        + 0.4 * max(-1.0, min(1.0, m180 / 0.004))
    )

    kalshi_mid = (yes_bid + yes_ask) / 2.0 if (yes_bid or yes_ask) else 0.5
    technical = 0.5 + 0.40 * math.tanh(z / 1.8)
    technical += 0.07 * momentum + 0.04 * book + 0.03 * sent
    technical = max(0.08, min(0.92, technical))

    probability_up = max(0.05, min(0.95, 0.70 * technical + 0.30 * kalshi_mid))
    probability_down = 1.0 - probability_up

    up_edge = probability_up - yes_ask
    down_edge = probability_down - no_ask if no_ask else 0.0

    if up_edge >= 0.07 and probability_up >= 0.66:
        action, edge = "BUY UP", up_edge
    elif down_edge >= 0.07 and probability_down >= 0.66:
        action, edge = "BUY DOWN", down_edge
    else:
        action, edge = "WAIT", max(up_edge, down_edge)

    reasons = [
        {
            "type": (
                "good"
                if probability_up >= 0.66 or probability_down >= 0.66
                else "neutral"
            ),
            "text": (
                f"Model probability is {max(probability_up, probability_down)*100:.1f}% "
                f"for {'UP' if probability_up >= probability_down else 'DOWN'}."
                if max(probability_up, probability_down) >= 0.66
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

    return {
        "ok": True,
        "btc": btc,
        "strike": strike,
        "seconds_left": seconds_left,
        "close_time": close_ts,
        "ticker": m.get("ticker"),
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "p_up": probability_up,
        "p_down": probability_down,
        "edge": edge,
        "action": action,
        "momentum_label": "Bullish" if momentum > 0.15 else "Bearish" if momentum < -0.15 else "Mixed",
        "volatility_label": "Elevated" if sigma > 0.00000009 else "Normal",
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

    with state_changed:
        state = new_state
        state_changed.notify_all()

    return new_state


def loop():
    # Run immediately on startup, then keep the signal fresh.
    while True:
        update_state()
        time.sleep(0.5)


_worker = threading.Thread(target=loop, name="signal-updater", daemon=True)
_worker.start()


@app.get("/health")
def health():
    with state_lock:
        current = dict(state)
    return jsonify({"ok": bool(current.get("ok")), "error": current.get("error", "")})


@app.get("/api/state")
def api_state():
    # Never perform a blocking upstream call inside a web request.
    with state_lock:
        current = dict(state)

    response = jsonify(current)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.get("/api/stream")
def api_stream():
    @stream_with_context
    def events():
        # Send the latest state immediately.
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
    response = send_from_directory("static", "index.html", max_age=0)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), threaded=True)
