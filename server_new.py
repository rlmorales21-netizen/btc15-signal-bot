import math, os, time, threading, re, json
from collections import deque
from datetime import datetime
import requests
from flask import Flask, jsonify, send_from_directory, Response, stream_with_context

app = Flask(__name__, static_folder="static")
KALSHI = "https://external-api.kalshi.com/trade-api/v2"
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
samples = deque(maxlen=900)
state = {"ok": False, "error": "Starting..."}
state_lock = threading.Lock()
state_changed = threading.Condition(state_lock)

# Small caches keep the upstream REST APIs from being hammered while the
# browser still receives state changes immediately through SSE.
cache_lock = threading.Lock()
cache = {
    "btc": (0.0, None),
    "market": (0.0, None),
    "orderbook": {},
    "sentiment": (0.0, (0, [])),
}

POS = set("surge rally rises rising bullish breakout gain gains strong stronger up upside approval adoption inflow inflows demand buy buying positive beat beats growth record high higher".split())
NEG = set("crash falls falling bearish breakdown loss losses weak weaker down downside outflow outflows sell selling negative fear hack hacked fraud ban bans lawsuit risk risks liquidation liquidations".split())


def get(url, params=None):
    r = requests.get(url, params=params, timeout=4)
    r.raise_for_status()
    return r.json()


def iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


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
        u = "https://api.gdeltproject.org/api/v2/doc/doc"
        p = {"query": "bitcoin BTC", "mode": "artlist", "format": "json", "maxrecords": 20, "timespan": "2h", "sort": "datedesc"}
        j = get(u, p)
        arts = j.get("articles", [])[:12]
        out, scores = [], []
        for a in arts:
            title = a.get("title", "")
            words = re.findall(r"[a-zA-Z]+", title.lower())
            score = sum(w in POS for w in words) - sum(w in NEG for w in words)
            score = max(-3, min(3, score))
            scores.append(score)
            out.append({"title": title, "source": a.get("domain", ""), "score": score})
        avg = (sum(scores) / len(scores)) / 3 if scores else 0
        value = (avg, out)
    except Exception:
        value = (0, [])
    with cache_lock:
        cache["sentiment"] = (now, value)
    return value


def market():
    now = time.time()
    with cache_lock:
        ts, value = cache["market"]
        # Contract metadata changes much less frequently than prices.
        if value is not None and now - ts < 5:
            ct = iso(value.get("close_time")) if value else None
            if ct and ct >= now:
                return value
    j = get(KALSHI + "/markets", {"series_ticker": "KXBTC15M", "status": "open", "limit": 100})
    ms = j.get("markets", [])
    if not ms:
        j = get(KALSHI + "/markets", {"series_ticker": "KXBTC15M", "limit": 100})
        ms = j.get("markets", [])
    live = []
    for m in ms:
        ct = iso(m.get("close_time"))
        if ct is not None and ct >= now:
            status = str(m.get("status", "")).lower()
            if status in ("", "open", "active"):
                live.append(m)
    value = min(live, key=lambda m: iso(m["close_time"])) if live else None
    with cache_lock:
        cache["market"] = (now, value)
    return value


def orderbook(ticker):
    now = time.time()
    with cache_lock:
        ts, value = cache["orderbook"].get(ticker, (0, None))
        if value is not None and now - ts < 0.45:
            return value
    try:
        j = get(f"{KALSHI}/markets/{ticker}/orderbook")
        ob = j.get("orderbook_fp") or j.get("orderbook") or {}
        yes = ob.get("yes_dollars") or ob.get("yes") or []
        no = ob.get("no_dollars") or ob.get("no") or []
        y = sum(float(x[1]) for x in yes[:10] if len(x) > 1)
        n = sum(float(x[1]) for x in no[:10] if len(x) > 1)
        value = (y - n) / (y + n) if y + n else 0
    except Exception:
        value = 0
    with cache_lock:
        cache["orderbook"][ticker] = (now, value)
    return value


def dollars(m, key):
    v = m.get(key + "_dollars")
    if v is None:
        v = m.get(key)
    try:
        v = float(v)
        if v > 1.0:
            v /= 100.0
        return max(0.0, min(1.0, v))
    except Exception:
        return 0.0


def compute():
    btc = get_btc()
    now = time.time()
    samples.append((now, btc))
    m = market()
    if not m:
        return {"ok": True, "btc": btc, "action": "WAIT", "error": "No active KXBTC15M market found."}

    strike = m.get("floor_strike", m.get("strike"))
    strike = float(strike) if strike is not None else None
    left = max(0, iso(m["close_time"]) - now)

    rs = []
    pts = list(samples)
    for (t0, p0), (t1, p1) in zip(pts, pts[1:]):
        if p0 > 0 and p1 > 0 and t1 > t0:
            rs.append((math.log(p1 / p0), t1 - t0))
    sigma = None
    if len(rs) >= 20:
        mean = sum(x for x, _ in rs) / len(rs)
        var = sum((x - mean) ** 2 for x, _ in rs) / (len(rs) - 1)
        elapsed = max(1, pts[-1][0] - pts[0][0])
        sigma = math.sqrt(var * len(rs) / elapsed)
    sigma = max(sigma or 0, 0.000000035)

    if strike and left > 0:
        z = math.log(btc / strike) / (sigma * math.sqrt(left))
        p_up = max(.001, min(.999, .5 * (1 + math.erf(z / math.sqrt(2)))))
    else:
        z = 0
        p_up = .5

    yb = dollars(m, "yes_bid")
    ya = dollars(m, "yes_ask")
    nb = dollars(m, "no_bid")
    na = dollars(m, "no_ask")
    if ya == 0 and yb > 0: ya = yb
    if na == 0 and nb > 0: na = nb

    book = orderbook(m["ticker"])
    sent, heads = sentiment()

    def past(sec):
        arr = [p for t, p in pts if now - t >= sec]
        return arr[-1] if arr else None

    p60, p180 = past(60), past(180)
    m60 = (btc / p60 - 1) if p60 else 0
    m180 = (btc / p180 - 1) if p180 else 0
    momentum = 0.6 * max(-1, min(1, m60 / 0.002)) + 0.4 * max(-1, min(1, m180 / 0.004))

    kalshi_mid = (yb + ya) / 2 if (yb or ya) else 0.5
    technical = 0.5 + 0.40 * math.tanh(z / 1.8) if strike and left > 0 else 0.5
    technical += 0.07 * momentum + 0.04 * book + 0.03 * sent
    technical = max(0.08, min(0.92, technical))
    adj = 0.70 * technical + 0.30 * kalshi_mid
    adj = max(.05, min(.95, adj))
    p_down = 1 - adj
    yes_edge = adj - ya
    no_edge = p_down - na if na else 0.0

    if yes_edge >= .07 and adj >= .66:
        action, edge = "BUY UP", yes_edge
    elif no_edge >= .07 and p_down >= .66:
        action, edge = "BUY DOWN", no_edge
    else:
        action, edge = "WAIT", max(yes_edge, no_edge)

    reasons = []
    if adj >= .66:
        reasons.append({"type": "good", "text": f"Model probability is {adj*100:.1f}% for UP."})
    elif p_down >= .66:
        reasons.append({"type": "good", "text": f"Model probability is {p_down*100:.1f}% for DOWN."})
    else:
        reasons.append({"type": "neutral", "text": "Probability threshold is not strong enough for a buy signal."})
    reasons.append({"type": "good" if momentum > .15 else "bad" if momentum < -.15 else "neutral", "text": f"Short-term momentum: {momentum*100:+.1f} normalized."})
    reasons.append({"type": "good" if book > .15 else "bad" if book < -.15 else "neutral", "text": f"Order-book imbalance: {book*100:+.1f}%."})
    reasons.append({"type": "good" if sent > .15 else "bad" if sent < -.15 else "neutral", "text": f"News sentiment: {sent*100:+.1f}%."})

    return {
        "ok": True, "btc": btc, "strike": strike, "seconds_left": left,
        "yes_bid": yb, "yes_ask": ya, "no_bid": nb, "no_ask": na,
        "p_up": adj, "p_down": p_down, "edge": edge, "action": action,
        "momentum_label": "Bullish" if momentum > .15 else "Bearish" if momentum < -.15 else "Mixed",
        "volatility_label": "Elevated" if sigma > 0.00000009 else "Normal",
        "book_label": "Bid-heavy" if book > .15 else "Ask-heavy" if book < -.15 else "Balanced",
        "sentiment_label": "Bullish" if sent > .15 else "Bearish" if sent < -.15 else "Neutral",
        "reasons": reasons, "headlines": heads, "error": ""
    }


def update_state():
    global state
    try:
        new_state = compute()
    except Exception as e:
        new_state = {"ok": False, "action": "WAIT", "error": f"Data update failed: {e}"}
    with state_changed:
        state = new_state
        state_changed.notify_all()
    return new_state


def loop():
    while True:
        update_state()
        # Fast enough for action, while the caches above protect upstream APIs.
        time.sleep(0.5)


_worker = threading.Thread(target=loop, daemon=True)
_worker.start()


@app.get("/api/state")
def api_state():
    with state_lock:
        current = dict(state)
    if not current.get("ok") and current.get("error") == "Starting...":
        current = update_state()
    response = jsonify(current)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.get("/api/stream")
def api_stream():
    @stream_with_context
    def events():
        # Send current state immediately on connection.
        with state_changed:
            last = json.dumps(state, separators=(",", ":"))
        yield f"event: state\ndata: {last}\n\n"

        while True:
            with state_changed:
                state_changed.wait(timeout=10)
                payload = json.dumps(state, separators=(",", ":"))
            yield f"event: state\ndata: {payload}\n\n"

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