# BTC 15-Minute Signal Bot

Read-only BTC/Kalshi 15-minute dashboard. It places no orders and uses no credentials.

## Layout

```
server.py
requirements.txt
static/index.html   <- must be inside static/
```

## Render

Build Command:
`pip install -r requirements.txt`

Start Command:
`gunicorn --worker-class gevent --workers 1 --timeout 0 server:app`

Keep `--workers 1`: the pollers start on import, so more workers would duplicate them and split state. The gevent worker lets many dashboards hold live streams open without starving `/health`.

## What it does

- Live BTC price (Coinbase) and the active Kalshi KXBTC15M market, target, and countdown
- Model UP/DOWN probability from distance-to-target vs. volatility, plus momentum, order-book imbalance, and news sentiment, blended 70/30 with Kalshi's own mid-price
- Edge **after estimated fees** (`FEE_RATE x p x (1-p)` per contract)
- Signal blocked if there is no usable ask price or fewer than `MIN_ASK_SIZE` contracts at the ask
- Slow data (Kalshi market, order book, news) is polled on separate threads, so the signal never stalls
- Logs a row every 5 seconds to `logs/signals.csv`, and each market's result to `logs/outcomes.csv`
- Dashboard "Track record" card: win rate and average P&L of the first BUY signal per resolved market

## Phone alerts

At about 5:00 left on each 15-minute market (a 30-second window, `ALERT_AT_SECONDS` / `ALERT_WINDOW_SECONDS`), the bot checks the signal once. If it is BUY UP or BUY DOWN, it sends one push notification and logs it to `logs/alerts.csv`. If it is WAIT, nothing is sent.

Set one of these:
- `NTFY_TOPIC`: a hard-to-guess name. Install the free ntfy app and subscribe to the same name.
- `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`: for Telegram.

The Track record card only counts alerted signals, so it shows how acting on the alerts would have gone.

## Reference price and target

- **Target:** read directly from Kalshi's market record (`floor_strike`), so it is exact.
- **Settlement:** Kalshi settles KXBTC15M on a 60-second average of the CF Benchmarks Bitcoin Real-Time Index (BRTI). BRTI is licensed data and Kalshi's public API does not stream it.
- **What the bot uses instead:** the median of Coinbase, Kraken, Bitstamp and Gemini prices (`PRICE_FEEDS` to change). The model also treats the outcome as a 60-second average, not the last tick.
- **How far off it is:** after each market settles, the bot records Kalshi's official `expiration_value` next to its own 60-second proxy average and shows the gap on the dashboard. Judge the proxy by that number.
- To use the true index, add a licensed CF Benchmarks feed as another source in `server.py`.

## Settings (environment variables)

| Name | Default | Meaning |
|---|---|---|
| `FEE_RATE` | 0.07 | Estimated taker fee rate. Check Kalshi's current fee schedule. |
| `MIN_EDGE` | 0.07 | Minimum edge after fees |
| `MIN_PROB` | 0.66 | Minimum model probability |
| `MIN_ASK_SIZE` | 5 | Minimum contracts available at the ask |
| `PRICE_FEEDS` | coinbase,kraken,bitstamp,gemini | Exchanges in the proxy median |
| `LOG_DIR` | logs | Where CSVs are written |

Download logs from `/api/log/signals.csv` and `/api/log/outcomes.csv`.

## Limits

- Render's free disk is wiped on redeploy or restart. Use a persistent disk or download the CSVs regularly.
- The price is a PROXY, not Kalshi's reference index. See "Reference price" below.
- News sentiment is simple keyword counting.
- No login. Anyone with the URL can see the dashboard and logs.
- Weights and thresholds are not backtested. Use the track record to judge them before relying on the signal.
