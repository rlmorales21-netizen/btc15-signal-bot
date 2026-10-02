# BTC 15-Minute Signal Bot

Read-only BTC 15-minute signal dashboard for Kalshi KXBTC15M.

Features:
- Live BTC price proxy from Coinbase
- Kalshi 15-minute market/target
- Live countdown
- Executable Kalshi UP price
- Momentum, volatility, order-book imbalance, and news sentiment
- Probability, edge, reasons, and BUY UP / BUY DOWN / WAIT signal
- Server-sent events (SSE) for immediate browser updates
- No Kalshi orders are ever placed

Render:
- Build: `pip install -r requirements.txt`
- Start: `gunicorn --workers 1 --threads 4 server:app`

Project structure:
- `server.py`
- `static/index.html`
- `requirements.txt`
- `README.md`
