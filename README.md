# BTC 15-Minute Signal Bot

Read-only BTC/Kalshi 15-minute dashboard.

## Render

Build Command:
`pip install -r requirements.txt`

Start Command:
`gunicorn --workers 1 --threads 4 server:app`

## Features

- Live BTC price
- Active Kalshi KXBTC15M market and target
- Countdown tied to the actual market close timestamp
- Kalshi Yes ask
- Model UP/DOWN probability
- Executable edge
- Momentum
- Volatility
- Order-book imbalance
- Recent BTC news sentiment
- Automatic market rollover
- Server-sent live updates
- No order placement or account credentials

This project is read-only. It does not place, cancel, or modify Kalshi orders.
