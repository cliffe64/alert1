# Alerting MVP

## Overview
This repository contains an MVP implementation plan for a crypto alerting platform covering Binance/DEX token monitoring, multi-level price alerts, volume/trend rules, notifications, and a Streamlit web UI.

## Requirements
- Python 3.10+
- pip

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use `.venv\\Scripts\\activate`
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env  # Fill in secrets as needed
```

## Running
The project entrypoint is `run.py`. The script currently offers two modes:

```bash
python run.py --help
python run.py --once
python run.py --loop
```

`--once` runs a single iteration of the pipeline, while `--loop` will start the long-running service (currently stubbed).

### DingTalk Configuration

Populate the following environment variables before running the router or demo scripts:

```bash
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=..."
export DINGTALK_SECRET="your-secret-if-enabled"
```

Update `.env` with these variables for local development. The router automatically skips delivery if the webhook is not configured.

### Local Sound Notifier

The standalone notifier keeps polling the SQLite `events` table and plays local audio once per event:

```bash
python -m agent.local_notifier --client-id workstation-1 --poll-interval 5 --min-severity warning
python -m agent.local_notifier --self-test
```

State is persisted in the `local_notifier_state` table so that no alerts are missed across restarts.

### Docker & Compose

Build and launch the service/UI stack with the provided artifacts:

```bash
make docker-build
docker-compose up --build
```

The SQLite database is stored inside the `alert-data` volume and mounted into both the core service and Streamlit UI containers.

### Demo Data

To try the full pipeline without live market data, load the bundled sample dataset:

```bash
make demo
```

This command recreates the database schema, ingests demo 1m bars, runs rollups, triggers rules, and dispatches notifications to the configured channels (local playback/DingTalk if available).

### Backtest Utilities

Replay historical events and generate performance statistics/plots:

```bash
python -m backtest.replay --symbols BTCUSDT,ETHUSDT --days 7 --timeframe 5m
```

Outputs are written to `backtest/out/` including per-event CSV, summary CSV, and a return distribution chart.

## Project Structure
```
connectors/            # Market data adapters
aggregator/            # Timeframe aggregation
indicators/            # Technical indicators
rules/                 # Rule engines
alerts/                # Notification channels and router
ui/                    # Streamlit UI
agent/                 # Local sound agent
storage/               # SQLite management and migrations
backtest/              # Backtesting utilities
demo/                  # Sample data loaders
config.yaml            # Main configuration
dotenv example         # Environment variables sample
requirements.txt       # Python dependencies
run.py                 # Main entrypoint
```
