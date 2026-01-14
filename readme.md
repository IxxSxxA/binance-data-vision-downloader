# Binance Data Downloader

Download historical market data from Binance and convert to Parquet format.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt
```

# Download all BTCUSDT 1m data
python downloader.py

# Download only 2024-2025
python downloader.py --years 2024 2025

# Download with 10 parallel threads
python downloader.py --workers 10

# Download ETHUSDT 5m data
python downloader.py --symbol ETHUSDT --interval 5m


Features
Download futures/spot data

Filter by year/month

Parallel downloads

Auto-convert to Parquet

Handle both CSV formats (with/without header)

Usage
```bash
python downloader.py --help
```
Data source: https://data.binance.vision/