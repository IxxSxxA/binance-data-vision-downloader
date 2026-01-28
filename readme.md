# Binance Data Downloader

Download historical market data KLINE from Binance and convert to Parquet format.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt
```

```python
# Download all BTCUSDT 1m data
python download_binance_kline.py 
```

### Features
- Download futures/spot data
- Filter by year/month
- Parallel downloads
- Auto-convert to Parquet


Data source: https://data.binance.vision/
