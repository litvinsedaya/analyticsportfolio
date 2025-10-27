# Amplitude API Integration

A lightweight Python package for exporting events from **Amplitude Export API**.  
Free to use — useful when you need retro exports or automated backfills.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](./LICENSE)

---

## 🚀 Quick Start

### Setup

Add your credentials to a `.env` file (from [Amplitude project settings](https://amplitude.com/settings)):

```bash
AMPLITUDE_API_KEY=your_api_key_here
AMPLITUDE_SECRET_KEY=your_secret_key_here
```

### Install dependencies

```bash
pip install requests pandas python-dotenv
```

### Test connection

```bash
python amplitude_api_loader/test_connection.py
```

### Export data

```bash
# Yesterday
python amplitude_api_loader/export_events.py

# Specific date
python amplitude_api_loader/export_events.py --date 2024-01-15

# Filter by event type
python amplitude_api_loader/export_events.py --event-type "event_type_here"

# Filter by user ID
python amplitude_api_loader/export_events.py --user-id "123"

# Without progress bars (production mode)
python amplitude_api_loader/export_events.py --no-progress
```

---

## 📂 Files

- `amplitude_client.py` – Amplitude Export API client  
- `export_events.py` – main script for daily exports  
- `test_connection.py` – API connectivity check  
- `example_usage.py` – usage examples  

---

## ⚙️ How it works

1. Downloads data from [Amplitude Export API](https://amplitude.com/docs/apis/analytics/export)  
2. Retrieves a ZIP archive with JSON files (hourly chunks)  
3. Extracts and processes JSON data from all files  
4. Converts them into a single CSV for convenience  

---

## ⏱ Time format

- Input format: `YYYY-MM-DD` (e.g., `2024-01-15`)  
- API format: `YYYYMMDDTHH` (e.g., `20240115T00` to `20240115T23`)  
- Full day: from `T00` to `T23`  

---

## 🔧 Parameters

- `--date` – date in `YYYY-MM-DD` format (default: yesterday)  
- `--event-type` – filter by event type  
- `--user-id` – filter by user ID  
- `--output-dir` – output directory (default: `amplitude/exports`)  
- `--no-progress` – disable progress bars (production)  

---

## ⚠️ Limitations

- Max 4GB per request  
- Max 365 days per request  
- Data available with ~2h delay  
- For large-scale exports consider [Amazon S3 export](https://amplitude.com/docs/data/destination-catalog/amazon-s3#run-a-manual-export)  

---

## 📊 Output

The script produces:  
- ZIP archive with hourly JSON files (0–23)  
- Extracted & combined CSV file  
- All files saved to `amplitude/exports/`  

**Example file structure:**  
- `amplitude_events_20240115.zip` – original archive  
- `amplitude_events_20240115.csv` – combined CSV  
- Archive contains: `projectid_2024-01-15_0#0.json.gz`, `projectid_2024-01-15_1#0.json.gz`, …  

---

## ✅ Data accuracy check

Crosscheck with the Amplitude UI (random date, September 2025):  
- **Exact 100% match** for registration, payments, and key feature events  
- **High-frequency frontend events** show a negligible deviation (~0.2%)  

---

## 🛠 TODO (production)

- Add automatic retries and error handling  
- Implement batch retro exports for long ranges  
- Add scheduling support (e.g., cron, Airflow)  

---

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you’d like to add.  

---

## 📄 License

[MIT](./LICENSE) © 2025 — free for personal and commercial use.