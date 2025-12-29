# Worldcoin Market Analysis Using ETL Pipeline and Time-Series Data

## Overview
This repository contains the implementation and experimental framework for the research paper:

> **Worldcoin Market Analysis Using ETL Pipeline and Time-Series Data**  
> Published in the **2025 IEEE International Conference on Communication, Computer, and Information Technology (IC3IT)**

The project presents a **scalable, modular, and ML-ready ETL pipeline** designed for **high-frequency cryptocurrency analytics**, specifically targeting **Worldcoin (WLD)** market data. Unlike traditional CSV-based workflows, this system enables near real-time ingestion, transformation, and structured storage of market data, supporting advanced time-series analysis and predictive modeling.

---

## Publication
- **Conference:** IEEE IC3IT 2025  
- **Paper Title:** *Worldcoin Market Analysis Using ETL Pipeline and Time-Series Data*  
- **Status:** Presented at IEEE IC3IT  
- **IEEE Xplore:** *(Link will be added after publication)*

> This repository accompanies the peer-reviewed IEEE IC3IT paper and serves as a reproducible research artifact.

---

## Research Motivation
Most cryptocurrency analytics systems suffer from:
- Reliance on static CSV datasets
- Lack of real-time ingestion
- Absence of technical indicators for quantitative analysis
- Limited scalability for high-frequency data

This work addresses these limitations by proposing a **real-time ETL architecture** that directly ingests market data from the **Binance API**, enriches it with financial and temporal features, and stores it in a relational database optimized for analytics and machine learning.

---

## Key Contributions
- First ETL pipeline tailored for **high-frequency Worldcoin (WLD) data**
- Automated ingestion of **5-minute interval data** with ~15s latency
- Feature engineering with **SMA, EMA, and TMA** indicators
- Robust preprocessing including missing value handling and noise reduction
- Modular, extensible architecture suitable for ML and forecasting models
- Benchmarking against CSV-based and Kafka-based ETL systems

---

## Dataset Summary
- **Asset:** Worldcoin (WLD)
- **Source:** Binance REST API
- **Time Span:** July 2023 – May 2025
- **Frequency:** 5-minute intervals
- **Records:** 193,000+
- **Latency:** ~15 seconds (near real-time)

---

## System Architecture
The ETL pipeline follows a layered architecture:

1. **API Layer** – Binance REST API for market data
2. **Processing Engine** – Extraction, validation, transformation
3. **Database Layer** – MySQL 8.0 with indexing and ACID compliance
4. **Analytics Layer** – Visualization and ML-ready datasets

Key engineering features include:
- Schema validation
- Retry and backoff mechanisms
- Transaction safety and rollback
- Timestamp-based deduplication

---

## Feature Engineering
The transformation layer enriches raw OHLCV data with:

- **Technical Indicators**
  - Simple Moving Average (SMA)
  - Exponential Moving Average (EMA)
  - Triangular Moving Average (TMA)
- **Price Features**
  - Typical Price
  - Weighted Price
- **Temporal Features**
  - Weekday
  - ISO week
  - Month and year

These features enable immediate use in predictive models such as **LSTM, ARIMA, and Transformer-based architectures**.

---

## Prerequisites
- Python 3.9+
- pandas
- numpy
- sqlalchemy
- MySQL 8.0
- matplotlib / seaborn
- Binance API credentials

---

## Usage

### 1️⃣ Configure Environment
Set up database credentials and Binance API keys.

### 2️⃣ Run ETL Pipeline
```bash
python etl_pipeline.py
````

### 3️⃣ Inspect Stored Data

```sql
SELECT * FROM worldcoin_market_data LIMIT 10;
```

### 4️⃣ Analytics & Modeling

Use the transformed dataset directly for:

* Volatility analysis
* Liquidity studies
* Time-series forecasting (LSTM, ARIMA, Transformers)

---

## 📂 Directory Structure

```text
ETL-Pipeline/
├── extract.py        # Data extraction from Binance API
├── transform.py      # Data transformation and feature engineering
├── load.py           # Database and table creation, and data loading
├── main.py           # ETL orchestration script
├── .env              # Environment variables (MySQL credentials)
├── requirements.txt  # Python dependencies


---

## Key Findings

* Mondays show peak trading activity; Sundays exhibit minimal volume
* Liquidity shocks precede high volatility intervals
* SMA, EMA, and TMA indicators capture complementary market dynamics
* Proposed pipeline balances **latency, modularity, and reproducibility** better than CSV and Kafka-based systems

---

## Future Work

* Multi-asset cryptocurrency ingestion
* Web-based real-time analytics dashboard
* Integration with LSTM, ARIMA, and Transformer models
* Risk metrics and anomaly detection
* Privacy-preserving and ethics-aware analytics

---

## Contributing

Contributions are welcome!
Please open an issue or submit a pull request for enhancements, optimizations, or extensions.

---


