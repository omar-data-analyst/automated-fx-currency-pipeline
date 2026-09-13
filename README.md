# 🚀 Automated Currency Exchange Rate ETL Pipeline

<p align="center">
  <img width="1376" height="1075" alt="seo-hero-etl-pipeline_ag7zd4" src="https://github.com/user-attachments/assets/a008d5fb-dd70-4e27-9506-bf5cdf693759" />

</p>

An end-to-end, production-grade **ETL (Extract, Transform, Load)** pipeline built with Python, Prefect, Pandas, Google BigQuery, and Telegram API. 

The pipeline automatically extracts daily exchange rates, performs data quality gatekeeping, loads sanitized data into Google BigQuery, and alerts engineers via Telegram.

---

## 🏗️ Architecture & Workflow

[ Public Currency API ] ──(Extract)──> [ Prefect Ingestion Task ]
                                                   │
                                             (Transform)
                                                   ▼
                                      [ Pandas Data Quality Check ]
                                                   │
                                                (Load)
                                                   ▼
                                       [ BigQuery Data Warehouse ]
                                                   │
                                                (Alert)
                                                   ▼
                                      [ Telegram Notification Bot ]

---

## 🐍 Python ETL Implementation

The core ingestion engine is managed via modular Prefect flows and tasks ensuring resilience, quality validation, and automatic retries.

### 1. Data Extraction & Fetching
<p align="center">
  <img width="1292" height="570" alt="ETL Python Code 1" src="https://github.com/user-attachments/assets/af392fe3-bfcf-4b0d-94c5-ecf633827098" />


</p>

### 2. Transformation & Quality Validation
<p align="center">
  <img width="1303" height="577" alt="ETL Python Code 2" src="https://github.com/user-attachments/assets/61b919e2-a1f7-411a-b60a-cf03806061ae" />

</p>

### 3. BigQuery Loader & Notification Engine
<p align="center">
  <img width="1181" height="575" alt="python code 4" src="https://github.com/user-attachments/assets/432d5314-c4cc-452d-a8ff-8a82c882efca" />


</p>

---

## 📊 Google BigQuery Target Warehouse

Data is incrementally loaded (WRITE_APPEND) into Google BigQuery for analytical querying and downstream analytics.

<p align="center">
  <img width="1007" height="527" alt="Trasformed Data in PigQuery" src="https://github.com/user-attachments/assets/898fab56-3bbc-4bfa-bd6b-ebb2e82817a7" />

</p>

### Public Live Table Reference
- **Table ID:** project-1-508215.currency_db.daily_rates
- **Console Access:** https://console.cloud.google.com/bigquery?project=project-1-508215&p=project-1-508215&d=currency_db&t=daily_rates&page=table

---

## 📈 Analytical SQL Queries

Below are complex analytics queries executed directly on the data warehouse.

### 1. 7-Day Moving Average & Trend Analysis
<p align="center">
  <img width="1299" height="552" alt="SQL Code" src="https://github.com/user-attachments/assets/2af46553-3cca-4b2b-b6c1-bdad1caf6505" />

</p>

### 2. Volatility Scoring & Anomaly Detection
<p align="center">
  <img width="895" height="373" alt="SQL Code 3" src="https://github.com/user-attachments/assets/0f2948f9-cf2b-41ca-805b-3de62bec47da" />

</p>


### 3. Extreme Market Anomaly Detection (Spikes & Drops)
<p align="center">
  <img width="965" height="488" alt="SQL Code 2" src="https://github.com/user-attachments/assets/e011d0ab-c6d8-4a85-a93a-c7aa429adb9e" />


</p>


---

## 🛠️ Tech Stack

- **Orchestration:** Prefect 3.x
- **Data Engine:** Pandas, Python 3.10+
- **Cloud Warehouse:** Google BigQuery
- **Alerting:** Telegram API
- **Version Control:** Git, GitHub

---

## ⚙️ How to Run Locally

1. **Clone Repository:**
   git clone https://github.com/your-username/Currency-ETL-Pipeline.git

2. **Install Dependencies:**
   pip install -r requirements.txt

3. **Environment Setup:**
   Ensure your GCP Service Account JSON key is placed safely outside version control and configure your Telegram credentials.

4. **Execute Pipeline:**
   python "Pipline Code.py"

---

## 🛡️ Security
All sensitive API credentials, Service Account JSON keys, and tokens are protected via .gitignore.
