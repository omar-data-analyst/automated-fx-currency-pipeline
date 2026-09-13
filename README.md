# automated-fx-currency-pipeline
An automated, production-ready ETL pipeline built with Python and Prefect. It extracts daily FX rates via API, cleans and validates data quality with Pandas (null &amp; range checks), and incrementally loads clean records into Google BigQuery. Features robust error handling, Telegram logging alerts, and local Windows Task Scheduler automation.
