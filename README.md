# Flight Delay Prediction & Data Pipeline - README

## Overview
This project demonstrates a simple flight delay prediction pipeline:
- ingest (OpenSky or synthetic)
- transform & feature engineering
- load to Snowflake (optional)
- train RandomForest model
- serve predictions via Flask API

## Project structure
flight-delay-project/
├─ airflow/
├─ data/
├─ src/
│ ├─ ingest.py
│ ├─ transform.py
│ ├─ load_to_snowflake.py
│ ├─ train_model.py
│ ├─ predict_api.py
│ └─ utils.py
├─ models/
│ └─ rf_delay_model.joblib # generated after training
├─ requirements.txt
└─ README.md