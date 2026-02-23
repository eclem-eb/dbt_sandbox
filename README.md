# dbt_sandbox
A space to practice DBT and other fun data stuff

## Goal

This repo is a personal data porftolio sandbox, focusing on some standard analytics and analaytics engineering examples, and possibly some lightweight data engineering. It will utilize public or generated datasets, duckDB, DBT, python, R, and dagster.

In this first version we will load Olist transactional and marketing data as an all purpose data set of orders and leads. This will be a good practice data set for some basic DBT techinques.

## Setup

### 1) Create a Python virtual environment

```bash
cd dbt_sandbox
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2) Kaggle authentication (required)

The scripts in `scripts/` use `kagglehub`, which will authenticate automatically if you have **either**:
- `~/.kaggle/kaggle.json` (if you already have this, you're good)
- `KAGGLE_API_TOKEN` exported in your shell session

If you need a token, generate one on Kaggle.com via **Your Profile -> Settings -> API Tokens**.

```bash
export KAGGLE_API_TOKEN="YOUR_TOKEN_HERE"
```

This will allow us to pull the Olist data from Kaggle. Next we will Extract and Load to try and mimic a real world landing of our data up stream of DBT.

### 3) Land raw CSV artifacts from Kaggle

This downloads the Kaggle datasets and land them into raw CSV artifacts `data/source/...`.

```bash
python scripts/extract/extract_kaggle_to_csv_olist.py
```

### 4) Load source CSVs into DuckDB (creates `source_*` schemas)

Setup DuckDB and load the source tables.

```bash
python scripts/load/load_csv_to_duckdb_olist.py
```

### 5) Run DBT Models

As of now only base models are built. You can run them to query them in DuckDB to begin exploring mart model ideas.

```bash
dbt run --select path:models/base  
```

## Notes

- `data/source/**` is intentionally gitignored as to not commit Kaggle datasets.