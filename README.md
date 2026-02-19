# dbt_sandbox
A space to practice DBT and other fun data stuff

## Goal

This repo is a personal data porftolio sandbox, focusing on some standard analytics and analaytics engineering examples, and possibly some lightweight data engineering.

It will utilize public or generated datasets, duckDB, DBT, python, R, and dagster.

## Setup

### 1) Create a Python virtual environment

```bash
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

### 3) Land raw CSV artifacts from Kaggle

This downloads the Kaggle datasets and land them into raw CSV artifacts `data/raw/...`.

```bash
python scripts/load_raw_data.py
```

### 4) Load raw CSVs into DuckDB (creates `raw_*` schemas)

This creates a local DuckDB file `dev.duckdb` and loads the raw tables.

```bash
python scripts/load_olist_csvs_to_duckdb.py
```

## Notes

- `data/raw/**` is intentionally gitignored as to not commit raw Kaggle datasets.
- If you rotate/regenerate your Kaggle API key, update `~/.kaggle/kaggle.json` (or your env var) accordingly.
