"""
Land Kaggle datasets to local CSV files as raw artifacts for this portfolio repo.
Yes we can load this directly to duckDB, but this somewhat mimics real-world landing into S3 etc. first
Outputs:
    - data/raw/olist/csv/*.csv
    - data/raw/marketing_funnel_olist/csv/*.csv

Next steps:
    - Run scripts/load_olist_csvs_to_duckdb.py to create raw_olist.* DuckDB tables
      from the landed Olist CSVs.
"""

import kagglehub
from pathlib import Path
import shutil

# Resolve the project root relative to this script's location.
project_root = Path(__file__).parent.parent

# -- Dataset 1: Olist Brazilian E-Commerce ----------------------------------

olist_dataset_ref = "olistbr/brazilian-ecommerce"
olist_raw_csv_dir = project_root / "data" / "raw" / "olist" / "csv"
olist_raw_csv_dir.mkdir(parents=True, exist_ok=True)

olist_csv_filenames = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

print(f"Dataset: {olist_dataset_ref}")
for csv_filename in olist_csv_filenames:
    print(f"\nFetching {csv_filename} from Kaggle...")
    cached_csv_path = Path(kagglehub.dataset_download(olist_dataset_ref, path=csv_filename))
    landed_csv_path = olist_raw_csv_dir / csv_filename
    shutil.copyfile(cached_csv_path, landed_csv_path)
    print(f"  Landed raw CSV: {landed_csv_path}")

# -- Dataset 2: Olist marketing funnel --------------------------------------

marketing_funnel_dataset_ref = "olistbr/marketing-funnel-olist"
marketing_funnel_raw_csv_dir = project_root / "data" / "raw" / "marketing_funnel_olist" / "csv"
marketing_funnel_raw_csv_dir.mkdir(parents=True, exist_ok=True)

marketing_funnel_csv_filenames = [
    "olist_closed_deals_dataset.csv",
    "olist_marketing_qualified_leads_dataset.csv",
]

print(f"\nDataset: {marketing_funnel_dataset_ref}")
for csv_filename in marketing_funnel_csv_filenames:
    print(f"\nFetching {csv_filename} from Kaggle...")
    cached_csv_path = Path(kagglehub.dataset_download(marketing_funnel_dataset_ref, path=csv_filename))
    landed_csv_path = marketing_funnel_raw_csv_dir / csv_filename
    shutil.copyfile(cached_csv_path, landed_csv_path)
    print(f"  Landed raw CSV: {landed_csv_path}")

# -- Summary ----------------------------------------------------------------

print("\nDone.")
print(f"  Olist CSVs: {olist_raw_csv_dir}")
print(f"  Marketing funnel CSVs: {marketing_funnel_raw_csv_dir}")
print("Next: run scripts/load_olist_csvs_to_duckdb.py to load Olist into DuckDB (raw_olist.*).")
