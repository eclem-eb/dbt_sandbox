"""
Load landed raw CSV artifacts into DuckDB as raw_* schemas.

Input:
    Produced by scripts/load_raw_data.py:
      - data/raw/olist/csv/*.csv
      - data/raw/marketing_funnel_olist/csv/*.csv

Output:
    dev.duckdb with tables created in:
      - raw_olist.* (e-commerce transactional dataset)
      - raw_marketing_funnel_olist.* (marketing funnel dataset)

Assumptions:
    - This is a raw load step: we do not rename columns or cast types here
      (dbt staging models handle that)
    - We drop and recreate tables each run so it is safe to rerun locally
"""

import duckdb
from pathlib import Path
import sys

# -- Configuration ----------------------------------------------------------

project_root = Path(__file__).parent.parent
db_path = project_root / "dev.duckdb"

olist_raw_csv_dir = project_root / "data" / "raw" / "olist" / "csv"
marketing_funnel_raw_csv_dir = project_root / "data" / "raw" / "marketing_funnel_olist" / "csv"

olist_files_to_load = [
    ("olist_orders_dataset.csv", "orders"),
    ("olist_order_items_dataset.csv", "order_items"),
    ("olist_order_payments_dataset.csv", "order_payments"),
    ("olist_order_reviews_dataset.csv", "order_reviews"),
    ("olist_products_dataset.csv", "products"),
    ("olist_sellers_dataset.csv", "sellers"),
    ("olist_customers_dataset.csv", "customers"),
    ("olist_geolocation_dataset.csv", "geolocation"),
    ("product_category_name_translation.csv", "product_category_name_translation"),
]

marketing_funnel_files_to_load = [
    ("olist_closed_deals_dataset.csv", "closed_deals"),
    ("olist_marketing_qualified_leads_dataset.csv", "marketing_qualified_leads"),
]

# -- Load raw tables --------------------------------------------------------

print(f"Target DuckDB file: {db_path}")

if not olist_raw_csv_dir.exists():
    print(f"ERROR: Olist raw CSV directory not found: {olist_raw_csv_dir}")
    print("Run scripts/load_raw_data.py first to land the Kaggle CSVs.")
    sys.exit(1)

if not marketing_funnel_raw_csv_dir.exists():
    print(f"ERROR: Marketing funnel raw CSV directory not found: {marketing_funnel_raw_csv_dir}")
    print("Run scripts/load_raw_data.py first to land the Kaggle CSVs.")
    sys.exit(1)

print("\nConnecting to DuckDB...")
conn = duckdb.connect(str(db_path))

# Keep raw tables in their own schemas (separate from dbt-managed schemas)
conn.execute("create schema if not exists raw_olist")
conn.execute("create schema if not exists raw_marketing_funnel_olist")

print(f"\nLoading Olist raw tables from: {olist_raw_csv_dir}")
for csv_filename, table_name in olist_files_to_load:
    landed_csv_path = olist_raw_csv_dir / csv_filename
    if not landed_csv_path.exists():
        print(f"ERROR: Expected Olist raw CSV not found: {landed_csv_path}")
        print("Re-run scripts/load_raw_data.py to re-land the raw artifacts.")
        sys.exit(1)

    full_table_name = f"raw_olist.{table_name}"
    print(f"\nCreating table {full_table_name} from {landed_csv_path}...")

    conn.execute(f"drop table if exists {full_table_name}")
    conn.execute(
        f"""
        create table {full_table_name} as
        select *
        from read_csv_auto('{landed_csv_path.as_posix()}', header=true)
        """
    )

    row_count = conn.execute(f"select count(*) from {full_table_name}").fetchone()[0]
    print(f"  Loaded {full_table_name}: {row_count:,} rows")

print(f"\nLoading marketing funnel raw tables from: {marketing_funnel_raw_csv_dir}")
for csv_filename, table_name in marketing_funnel_files_to_load:
    landed_csv_path = marketing_funnel_raw_csv_dir / csv_filename
    if not landed_csv_path.exists():
        print(f"ERROR: Expected marketing funnel raw CSV not found: {landed_csv_path}")
        print("Re-run scripts/load_raw_data.py to re-land the raw artifacts.")
        sys.exit(1)

    full_table_name = f"raw_marketing_funnel_olist.{table_name}"
    print(f"\nCreating table {full_table_name} from {landed_csv_path}...")

    conn.execute(f"drop table if exists {full_table_name}")
    conn.execute(
        f"""
        create table {full_table_name} as
        select *
        from read_csv_auto('{landed_csv_path.as_posix()}', header=true)
        """
    )

    row_count = conn.execute(f"select count(*) from {full_table_name}").fetchone()[0]
    print(f"  Loaded {full_table_name}: {row_count:,} rows")

print("\nDone.")
conn.close()
