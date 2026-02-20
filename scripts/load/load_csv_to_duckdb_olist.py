"""
Load source CSV artifacts into DuckDB as source_* schemas.
Creates source_olist_ecom.* and source_olist_marketing.* tables in dev.duckdb
Mimics a Load step in ELT 
"""

import duckdb
from pathlib import Path
import sys

# -- Configuration ----------------------------------------------------------

db_path = Path("dev.duckdb")

olist_source_csv_dir = Path("data/source/olist_ecom")
marketing_funnel_source_csv_dir = Path("data/source/olist_marketing")

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

# -- Load source tables -----------------------------------------------------

print(f"Target DuckDB file: {db_path}")

if not olist_source_csv_dir.exists():
    print(f"ERROR: Olist source CSV directory not found: {olist_source_csv_dir}")
    print("Run scripts/extract/extract_kaggle_to_csv_olist.py first to extract the Kaggle CSVs.")
    sys.exit(1)

if not marketing_funnel_source_csv_dir.exists():
    print(f"ERROR: Marketing funnel source CSV directory not found: {marketing_funnel_source_csv_dir}")
    print("Run scripts/extract/extract_kaggle_to_csv_olist.py first to extract the Kaggle CSVs.")
    sys.exit(1)

conn = duckdb.connect(str(db_path))

# Keep source tables in their own schemas (separate from dbt-managed schemas)
conn.execute("create schema if not exists source_olist_ecom")
conn.execute("create schema if not exists source_olist_marketing")

print(f"\nLoading Olist source tables from: {olist_source_csv_dir}")
for csv_filename, table_name in olist_files_to_load:
    landed_csv_path = olist_source_csv_dir / csv_filename
    if not landed_csv_path.exists():
        print(f"ERROR: Expected Olist source CSV not found: {landed_csv_path}")
        print("Run scripts/extract/extract_kaggle_to_csv_olist.py to extract the source artifacts.")
        sys.exit(1)

    full_table_name = f"source_olist_ecom.{table_name}"
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
    print(f"Loaded {full_table_name}: {row_count:,} rows")

print(f"\nLoading marketing funnel source tables from: {marketing_funnel_source_csv_dir}")
for csv_filename, table_name in marketing_funnel_files_to_load:
    landed_csv_path = marketing_funnel_source_csv_dir / csv_filename
    if not landed_csv_path.exists():
        print(f"ERROR: Expected marketing funnel source CSV not found: {landed_csv_path}")
        print("Re-run scripts/extract/extract_kaggle_to_csv_olist.py to extract the source artifacts.")
        sys.exit(1)

    full_table_name = f"source_olist_marketing.{table_name}"
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
