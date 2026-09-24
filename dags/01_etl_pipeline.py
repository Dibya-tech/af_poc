"""
## Sales ETL Pipeline

Extracts raw CSV data (orders, customers, products), transforms it by
joining and enriching with calculated fields (revenue, margin, customer tier),
and loads the final dataset to a processed output file.

**Airflow features demonstrated:**
- TaskFlow API with `@task` decorators
- TaskGroups for organized Graph view in UI
- XCom for passing data and metrics between tasks
- Asset outlets to trigger downstream DAGs
- Rich structured logging at every step
"""

import logging
import os
from pathlib import Path

import pandas as pd
from airflow.sdk import Asset, dag, task
from airflow.sdk.definitions.taskgroup import TaskGroup
from pendulum import datetime

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "data"
OUTPUT_DIR = DATA_DIR / "processed"

SALES_ASSET = Asset("sales_pipeline_output")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["etl", "sales", "demo"],
)
def sales_etl_pipeline():

    # ── EXTRACT ──────────────────────────────────────────────────────────

    @task()
    def extract_orders() -> dict:
        """Read raw orders CSV and return as dict for XCom."""
        filepath = DATA_DIR / "raw_orders.csv"
        log.info("──── EXTRACT: Orders ────")
        log.info("Reading from: %s", filepath)

        df = pd.read_csv(filepath)
        row_count = len(df)
        log.info("Loaded %d order rows", row_count)
        log.info("Columns: %s", list(df.columns))
        log.info("Date range: %s to %s", df["order_date"].min(), df["order_date"].max())
        log.info("Status breakdown: %s", df["status"].value_counts().to_dict())
#          ORD031,,,1,,2024-02-14,pending
        if df.isnull().any().any():
            null_counts = df.isnull().sum()
            log.warning("Null values detected: %s", null_counts[null_counts > 0].to_dict())
        else:
            log.info("Data quality check passed — no null values")

        return {"data": df.to_dict(orient="records"), "row_count": row_count}

    @task()
    def extract_customers() -> dict:
        """Read raw customers CSV."""
        filepath = DATA_DIR / "raw_customers.csv"
        log.info("──── EXTRACT: Customers ────")
        log.info("Reading from: %s", filepath)

        df = pd.read_csv(filepath)
        row_count = len(df)
        log.info("Loaded %d customer rows", row_count)
        log.info("Tier distribution: %s", df["tier"].value_counts().to_dict())
        log.info("Cities represented: %s", sorted(df["city"].unique().tolist()))

        return {"data": df.to_dict(orient="records"), "row_count": row_count}

    @task()
    def extract_products() -> dict:
        """Read raw products CSV."""
        filepath = DATA_DIR / "raw_products.csv"
        log.info("──── EXTRACT: Products ────")
        log.info("Reading from: %s", filepath)

        df = pd.read_csv(filepath)
        row_count = len(df)
        log.info("Loaded %d product rows", row_count)
        log.info("Categories: %s", df["category"].value_counts().to_dict())
        log.info(
            "Price range: %.2f - %.2f",
            df["list_price"].min(),
            df["list_price"].max(),
        )

        return {"data": df.to_dict(orient="records"), "row_count": row_count}

    @task()
    def join_datasets(
        orders_raw: dict, customers_raw: dict, products_raw: dict
    ) -> dict:
        """Join orders with customers and products."""
        log.info("──── TRANSFORM: Join Datasets ────")

        orders = pd.DataFrame(orders_raw["data"])
        customers = pd.DataFrame(customers_raw["data"])
        products = pd.DataFrame(products_raw["data"])

        log.info(
            "Input sizes — orders: %d, customers: %d, products: %d",
            len(orders), len(customers), len(products),
        )

        merged = orders.merge(customers, on="customer_id", how="left", suffixes=("", "_cust"))
        unmatched = merged["name"].isnull().sum()
        if unmatched:
            log.warning("%d orders have no matching customer", unmatched)
        else:
            log.info("All orders matched to customers successfully")

        merged = merged.merge(products, on="product_id", how="left", suffixes=("", "_prod"))
        unmatched = merged["name_prod"].isnull().sum()
        if unmatched:
            log.warning("%d orders have no matching product", unmatched)
        else:
            log.info("All orders matched to products successfully")

        log.info("Joined dataset: %d rows x %d columns", len(merged), len(merged.columns))

        return {"data": merged.to_dict(orient="records"), "row_count": len(merged)}

    @task()
    def enrich_data(joined: dict) -> dict:
        """Add calculated fields: revenue, margin, margin_pct."""
        log.info("──── TRANSFORM: Enrich Data ────")

        df = pd.DataFrame(joined["data"])

        df["revenue"] = df["quantity"] * df["unit_price"]
        df["cost"] = df["quantity"] * df["cost_price"]
        df["margin"] = df["revenue"] - df["cost"]
        df["margin_pct"] = ((df["margin"] / df["revenue"]) * 100).round(2)

        log.info("Added calculated columns: revenue, cost, margin, margin_pct")
        log.info("Total revenue: %.2f", df["revenue"].sum())
        log.info("Total margin: %.2f", df["margin"].sum())
        log.info("Average margin %%: %.1f%%", df["margin_pct"].mean())

        low_margin = df[df["margin_pct"] < 50]
        if not low_margin.empty:
            log.warning(
                "%d orders with margin below 50%% — review pricing",
                len(low_margin),
            )

        return {"data": df.to_dict(orient="records"), "row_count": len(df)}

    @task()
    def filter_completed(enriched: dict) -> dict:
        """Keep only completed orders for the final output."""
        log.info("──── TRANSFORM: Filter Completed Orders ────")

        df = pd.DataFrame(enriched["data"])
        before_count = len(df)

        completed = df[df["status"] == "completed"]
        filtered_out = before_count - len(completed)

        log.info("Before filter: %d rows", before_count)
        log.info("After filter (completed only): %d rows", len(completed))
        log.info(
            "Filtered out: %d rows (pending: %d, cancelled: %d)",
            filtered_out,
            len(df[df["status"] == "pending"]),
            len(df[df["status"] == "cancelled"]),
        )

        return {"data": completed.to_dict(orient="records"), "row_count": len(completed)}

    @task(outlets=[SALES_ASSET])
    def load_to_csv(filtered: dict) -> str:
        """Write processed data to CSV output file."""
        log.info("──── LOAD: Write Output ────")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / "processed_sales.csv"

        df = pd.DataFrame(filtered["data"])
        output_cols = [
            "order_id", "order_date", "name", "city", "tier",
            "name_prod", "category", "quantity", "unit_price",
            "revenue", "cost", "margin", "margin_pct",
        ]
        df = df[output_cols]
        df.columns = [
            "order_id", "order_date", "customer_name", "city", "customer_tier",
            "product_name", "category", "quantity", "unit_price",
            "revenue", "cost", "margin", "margin_pct",
        ]
        df.to_csv(output_path, index=False)

        log.info("Output written to: %s", output_path)
        log.info("Final row count: %d", len(df))

        log.info("=" * 50)
        log.info("  ETL PIPELINE SUMMARY")
        log.info("=" * 50)
        log.info("  Total orders processed: %d", len(df))
        log.info("  Unique customers: %d", df["customer_name"].nunique())
        log.info("  Unique products: %d", df["product_name"].nunique())
        log.info("  Total revenue: %.2f", df["revenue"].sum())
        log.info("  Total margin: %.2f", df["margin"].sum())
        log.info("  Top customer: %s", df.groupby("customer_name")["revenue"].sum().idxmax())
        log.info("  Top product: %s", df.groupby("product_name")["revenue"].sum().idxmax())
        log.info("=" * 50)

        return str(output_path)

    # ── WIRE DEPENDENCIES WITH TASK GROUPS ───────────────────────────────

    with TaskGroup("extract", tooltip="Read raw CSV source files"):
        orders = extract_orders()
        customers = extract_customers()
        products = extract_products()

    with TaskGroup("transform", tooltip="Join, enrich, and filter data"):
        joined = join_datasets(orders, customers, products)
        enriched = enrich_data(joined)
        filtered = filter_completed(enriched)

    with TaskGroup("load", tooltip="Write final output"):
        load_to_csv(filtered)


sales_etl_pipeline()
