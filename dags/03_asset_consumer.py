"""
## Sales Analytics (Asset-Triggered)

This DAG runs **automatically** whenever the `sales_etl_pipeline` DAG
completes and updates the `sales_pipeline_output` Asset.

It reads the processed sales data and generates analytics summaries
with detailed logging.

**Airflow features demonstrated:**
- Asset-triggered scheduling (producer → consumer pattern)
- Visible in the Assets tab of the Airflow UI
- Detailed analytics logging
- No manual schedule — purely event-driven
"""

import logging
import os
from pathlib import Path

import pandas as pd
from airflow.sdk import Asset, dag, task
from pendulum import datetime

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")) / "include" / "data"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = DATA_DIR / "reports"

SALES_ASSET = Asset("sales_pipeline_output")


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[SALES_ASSET],
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "data-engineering", "retries": 1},
    tags=["analytics", "asset-triggered", "demo"],
)
def sales_analytics_consumer():

    @task()
    def load_processed_data() -> dict:
        """Load the processed sales data produced by the ETL pipeline."""
        filepath = PROCESSED_DIR / "processed_sales.csv"
        log.info("──── LOAD: Processed Sales Data ────")
        log.info("Reading from: %s", filepath)

        if not filepath.exists():
            log.error("Processed file not found at %s", filepath)
            raise FileNotFoundError(f"Expected file missing: {filepath}")

        df = pd.read_csv(filepath)
        log.info("Loaded %d rows x %d columns", len(df), len(df.columns))
        log.info("Columns: %s", list(df.columns))

        return {"data": df.to_dict(orient="records")}

    @task()
    def revenue_by_customer(data: dict) -> dict:
        """Aggregate revenue and margin by customer."""
        log.info("──── ANALYTICS: Revenue by Customer ────")

        df = pd.DataFrame(data["data"])
        summary = (
            df.groupby(["customer_name", "customer_tier", "city"])
            .agg(
                total_orders=("order_id", "count"),
                total_revenue=("revenue", "sum"),
                total_margin=("margin", "sum"),
                avg_order_value=("revenue", "mean"),
            )
            .round(2)
            .sort_values("total_revenue", ascending=False)
            .reset_index()
        )

        log.info("=" * 60)
        log.info("  REVENUE BY CUSTOMER")
        log.info("=" * 60)
        for _, row in summary.iterrows():
            log.info(
                "  %-20s | %-6s | Orders: %2d | Revenue: %8.2f | Margin: %8.2f",
                row["customer_name"],
                row["customer_tier"],
                row["total_orders"],
                row["total_revenue"],
                row["total_margin"],
            )
        log.info("-" * 60)
        log.info(
            "  TOTAL %36s | Revenue: %8.2f | Margin: %8.2f",
            "",
            summary["total_revenue"].sum(),
            summary["total_margin"].sum(),
        )
        log.info("=" * 60)

        # Tier-level summary
        tier_summary = df.groupby("customer_tier")["revenue"].sum().to_dict()
        log.info("Revenue by tier: %s", tier_summary)

        return {"data": summary.to_dict(orient="records")}

    @task()
    def revenue_by_product(data: dict) -> dict:
        """Aggregate revenue and margin by product."""
        log.info("──── ANALYTICS: Revenue by Product ────")

        df = pd.DataFrame(data["data"])
        summary = (
            df.groupby(["product_name", "category"])
            .agg(
                units_sold=("quantity", "sum"),
                total_revenue=("revenue", "sum"),
                total_margin=("margin", "sum"),
                avg_margin_pct=("margin_pct", "mean"),
            )
            .round(2)
            .sort_values("total_revenue", ascending=False)
            .reset_index()
        )

        log.info("=" * 60)
        log.info("  REVENUE BY PRODUCT")
        log.info("=" * 60)
        for _, row in summary.iterrows():
            log.info(
                "  %-30s | %-12s | Units: %3d | Revenue: %8.2f | Margin: %.1f%%",
                row["product_name"],
                row["category"],
                row["units_sold"],
                row["total_revenue"],
                row["avg_margin_pct"],
            )
        log.info("=" * 60)

        # Flag products with margin below threshold
        low_margin = summary[summary["avg_margin_pct"] < 55]
        if not low_margin.empty:
            log.warning("Products with avg margin below 55%%:")
            for _, row in low_margin.iterrows():
                log.warning("  %s — %.1f%% margin", row["product_name"], row["avg_margin_pct"])

        return {"data": summary.to_dict(orient="records")}

    @task()
    def generate_report(
        data: list, customer_summary: dict, product_summary: dict
    ) -> str:
        """Generate a final summary report file."""
        log.info("──── REPORT: Generating Summary ────")

        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(data["data"])

        from pendulum import now
        timestamp = now().format("YYYYMMDD_HHmmss")
        report_path = REPORTS_DIR / f"sales_report_{timestamp}.txt"

        lines = [
            "=" * 50,
            "  SALES ANALYTICS REPORT",
            f"  Generated: {now().to_datetime_string()}",
            "=" * 50,
            "",
            f"  Total orders:     {len(df)}",
            f"  Total revenue:    {df['revenue'].sum():.2f}",
            f"  Total margin:     {df['margin'].sum():.2f}",
            f"  Avg order value:  {df['revenue'].mean():.2f}",
            f"  Unique customers: {df['customer_name'].nunique()}",
            f"  Unique products:  {df['product_name'].nunique()}",
            "",
            f"  Date range: {df['order_date'].min()} to {df['order_date'].max()}",
            "",
            "  Top 3 customers by revenue:",
        ]

        top_cust = df.groupby("customer_name")["revenue"].sum().nlargest(3)
        for name, rev in top_cust.items():
            lines.append(f"    - {name}: {rev:.2f}")

        lines.append("")
        lines.append("  Top 3 products by revenue:")
        top_prod = df.groupby("product_name")["revenue"].sum().nlargest(3)
        for name, rev in top_prod.items():
            lines.append(f"    - {name}: {rev:.2f}")

        lines.append("")
        lines.append("=" * 50)

        report_text = "\n".join(lines)
        report_path.write_text(report_text)

        log.info("Report written to: %s", report_path)
        for line in lines:
            log.info(line)

        return str(report_path)

    # ── WIRE DEPENDENCIES ────────────────────────────────────────────────

    data = load_processed_data()
    cust = revenue_by_customer(data)
    prod = revenue_by_product(data)
    generate_report(data, cust, prod)


sales_analytics_consumer()
