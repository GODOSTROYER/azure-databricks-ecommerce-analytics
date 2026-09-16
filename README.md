# 🛒 E-Commerce Analytics Platform

> Enterprise-grade data engineering solution built on Azure Databricks with Unity Catalog

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Azure](https://img.shields.io/badge/Azure-0089D6?style=for-the-badge&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)

## 📋 Overview

This platform processes e-commerce customer behavior data (~110M events) through a **Medallion Architecture** (Bronze → Silver → Gold) using:

- **Unity Catalog** for data governance
- **Delta Live Tables** for declarative pipelines
- **Databricks Asset Bundles** for CI/CD deployment
- **Structured Streaming** with Auto Loader for incremental ingestion

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Unity Catalog                            │
├─────────────────────────────────────────────────────────────────┤
│  ecommerce_analytics_{env}/                                     │
│  ├── bronze_layer/                                              │
│  │   ├── events_raw (Delta Table)                               │
│  │   ├── events_raw_streaming (Auto Loader target)              │
│  │   ├── raw_data (Volume - CSVs)                               │
│  │   └── _checkpoints (Volume - stream checkpoints)             │
│  ├── silver_layer/                                              │
│  │   └── events_cleaned                                         │
│  ├── gold_layer/                                                │
│  │   ├── customer_metrics                                       │
│  │   ├── product_performance                                    │
│  │   ├── daily_sales_summary                                    │
│  │   └── conversion_funnel                                      │
│  └── dlt_silver_layer/        (DLT pipeline output)             │
│      ├── events_raw / events_cleaned / events_quarantine        │
│      ├── customer_metrics / product_performance                 │
│      ├── daily_sales_summary / conversion_funnel                │
│      └── data_quality_metrics                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Two ways through the medallion

The same Bronze → Silver → Gold logic is implemented twice, so the platform can be
run either as classic notebook jobs or as a declarative DLT pipeline:

```
Batch path — src/bronze, src/silver, src/gold (run by Jobs)

  raw_data ──► ingest_events_csv ──► transform_events_cleaned ──► agg_customer_metrics
  (Kaggle      explicit schema,      dedupe on event_id,          agg_product_performance
   CSVs)       audit columns,        quality filters,             agg_daily_sales
               MERGE on composite    derived columns              agg_conversion_funnel
               key                                                  └─► gold_layer.*

DLT path — src/dlt/dlt_bronze_to_gold.py (one declarative graph)

  raw_data ──► events_raw ──► events_cleaned ──────► customer_metrics
  (Auto        @dlt.table     @dlt.expect_all_or_drop  product_performance
   Loader)                      └─► events_quarantine  daily_sales_summary
                                    (dead letter queue) conversion_funnel
                                                        data_quality_metrics

Streaming ingestion — src/streaming/stream_events_autoloader.py

  raw_data ──► cloudFiles stream ──► events_raw_streaming
               checkpointed, schema evolution (addNewColumns),
               trigger availableNow or processingTime
```

Bronze is partitioned by `event_type`, Silver by `event_date`. Silver adds an
`event_id` (SHA-256 of the composite key) that the batch MERGE uses as its
idempotency key, plus date parts and a three-level category hierarchy
(`category_l1/l2/l3`) split out of `category_code`.

## 📁 Project Structure

```
azure-databricks-ecommerce-analytics/
├── databricks.yml              # DABs main configuration
├── README.md
├── Create Catalog &  Get Raw Data From Kaggle.ipynb   # one-time: catalog, volume, kagglehub download
├── environments/               # Reference configs per environment (not included by the bundle)
│   ├── dev.yml
│   ├── staging.yml
│   └── prod.yml
├── resources/                  # DABs resource definitions
│   ├── jobs.yml
│   ├── pipelines.yml
│   └── clusters.yml
├── src/
│   ├── bronze/                 # Bronze layer notebooks
│   │   ├── ingest_events_csv.py
│   │   └── schema_events.py
│   ├── silver/                 # Silver layer notebooks
│   │   ├── transform_events_cleaned.py
│   │   └── data_quality_rules.py
│   ├── gold/                   # Gold layer notebooks
│   │   ├── agg_customer_metrics.py
│   │   ├── agg_product_performance.py
│   │   ├── agg_daily_sales.py
│   │   └── agg_conversion_funnel.py
│   ├── dlt/                    # Delta Live Tables
│   │   └── dlt_bronze_to_gold.py
│   └── streaming/              # Streaming pipelines
│       └── stream_events_autoloader.py
├── setup/                      # Unity Catalog setup scripts
│   ├── quick_start_setup.py    # notebook: creates catalogs, schemas, volumes
│   ├── unity_catalog_setup.sql
│   ├── external_locations.sql  # ADLS Gen2 credentials/locations (templates)
│   ├── security_policies.sql
│   └── compute_policies.sql    # cluster policy definitions (reference JSON)
├── tests/                      # Unit tests
│   ├── test_bronze_ingestion.py
│   ├── test_silver_transformations.py
│   └── test_gold_aggregations.py
├── docs/                       # Documentation
│   ├── architecture.md
│   ├── medallion_design.md
│   ├── security_model.md
│   ├── performance_optimizations.md
│   ├── requirements_verification.md
│   └── demo_walkthrough.md
└── .github/
    └── workflows/
        └── deploy.yml          # CI/CD pipeline
```

## 🚀 Quick Start (Free Trial / Serverless)

> **✅ Serverless Compatible**: This project is configured to run on Databricks Free Trial with serverless compute.

### Prerequisites

- Azure Databricks **Free Trial** workspace
- Unity Catalog enabled (automatic on new workspaces)
- Databricks CLI v0.200+

### 1. Download Dataset

First, download the dataset from Kaggle and upload to your workspace:

1. Go to [Kaggle Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
2. Download CSV files (~14GB)
3. Create Unity Catalog volume and upload (or use included notebook)

The included notebook is [`Create Catalog &  Get Raw Data From Kaggle.ipynb`](Create%20Catalog%20%26%20%20Get%20Raw%20Data%20From%20Kaggle.ipynb)
at the repo root: it creates `ecommerce_analytics_dev`, the `bronze_layer` schema and
the `raw_data` volume, then pulls the dataset with `kagglehub` straight into the volume
(`kagglehub` needs Kaggle API credentials — it reads `KAGGLE_USERNAME` and `KAGGLE_KEY`).
Run [`setup/quick_start_setup.py`](setup/quick_start_setup.py) once as well if you want the
staging and prod catalogs created too.

### 2. Create Serverless SQL Warehouse

1. In Databricks UI, go to **SQL Warehouses**
2. Click **Create SQL Warehouse**
3. Select **Serverless** type
4. Name: `ecommerce-analytics-warehouse`
5. Copy the **Warehouse ID** for configuration

### 3. Configure CLI

```bash
# Install the Databricks CLI (v0.200+ ships the `bundle` command;
# the old `pip install databricks-cli` package does not)
# macOS/Linux:  brew tap databricks/tap && brew install databricks
# Windows:      winget install Databricks.DatabricksCLI
databricks --version

# Configure with your workspace
databricks configure --token
# Enter your workspace URL and Personal Access Token
```

### 4. Update Configuration

Three values in the repo point at the author's workspace and need to be yours:

```yaml
# databricks.yml — the host is set directly (DABs does not allow variables here)
workspace:
  host: "https://adb-xxxxx.azuredatabricks.net"

# databricks.yml — only needed if you wire a SQL Warehouse into your own queries
variables:
  warehouse_id:
    default: "your-warehouse-id-here"

# resources/pipelines.yml — all four DLT pipelines notify this address
notifications:
  - email_recipients:
      - your-email@company.com
```

Catalog names, schema names and the volume path come from the target you deploy
(`dev` / `staging` / `prod` in `databricks.yml`) and are passed into the job notebooks
as widget parameters, so the notebooks themselves stay environment-agnostic. The DLT
notebook is the one exception — see **Limitations** below.

### 5. Validate & Deploy

```bash
# Validate configuration
databricks bundle validate -t dev

# Deploy to workspace
databricks bundle deploy -t dev
```

### 6. Run Pipelines

```bash
# Option 1: Run batch ingestion job
databricks bundle run -t dev bronze_ingestion_job

# Option 2: Run full pipeline (Bronze → Silver → Gold)
databricks bundle run -t dev full_pipeline_job

# Option 3: Run DLT pipeline (serverless)
databricks bundle run -t dev ecommerce_dlt_pipeline
```

### 7. Interactive Notebooks (Alternative)

For Free Trial, you can also run notebooks directly:

1. Navigate to Workspace in Databricks UI
2. Open notebooks from deployed bundle
3. Attach to **Serverless** compute
4. Run cells interactively

## ⏱️ Orchestration

### Jobs — [`resources/jobs.yml`](resources/jobs.yml)

| Job                        | Tasks                                                                                          | Schedule (UTC)     |
| -------------------------- | ---------------------------------------------------------------------------------------------- | ------------------ |
| `bronze_ingestion_job`     | `ingest_csv_to_bronze`                                                                          | daily 01:00        |
| `silver_transformation_job`| `transform_events_to_silver`                                                                    | daily 01:30        |
| `gold_aggregation_job`     | `customer_metrics` → (`product_performance`, `daily_sales`) → `conversion_funnel`               | daily 03:00        |
| `full_pipeline_job`        | bronze → silver → three gold tasks in parallel → `gold_conversion_funnel`                       | daily 00:00        |
| `streaming_ingestion_job`  | `stream_events` (Auto Loader, `trigger_mode=availableNow`)                                      | on demand          |

Every schedule ships with `pause_status: PAUSED`, so nothing runs until you enable it
in the Workflows UI. Tasks carry their own timeouts and retries (Silver gets 2h and
2 retries, Gold 30–60m and 1 retry) and are tagged with `layer` and `environment`.

### DLT pipelines — [`resources/pipelines.yml`](resources/pipelines.yml)

All four are serverless, `development: true`, `continuous: false`, channel `CURRENT`,
and all load the same [`src/dlt/dlt_bronze_to_gold.py`](src/dlt/dlt_bronze_to_gold.py):

- `ecommerce_dlt_pipeline` — the whole graph, written to the `dlt_silver_layer` schema
- `bronze_ingestion_dlt_pipeline` / `silver_transformation_dlt_pipeline` / `gold_aggregation_dlt_pipeline` — the same notebook narrowed with `filters.include`, each writing into its own layer schema

## 📊 Dataset

**Source**: [Kaggle - eCommerce Behavior Data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

| Metric       | Value                                  |
| ------------ | -------------------------------------- |
| Total Events | ~110 million                           |
| Time Period  | October - November 2019                |
| File Size    | ~14 GB (CSV)                           |
| Event Types  | view, cart, remove_from_cart, purchase |

### Schema

| Column        | Type      | Description                         |
| ------------- | --------- | ----------------------------------- |
| event_time    | timestamp | Event timestamp (UTC)               |
| event_type    | string    | view/cart/remove_from_cart/purchase |
| product_id    | long      | Product identifier                  |
| category_id   | long      | Category identifier                 |
| category_code | string    | Category taxonomy (nullable)        |
| brand         | string    | Brand name (nullable)               |
| price         | double    | Product price                       |
| user_id       | long      | User identifier                     |
| user_session  | string    | Session identifier                  |

## 🔐 Security Features

- **Row Level Security (RLS)**: Data isolation by user segment
- **Column Level Security (CLS)**: PII protection for sensitive columns
- **Dynamic Data Masking**: Real-time masking based on user roles
- **Audit Logging**: Complete access trail via Unity Catalog

All of it lives in [`setup/security_policies.sql`](setup/security_policies.sql): a
`user_segment_access` mapping table feeding `check_segment_access()`, the secured views
(`customer_metrics_secured`, `product_performance_secured`), the PII-free and masked views
(`customer_metrics_no_pii`, `customer_metrics_masked`, `events_cleaned_masked`), the
`mask_user_id` / `mask_email` / `mask_price` / `mask_session` functions, sensitivity tags,
and `system.access.audit` queries. Grants per catalog are in
[`setup/unity_catalog_setup.sql`](setup/unity_catalog_setup.sql).

## 📈 Key Metrics (Gold Layer)

### Customer Metrics

- Customer Lifetime Value (CLV)
- Session counts and engagement
- Purchase frequency
- Churn indicators
- RFM scoring (recency + frequency + monetary) and segments: Champion, Loyal,
  Potential Loyalist, New Customer, Cart Abandoner, Visitor
- Peak shopping hour per customer (`most_active_hour`), top category, top brand

### Product Performance

- Conversion funnel (View → Cart → Purchase)
- Revenue by product/category/brand
- Top performing products

### Daily Sales Summary

- Revenue trends, with day-over-day change from a `lag()` window
- Order counts
- Average order value
- Unique users, sessions and buyers per day

### Conversion Funnel

- Session-level view → cart → purchase rates, broken down by date and `category_l1`
- Cart abandonment rate

## 🧪 Testing

```bash
# Local Spark is enough - no workspace needed
pip install pytest pyspark delta-spark

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_silver_transformations.py -v
```

The suites spin up a local `SparkSession` and exercise the transformation logic on small
hand-built fixtures: schema and audit columns for Bronze, cleaning/dedup/derived columns
for Silver, and the aggregation maths for Gold.

## 🔄 CI/CD

[`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) runs validate → test → deploy:

| Job              | When                                            |
| ---------------- | ----------------------------------------------- |
| `validate`       | every push/PR — `bundle validate` for dev, and for staging/prod on `main` |
| `test`           | after validate — `pytest tests/`                |
| `deploy-dev`     | `develop` and `feature/**` branches             |
| `deploy-staging` | `develop` and `release/**` branches             |
| `deploy-prod`    | `main`, after staging                           |
| `manual-deploy`  | `workflow_dispatch` with an environment input   |

Repository secrets used, by name: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and
`DATABRICKS_TOKEN_PROD` for the production deploy. Nothing else is read from the
environment.

## ⚠️ Limitations

Worth knowing before you fork it:

- **The DLT notebook hard-codes the dev volume path.** `src/dlt/dlt_bronze_to_gold.py`
  reads `/Volumes/ecommerce_analytics_dev/bronze_layer/raw_data` and a `/tmp` schema
  location directly instead of using the `configuration` values the pipeline passes it,
  so the DLT path is dev-only until that is parameterised.
- **`environments/*.yml` are reference documents.** `databricks.yml` only includes
  `resources/*.yml`; the per-environment cluster, Photon and retention settings in
  `environments/` are not applied by the bundle.
- **Environments are catalogs, not workspaces.** dev, staging and prod all deploy to the
  same workspace and are isolated by catalog name only.
- **Serverless only.** `resources/clusters.yml` is entirely commented out, and
  `setup/compute_policies.sql` documents cluster policies as JSON rather than creating them.
- **`setup/external_locations.sql` is a template.** Every `CREATE STORAGE CREDENTIAL` /
  `CREATE EXTERNAL LOCATION` statement is commented out and needs real ADLS Gen2 values.
- **The `warehouse_id` variable is declared but unused** by any job or pipeline.
- **No run artefacts are committed.** The notebooks print record counts, quality scores
  and distributions when they run, but no cell outputs or dashboards are stored in the
  repo — the figures in `docs/` describe the intended design, not a recorded run.

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [Medallion Design](docs/medallion_design.md)
- [Security Model](docs/security_model.md)
- [Performance Optimizations](docs/performance_optimizations.md)
- [Requirements Verification](docs/requirements_verification.md)
- [Demo Walkthrough](docs/demo_walkthrough.md)

## 🤝 Contributing

1. Create a feature branch from `main`
2. Make changes and test locally
3. Submit a pull request
4. CI/CD will validate and deploy to dev

## 📄 License

This project is for educational/assessment purposes.

---

**Author**: Built for Azure Databricks Associate Practical Assessment  
**Dataset Credit**: [Michael Kechinov](https://www.kaggle.com/mkechinov)

**Arnav Bule** — Databricks Certified Data Engineer Professional and Machine Learning Professional

- Portfolio: [www.arnavbule.in](https://www.arnavbule.in)
- GitHub: [@GODOSTROYER](https://github.com/GODOSTROYER)
