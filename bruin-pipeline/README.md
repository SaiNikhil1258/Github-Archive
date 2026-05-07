# GitHub Archive Data Pipeline

This repository contains the Bruin assets for an end-to-end data pipeline that ingests, processes, and analyzes GitHub Archive data. The pipeline follows a Medallion Architecture (Raw -> Bronze -> Silver -> Gold/Reports) built on Google Cloud Platform (GCS and BigQuery) and orchestrated by Bruin.

## Architecture Overview

1. **Raw Layer (Ingestion):** Python script downloads `.json.gz` data from the GitHub Archive API and lands it in a Google Cloud Storage (GCS) bucket.
2. **Bronze Layer (Staging):** Deduplicates raw events and creates foundational tables for generic events, commits, and pull requests.
3. **Silver Layer (Intermediate):** Unifies various event types into a single, comprehensive timeline of developer activity and creates daily repository-level summaries.
4. **Gold Layer (Reports):** Highly aggregated tables optimized for BI dashboards, focusing on daily activity trends, commit statistics, monthly developer productivity, and overall repository health.

---

## 📦 1. Raw Layer (Data Ingestion)

### `fetch_gh_archive_to_gcs` (Python Asset)
* **File:** `my_python_asset.py`
* **Purpose:** Acts as the entry point of the pipeline. It fetches a full day (24 hours) of GitHub Archive data and uploads it to the raw zone in GCS.
* **Key Features:**
  * Uses Bruin's native context (`BRUIN_START_DATE` and `BRUIN_VARS`) to determine the execution date and target GCS bucket (`data-engineering-488412-dev-raw-zone`).
  * Utilizes `concurrent.futures.ThreadPoolExecutor` for multi-threaded downloads, optimizing network I/O based on the VM's available cores.
  * Downloads files locally to a temporary directory, uploads them directly to GCS partitioned by date (`github_events/YYYY-MM-DD/`), and immediately cleans up local storage.

---

## 🥉 2. Bronze Layer (Staging / Deduplication)

These SQL assets materialize as daily-partitioned merge tables in the `gcp_id.dev_staging_layer`. They enforce data quality rules (like non-null checks) and deduplicate records based on `event_id`.

### `bronze_layer_github_events`
* **File:** `bronze_events.sql`
* **Purpose:** Core staging table for all GitHub events. Deduplicates the underlying `github_events` table using `QUALIFY ROW_NUMBER() = 1` over `event_id`.
* **Primary Key:** `event_id`
* **Clustering:** `type`, `repo_name`, `actor_login`

### `bronze_layer_github_commits`
* **File:** `bronze_commits.sql`
* **Purpose:** Staging table specific to commit data (PushEvents).
* **Dependencies:** Depends on `bronze_layer_github_events` to ensure it only processes valid, deduplicated events.
* **Key Columns:** `commit_sha` (Primary Key), `commit_message`, `author_name`.
* **Clustering:** `repo_name`, `actor_login`

### `bronze_layer_github_developer_productivity`
* **File:** `bronze_developer_productivity.sql`
* **Purpose:** Staging table capturing Pull Request metrics.
* **Dependencies:** Depends on `bronze_layer_github_events`.
* **Key Metrics:** `lines_added`, `lines_deleted`, `files_changed`, `pr_state`, `is_merged`.
* **Primary Keys:** `event_id`, `developer_login`

---

## 🥈 3. Silver Layer (Intermediate / Integration)

These assets combine the isolated bronze tables into clean, unified models materialized in `gcp_id.dev_intermediate_layer`.

### `silver_developer_activity`
* **File:** `silver_developer_activity.sql`
* **Purpose:** Creates a single, unified activity stream of all developer actions.
* **Dependencies:** Depends on all three Bronze layer tables (`commits`, `developer_productivity`, `events`).
* **Logic:** Uses `UNION ALL` to stitch together:
  1. Push events (from bronze commits)
  2. Pull Request events (from bronze productivity)
  3. All other events (Watch, Fork, Issues, etc., from bronze events)
* **Primary Keys:** `event_id`, `developer_login`, `event_type`

### `silver_repo_summary`
* **File:** `silver_repo_summary.sql`
* **Purpose:** Aggregates the `silver_developer_activity` data into a daily summary at the repository level.
* **Dependencies:** Depends on `silver_developer_activity`.
* **Key Metrics:** `total_events`, `unique_contributors`, `total_commits`, `total_prs`, `merged_prs`, `merge_rate`, total lines added/deleted.
* **Primary Keys:** `repo_name`, `date`

---

## 🥇 4. Gold Layer (Reports / Analytics)

These assets provide high-level aggregations designed for end-user reporting and dashboards, materialized in `gcp_id.dev_reports_layer`.

### `activity_by_day`
* **File:** `activity_day_by_day.sql`
* **Purpose:** Daily aggregation of GitHub activity grouped by event type.
* **Dependencies:** `silver_developer_activity`
* **Metrics:** `total_events`, `unique_repos`, `unique_actors`.

### `commit_stats_by_day`
* **File:** `commit_stats_by_day.sql`
* **Purpose:** Daily commit statistics per repository and actor.
* **Dependencies:** `silver_developer_activity`
* **Metrics:** `total_commits`. It also includes a data quality/security flag (`is_author_actor_match`) to check if the git author matches the GitHub user pushing the commit.

### `developer_productivity_monthly`
* **File:** `developer_productivity_monthly.sql`
* **Purpose:** A monthly snapshot of developer performance per repository.
* **Dependencies:** `silver_developer_activity`
* **Metrics:** Calculates advanced metrics specifically from `PullRequestEvent` records, including `total_prs`, `merged_prs`, `merge_rate`, `net_loc` (lines added minus deleted), `avg_files_changed`, and `avg_pr_size`.

### `repo_health_scorecard`
* **File:** `repo_health_scorecard.sql`
* **Purpose:** A monthly executive summary evaluating the overall health and activity of repositories.
* **Dependencies:** `silver_repo_summary`
* **Metrics:** Evaluates `total_events`, peak `unique_contributors`, `total_commits`, `total_prs`, `merge_rate`, `net_loc`, and `avg_commits_per_contributor`.

---

## ⚙️ General Pipeline Details

* **Materialization Strategy:** All SQL assets use BigQuery's `merge` strategy partitioned by `date` (except for monthly rollups which partition by `null` / unpartitioned but merge on keys).
* **Lookback Window:** The SQL models leverage a sliding lookback window (e.g., `WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)` for daily staging/silver tables and `INTERVAL 3 MONTH` for monthly reports) to ensure late-arriving data is captured and updated without requiring full historical recalculations.
* **Data Quality Checks:** Extensive Bruin data quality checks are defined in the YAML headers, including `not_null`, `unique`, and `not_negative` validations on key fields and metrics.