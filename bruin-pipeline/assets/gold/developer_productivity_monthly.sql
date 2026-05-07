/* @bruin
name: gcp_id.dev_reports_layer.developer_productivity_monthly
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: null
  strategy: merge
  cluster_by:
    - developer_login
    - repo_name
    - year

depends:
  - asset: gcp_id.dev_intermediate_layer.silver_developer_activity

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: year
    type: INT64
    description: Year of the activity.
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INT64
    description: Month of the activity.
    primary_key: true
    checks:
      - name: not_null
  - name: month_start_date
    type: DATE
    description: First day of the month.
    checks:
      - name: not_null
  - name: developer_login
    type: STRING
    description: GitHub login of the developer.
    primary_key: true
    checks:
      - name: not_null
  - name: repo_name
    type: STRING
    description: Name of the GitHub repository.
    primary_key: true
    checks:
      - name: not_null
  - name: total_prs
    type: INT64
    description: Total number of pull request events.
    checks:
      - name: not_negative
  - name: merged_prs
    type: INT64
    description: Number of pull requests that were merged.
    checks:
      - name: not_negative
  - name: merge_rate
    type: FLOAT64
    description: Ratio of merged PRs to total PRs.
  - name: total_lines_added
    type: INT64
    description: Total lines of code added across all PRs.
    checks:
      - name: not_negative
  - name: total_lines_deleted
    type: INT64
    description: Total lines of code deleted across all PRs.
    checks:
      - name: not_negative
  - name: net_loc
    type: INT64
    description: Net lines of code change (lines_added - lines_deleted).
  - name: avg_files_changed
    type: FLOAT64
    description: Average number of files changed per PR.
  - name: avg_pr_size
    type: FLOAT64
    description: Average PR size measured in total lines touched.
  - name: ingested_at
    type: TIMESTAMP

@bruin */

SELECT
    year,
    month,
    DATE(
        CAST(year AS STRING) || '-' ||
        LPAD(CAST(month AS STRING), 2, '0') || '-01'
    )                                                               AS month_start_date,
    developer_login,
    repo_name,
    COUNT(*)                                                        AS total_prs,
    COUNTIF(is_merged = TRUE)                                       AS merged_prs,
    LEAST(
        SAFE_DIVIDE(
            COUNTIF(is_merged = TRUE),
            NULLIF(COUNT(*), 0)
        ), 1.0
    )                                                               AS merge_rate,
    COALESCE(SUM(lines_added), 0)                                   AS total_lines_added,
    COALESCE(SUM(lines_deleted), 0)                                 AS total_lines_deleted,
    COALESCE(SUM(lines_added), 0) - COALESCE(SUM(lines_deleted), 0) AS net_loc,
    AVG(COALESCE(files_changed, 0))                                 AS avg_files_changed,
    AVG(COALESCE(lines_added, 0) + COALESCE(lines_deleted, 0))     AS avg_pr_size,
    CURRENT_TIMESTAMP()                                             AS ingested_at
FROM
    `gcp_id.dev_intermediate_layer.silver_developer_activity`
WHERE
    event_type = 'PullRequestEvent'
    -- AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
    -- AND month >= EXTRACT(MONTH FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
GROUP BY
    year,
    month,
    developer_login,
    repo_name