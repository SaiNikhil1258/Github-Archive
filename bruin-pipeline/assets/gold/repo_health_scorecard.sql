/* @bruin
name: gcp_id.dev_reports_layer.repo_health_scorecard
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: null
  strategy: merge
  cluster_by:
    - repo_name
    - year
    - month

depends:
  - asset: gcp_id.dev_intermediate_layer.silver_repo_summary

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: repo_name
    type: STRING
    description: Name of the GitHub repository.
    primary_key: true
    checks:
      - name: not_null
  - name: month_start_date
    type: DATE
    description: First day of the month.
    checks:
      - name: not_null
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
  - name: total_events
    type: INT64
    description: Total number of events for this repo in this month.
    checks:
      - name: not_negative
  - name: unique_contributors
    type: INT64
    description: Peak number of unique contributors in any single day this month.
    checks:
      - name: not_negative
  - name: total_commits
    type: INT64
    description: Total number of commits this month.
    checks:
      - name: not_negative
  - name: total_prs
    type: INT64
    description: Total number of pull requests this month.
    checks:
      - name: not_negative
  - name: merged_prs
    type: INT64
    description: Total number of merged pull requests this month.
    checks:
      - name: not_negative
  - name: merge_rate
    type: FLOAT64
    description: Average daily merge rate for this repo this month.
  - name: net_loc
    type: INT64
    description: Net lines of code change for the month.
  - name: avg_commits_per_contributor
    type: FLOAT64
    description: Average commits per unique contributor this month.
  - name: ingested_at
    type: TIMESTAMP

@bruin */

SELECT
    repo_name,
    year,
    month,
    DATE(
      CAST(year as string) || '-' ||
      LPAD(CAST(month as STRING), 2, '0') || '-01'
    )                                                               AS month_start_date,
    SUM(total_events)                                               AS total_events,
    MAX(unique_contributors)                                        AS unique_contributors,
    SUM(total_commits)                                              AS total_commits,
    SUM(total_prs)                                                  AS total_prs,
    SUM(merged_prs)                                                 AS merged_prs,
    SAFE_DIVIDE(
        SUM(merged_prs),
        NULLIF(SUM(total_prs), 0)
    )                                                               AS merge_rate,
    SUM(total_lines_added) - SUM(total_lines_deleted)               AS net_loc,
    SAFE_DIVIDE(
        SUM(total_commits),
        NULLIF(MAX(unique_contributors), 0)
    )                                                               AS avg_commits_per_contributor,
    CURRENT_TIMESTAMP()                                             AS ingested_at
FROM
    `gcp_id.dev_intermediate_layer.silver_repo_summary`
WHERE date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH))
GROUP BY
    repo_name,
    year,
    month