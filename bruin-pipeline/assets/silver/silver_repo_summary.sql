/* @bruin
name: gcp_id.dev_intermediate_layer.silver_repo_summary
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - repo_name
    - year
    - month

depends:
  - asset: gcp_id.dev_intermediate_layer.silver_developer_activity

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
  - name: date
    type: DATE
    description: Date of the activity.
    primary_key: true
    checks:
      - name: not_null
  - name: created_at
    type: TIMESTAMP
    description: Timestamp of the most recent event for this repo on this date.
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    description: Month of the activity.
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    description: Year of the activity.
    checks:
      - name: not_null
  - name: total_events
    type: INTEGER
    description: Total number of distinct events for this repo on this date.
    checks:
      - name: not_negative
  - name: unique_contributors
    type: INTEGER
    description: Number of distinct developers who contributed on this date.
    checks:
      - name: not_negative
  - name: total_commits
    type: INTEGER
    description: Total number of push events (commits) on this date.
    checks:
      - name: not_negative
  - name: total_prs
    type: INTEGER
    description: Total number of pull request events on this date.
    checks:
      - name: not_negative
  - name: merged_prs
    type: INTEGER
    description: Number of pull requests that were merged on this date.
    checks:
      - name: not_negative
  - name: merge_rate
    type: FLOAT64
    description: Ratio of merged PRs to total PRs. NULL if no PRs exist.
  - name: total_lines_added
    type: INTEGER
    description: Total lines of code added across all PRs on this date.
    checks:
      - name: not_negative
  - name: total_lines_deleted
    type: INTEGER
    description: Total lines of code deleted across all PRs on this date.
    checks:
      - name: not_negative
  - name: ingested_at
    type: TIMESTAMP

@bruin */

SELECT
    repo_name,
    date,
    MAX(created_at)                                                     AS created_at,
    month,
    year,
    COUNT(DISTINCT event_id)                                            AS total_events,
    COUNT(DISTINCT developer_login)                                     AS unique_contributors,
    COUNTIF(event_type = 'PushEvent')                                   AS total_commits,
    COUNTIF(event_type = 'PullRequestEvent')                            AS total_prs,
    COUNTIF(event_type = 'PullRequestEvent' AND is_merged = TRUE)       AS merged_prs,
    SAFE_DIVIDE(
        COUNTIF(event_type = 'PullRequestEvent' AND is_merged = TRUE),
        NULLIF(COUNTIF(event_type = 'PullRequestEvent'), 0)
    )                                                                   AS merge_rate,
    COALESCE(SUM(lines_added), 0)                                       AS total_lines_added,
    COALESCE(SUM(lines_deleted), 0)                                     AS total_lines_deleted,
    CURRENT_TIMESTAMP()                                                 AS ingested_at
FROM
    `gcp_id.dev_intermediate_layer.silver_developer_activity`
GROUP BY
    repo_name,
    date,
    month,
    year;