/* @bruin
name: gcp_id.dev_reports_layer.activity_by_day
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - event_type
    - year
    - month

depends:
  - asset: gcp_id.dev_intermediate_layer.silver_developer_activity

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: date
    type: DATE
    description: Date of the activity.
    primary_key: true
    checks:
      - name: not_null
  - name: event_type
    type: STRING
    description: Type of GitHub event (PushEvent, PullRequestEvent, etc.)
    primary_key: true
    checks:
      - name: not_null
  - name: month
    type: INT64
    description: Month of the activity.
    checks:
      - name: not_null
  - name: year
    type: INT64
    description: Year of the activity.
    checks:
      - name: not_null
  - name: total_events
    type: INT64
    description: Total number of events for this event type on this date.
    checks:
      - name: not_negative
  - name: unique_repos
    type: INT64
    description: Number of distinct repositories active on this date.
    checks:
      - name: not_negative
  - name: unique_actors
    type: INT64
    description: Number of distinct developers active on this date.
    checks:
      - name: not_negative
  - name: ingested_at
    type: TIMESTAMP

@bruin */

SELECT
    date,
    month,
    year,
    event_type,
    COUNT(*)                                    AS total_events,
    COUNT(DISTINCT repo_name)                   AS unique_repos,
    COUNT(DISTINCT developer_login)             AS unique_actors,
    CURRENT_TIMESTAMP()                         AS ingested_at
FROM
    `gcp_id.dev_intermediate_layer.silver_developer_activity`
-- WHERE
--   date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY
    date,
    month,
    year,
    event_type;