/* @bruin
name: gcp_id.dev_staging_layer.bronze_layer_github_developer_productivity
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - repo_name
    - developer_login

secrets:
  - key: gcp_default
    inject_as: gcp_default

depends:
  - asset: gcp_id.dev_staging_layer.bronze_layer_github_events

columns:
  - name: event_id
    type: STRING
    description: Unique identifier for the event.
    primary_key: true  # Make sure this is truly a 1-to-1 relationship!
    checks:
      - name: not_null
  - name: created_at
    type: TIMESTAMP
  - name: date
    type: DATE
    checks:
      - name: not_null
  - name: month
    type: INTEGER
  - name: year
    type: INTEGER
  - name: repo_name
    type: STRING
  - name: developer_login
    type: STRING
    primary_key: true  # Make sure this is truly a 1-to-1 relationship!
    checks:
      - name: not_null
  - name: lines_added
    type: INTEGER
    checks:
      - name: not_negative
  - name: lines_deleted
    type: INTEGER
    checks:
      - name: not_negative
  - name: files_changed
    type: INTEGER
    checks:
      - name: not_negative
  - name: pr_state
    type: STRING
  - name: is_merged
    type: BOOLEAN
  - name: ingested_at
    type: TIMESTAMP

@bruin */
WITH deduplicated_events AS (
    -- This CTE guarantees exactly one row per event_id from the events table
    SELECT
        event_id,
        MAX(created_at) AS created_at
    FROM
        `gcp_id.dev_staging_layer.bronze_layer_github_events`
    -- WHERE
    --     date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)  
    GROUP BY
        event_id
)
SELECT
    d.event_id,
    e.created_at,
    -- Now pulling safely from our deduplicated CTE
    d.date,
    d.month,
    d.year,
    d.repo_name,
    d.developer_login,
    d.lines_added,
    d.lines_deleted,
    d.files_changed,
    d.pr_state,
    d.is_merged,
    CURRENT_TIMESTAMP() AS ingested_at
FROM
    `gcp_id.dev_staging_layer.developer_productivity` d
LEFT JOIN 
    deduplicated_events e ON d.event_id = e.event_id 
WHERE
    d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND e.event_id IS NOT NULL
    AND d.developer_login IS NOT NULL
    AND d.developer_login != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.event_id, d.developer_login
    ORDER BY e.created_at DESC
) = 1;