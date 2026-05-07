/* @bruin 
name: gcp_id.dev_staging_layer.bronze_layer_github_commits
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - repo_name
    - actor_login

depends:
  - asset: gcp_id.dev_staging_layer.bronze_layer_github_events

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: event_id
    type: STRING
    description: Unique identifier for the event.
    checks:
      - name: not_null
  - name: created_at
    type: TIMESTAMP
    checks:
      - name: not_null
  - name: date
    type: DATE
  - name: month
    type: INTEGER
  - name: year
    type: INTEGER
  - name: repo_name
    type: STRING
  - name: actor_login
    type: STRING
  - name: commit_sha
    type: STRING
    primary_key: true  # Make sure this is truly a 1-to-1 relationship!
    checks:
      - name: not_null
  - name: commit_message
    type: STRING
  - name: author_name
    type: STRING
  - name: ingested_at
    type: TIMESTAMP

@bruin */


WITH deduplicated_events AS (
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
    c.event_id,
    e.created_at,
    c.date,
    c.month,
    c.year,
    c.repo_name,
    c.actor_login,
    c.commit_sha,
    c.commit_message,
    c.author_name,
    CURRENT_TIMESTAMP() AS ingested_at
FROM
    `gcp_id.dev_staging_layer.github_commits` c
LEFT JOIN 
    deduplicated_events e 
ON 
    c.event_id = e.event_id
WHERE
    -- c.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    e.event_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.commit_sha
    ORDER BY e.created_at DESC
) = 1;