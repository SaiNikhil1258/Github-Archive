/* @bruin 

name: gcp_id.dev_staging_layer.bronze_layer_github_events
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - type
    - repo_name
    - actor_login

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: event_id
    type: STRING
    description: Unique identifier for the event.
    primary_key: true  
    checks:
      - name: not_null
      - name: unique   
  - name: type
    type: STRING
    description: The type of GitHub Event (e.g., 'PushEvent', 'PullRequestEvent')
  - name: created_at
    type: TIMESTAMP
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
  - name: ingested_at
    type: TIMESTAMP

@bruin */
SELECT
  event_id,
  type,
  created_at,
  date,
  month,
  year,
  repo_name,
  actor_login,
  CURRENT_TIMESTAMP() AS ingested_at
FROM
  `gcp_id.dev_staging_layer.github_events` 
-- WHERE DATE(created_at) >= CURRENT_DATE() - 3
-- WHERE
  -- date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY
      created_at DESC
) = 1;