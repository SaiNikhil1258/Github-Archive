/* @bruin
name: gcp_id.dev_intermediate_layer.silver_developer_activity
type: bq.sql
connection: gcp_default

materialization:
  type: table
  partition_by: date
  strategy: merge
  cluster_by:
    - developer_login
    - repo_name
    - event_type

depends:
  - asset: gcp_id.dev_staging_layer.bronze_layer_github_commits
  - asset: gcp_id.dev_staging_layer.bronze_layer_github_developer_productivity
  - asset: gcp_id.dev_staging_layer.bronze_layer_github_events

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: event_id
    type: STRING
    description: Unique identifier for the source event.
    primary_key: true
    checks:
      - name: not_null
  - name: developer_login
    type: STRING
    description: GitHub login of the developer who performed the activity.
    primary_key: true
    checks:
      - name: not_null
  - name: event_type
    type: STRING
    description: Type of GitHub event (PushEvent, PullRequestEvent, WatchEvent, etc.)
    primary_key: true
    checks:
      - name: not_null
  - name: created_at
    type: TIMESTAMP
    checks:
      - name: not_null
  - name: date
    type: DATE
    checks:
      - name: not_null
  - name: month
    type: INTEGER
    checks:
      - name: not_null
  - name: year
    type: INTEGER
    checks:
      - name: not_null
  - name: repo_name
    type: STRING
    checks:
      - name: not_null
  - name: commit_sha
    type: STRING
    description: SHA of the commit. Populated only for PushEvents.
  - name: author_name
    type: STRING
    description: Git author name. Populated only for PushEvents.
  - name: lines_added
    type: INTEGER
    description: Lines added. Populated only for PullRequestEvents.
    checks:
      - name: not_negative
  - name: lines_deleted
    type: INTEGER
    description: Lines deleted. Populated only for PullRequestEvents.
    checks:
      - name: not_negative
  - name: files_changed
    type: INTEGER
    description: Files changed. Populated only for PullRequestEvents.
    checks:
      - name: not_negative
  - name: net_loc
    type: INTEGER
    description: Net lines of code change (lines_added - lines_deleted). PullRequestEvents only.
  - name: pr_state
    type: STRING
    description: State of the PR at time of event. PullRequestEvents only.
  - name: is_merged
    type: BOOLEAN
    description: Whether the PR was merged. PullRequestEvents only.
  - name: ingested_at
    type: TIMESTAMP

@bruin */

-- Push events from bronze commits
SELECT
    c.event_id,
    c.actor_login                               AS developer_login,
    c.created_at,
    c.date,
    c.month,
    c.year,
    c.repo_name,
    'PushEvent'                                 AS event_type,
    c.commit_sha,
    c.author_name,
    NULL                                        AS lines_added,
    NULL                                        AS lines_deleted,
    NULL                                        AS files_changed,
    NULL                                        AS net_loc,
    NULL                                        AS pr_state,
    NULL                                        AS is_merged,
    CURRENT_TIMESTAMP()                         AS ingested_at
FROM
    `gcp_id.dev_staging_layer.bronze_layer_github_commits` c
WHERE
    -- c.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    c.actor_login IS NOT NULL
    AND c.repo_name IS NOT NULL
    AND c.actor_login NOT LIKE '%[bot]%'

UNION ALL

-- PR events from bronze developer productivity
SELECT
    pr.event_id,
    pr.developer_login,
    pr.created_at,
    pr.date,
    pr.month,
    pr.year,
    pr.repo_name,
    'PullRequestEvent'                          AS event_type,
    NULL                                        AS commit_sha,
    NULL                                        AS author_name,
    pr.lines_added,
    pr.lines_deleted,
    pr.files_changed,
    pr.lines_added - pr.lines_deleted           AS net_loc,
    pr.pr_state,
    pr.is_merged,
    CURRENT_TIMESTAMP()                         AS ingested_at
FROM
    `gcp_id.dev_staging_layer.bronze_layer_github_developer_productivity` pr
WHERE
    -- pr.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    pr.developer_login IS NOT NULL
    AND pr.repo_name IS NOT NULL
    AND pr.developer_login NOT LIKE '%[bot]%'

UNION ALL

-- All other events from bronze events (Watch, Fork, Issues, etc.)
SELECT
    e.event_id,
    e.actor_login                               AS developer_login,
    e.created_at,
    e.date,
    e.month,
    e.year,
    e.repo_name,
    e.type                                      AS event_type,
    NULL                                        AS commit_sha,
    NULL                                        AS author_name,
    NULL                                        AS lines_added,
    NULL                                        AS lines_deleted,
    NULL                                        AS files_changed,
    NULL                                        AS net_loc,
    NULL                                        AS pr_state,
    NULL                                        AS is_merged,
    CURRENT_TIMESTAMP()                         AS ingested_at
FROM
    `gcp_id.dev_staging_layer.bronze_layer_github_events` e
WHERE
    -- e.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    e.actor_login IS NOT NULL
    AND e.actor_login NOT LIKE '%[bot]%'
    AND e.repo_name IS NOT NULL
    AND e.type NOT IN ('PushEvent', 'PullRequestEvent')