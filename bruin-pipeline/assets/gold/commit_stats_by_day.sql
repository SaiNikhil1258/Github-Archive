/* @bruin
name: gcp_id.dev_reports_layer.commit_stats_by_day
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
  - asset: gcp_id.dev_intermediate_layer.silver_developer_activity

secrets:
  - key: gcp_default
    inject_as: gcp_default

columns:
  - name: date
    type: DATE
    description: Date of the commit activity.
    primary_key: true
    checks:
      - name: not_null
  - name: repo_name
    type: STRING
    description: Name of the GitHub repository.
    primary_key: true
    checks:
      - name: not_null
  - name: actor_login
    type: STRING
    description: GitHub login of the actor who pushed the commits.
    primary_key: true
    checks:
      - name: not_null
  - name: author_name
    type: STRING
    description: Git author name on the commit.
    primary_key: true
  - name: total_commits
    type: INT64
    description: Total number of commits pushed by this actor on this date.
    checks:
      - name: not_negative
  - name: is_author_actor_match
    type: BOOLEAN
    description: Whether the git author name matches the GitHub actor login.
  - name: ingested_at
    type: TIMESTAMP

@bruin */

SELECT
    date,
    repo_name,
    developer_login                             AS actor_login,
    author_name,
    COUNT(*)                                    AS total_commits,
    developer_login = author_name               AS is_author_actor_match,
    CURRENT_TIMESTAMP()                         AS ingested_at
FROM
    `gcp_id.dev_intermediate_layer.silver_developer_activity`
WHERE
    event_type = 'PushEvent'
  -- AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY
    date,
    repo_name,
    developer_login,
    author_name;