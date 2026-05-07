from google.cloud import bigquery

def create_github_tables(project_id, dataset_id):
    """
    Create BigQuery tables with proper partitioning and clustering.
    
    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset name
    """
    
    client = bigquery.Client(project=project_id)
    
    # ========== TABLE 1: github_events ==========
    print("Creating github_events table...")
    
    github_events_schema = [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("repo_name", "STRING"),
        bigquery.SchemaField("actor_login", "STRING"),
    ]
    
    github_events_table_id = f"{project_id}.{dataset_id}.github_events"
    github_events_table = bigquery.Table(github_events_table_id, schema=github_events_schema)
    github_events_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    github_events_table.clustering_fields = ["type", "repo_name"]
    
    try:
        client.create_table(github_events_table, exists_ok=True)
        print(f"✓ {github_events_table_id} created/updated successfully")
    except Exception as e:
        print(f"✗ Error creating {github_events_table_id}: {e}")
    
    # ========== TABLE 2: github_commits ==========
    print("\nCreating github_commits table...")
    
    github_commits_schema = [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("repo_name", "STRING"),
        bigquery.SchemaField("actor_login", "STRING"),
        bigquery.SchemaField("commit_sha", "STRING"),
        bigquery.SchemaField("commit_message", "STRING"),
        bigquery.SchemaField("author_name", "STRING"),
    ]
    
    github_commits_table_id = f"{project_id}.{dataset_id}.github_commits"
    github_commits_table = bigquery.Table(github_commits_table_id, schema=github_commits_schema)
    github_commits_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="date",
    )
    github_commits_table.clustering_fields = ["repo_name", "actor_login"]
    
    try:
        client.create_table(github_commits_table, exists_ok=True)
        print(f"✓ {github_commits_table_id} created/updated successfully")
    except Exception as e:
        print(f"✗ Error creating {github_commits_table_id}: {e}")
    
    # ========== TABLE 3: developer_productivity ==========
    print("\nCreating developer_productivity table...")
    
    developer_productivity_schema = [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("month", "INTEGER"),
        bigquery.SchemaField("year", "INTEGER"),
        bigquery.SchemaField("repo_name", "STRING"),
        bigquery.SchemaField("developer_login", "STRING"),
        bigquery.SchemaField("lines_added", "INTEGER"),
        bigquery.SchemaField("lines_deleted", "INTEGER"),
        bigquery.SchemaField("files_changed", "INTEGER"),
        bigquery.SchemaField("pr_state", "STRING"),
        bigquery.SchemaField("is_merged", "BOOLEAN"),
    ]
    
    developer_productivity_table_id = f"{project_id}.{dataset_id}.developer_productivity"
    developer_productivity_table = bigquery.Table(developer_productivity_table_id, schema=developer_productivity_schema)
    developer_productivity_table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="date",
    )
    developer_productivity_table.clustering_fields = ["repo_name", "developer_login"]
    
    try:
        client.create_table(developer_productivity_table, exists_ok=True)
        print(f"✓ {developer_productivity_table_id} created/updated successfully")
    except Exception as e:
        print(f"✗ Error creating {developer_productivity_table_id}: {e}")
    
    print("\n✓ All tables created successfully!")


if __name__ == "__main__":
    # ===== CONFIGURATION =====
    PROJECT_ID = "gcp_id"  # Replace with your project ID
    DATASET_ID = "dev_staging_layer"        # Replace with your dataset name
    
    create_github_tables(PROJECT_ID, DATASET_ID)