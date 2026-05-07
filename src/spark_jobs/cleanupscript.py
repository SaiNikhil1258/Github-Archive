from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, explode, year, month
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    ArrayType,
    BooleanType,
)

import argparse


# ==================
# ARG PARSE SETUP
# =================
parser = argparse.ArgumentParser(description="GitHub Events ETL pipeline")
parser.add_argument(
    "--year",
    type=int,
    required=True,
    help="Year (e.g., 2024)"
)
parser.add_argument(
    "--month",
    type=int,
    required=True,
    help="Month (1-12)"
)
args = parser.parse_args()



def main():
    # 1. Initialize Spark Session
    spark = (
        SparkSession.builder.appName("GitHub_Archive_ETL")
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
        .config("spark.sql.shuffle.partitions", "72")
        .getOrCreate()
    )

    # --- CONFIGURATION ---
    # Update these paths to match your actual GCP buckets and dataset
    # source_path = "gs://your-source-bucket/raw_data/*.json" # Or specific filename

    dest_gcs_path = "project_id-dev-processed-zone/gh_events"
    bq_temp_bucket = (
        "gcp_id-dev-dataproc-temp"  # Required for Spark-BQ connector
    )

    bq_dataset = "gcp_id.dev_staging_layer"

# gcp_id-dev-raw-zone/github_events

    base_raw_path = "project_id-dev-raw-zone/github_events/"

    year_arg = args.year
    month_arg = f"{args.month:02d}"
    source_path = f"{base_raw_path}{year_arg}-{month_arg}/*json.gz"

    # source_path = f"{base_raw_path}2024-03/*json.gz"

    spark.conf.set("temporaryGcsBucket", bq_temp_bucket)

    print("Defining explicit schemas...")

    # 2. SCHEMA DEFINITION
    github_schema = StructType([
        StructField("id",         StringType(), True),
        StructField("type",       StringType(), True),
        StructField("created_at", StringType(), True),

        StructField("actor", StructType([
            StructField("id",            LongType(),   True),
            StructField("login",         StringType(), True),
            StructField("display_login", StringType(), True),
        ]), True),

        StructField("repo", StructType([
            StructField("id",   LongType(),   True),
            StructField("name", StringType(), True),
        ]), True),

        StructField("payload", StructType([

            # PushEvent fields
            StructField("push_id", LongType(),   True),
            StructField("before",  StringType(), True),  # HEAD SHA before push
            StructField("head",    StringType(), True),  # HEAD SHA after push
            StructField("ref",     StringType(), True),  # branch ref

            # Shared / PullRequestEvent fields
            StructField("action", StringType(), True),   # opened/closed/synchronize/reopened

            # Lightweight PR reference — new schema only has these fields
            StructField("pull_request", StructType([
                StructField("id",     LongType(),   True),
                StructField("number", LongType(),   True),
                StructField("url",    StringType(), True),
                StructField("base", StructType([
                    StructField("ref", StringType(), True),
                    StructField("sha", StringType(), True),
                    StructField("repo", StructType([
                        StructField("id",   LongType(),   True),
                        StructField("name", StringType(), True),
                        StructField("url",  StringType(), True),
                    ]), True),
                ]), True),
                StructField("head", StructType([
                    StructField("ref", StringType(), True),
                    StructField("sha", StringType(), True),
                    StructField("repo", StructType([
                        StructField("id",   LongType(),   True),
                        StructField("name", StringType(), True),
                        StructField("url",  StringType(), True),
                    ]), True),
                ]), True),
            ]), True),

        ]), True),
    ])
    print(f"Reading raw data from: {source_path}")

    # 3. EXTRACT
    raw_df = spark.read.schema(github_schema).json(f"{source_path}")

    print("Executing Transformations...")

    # 4. TRANSFORM
    # Extracting base events
    # raw_df = spark.read.schema(github_schema).json("../../data/*.json.gz")

    # 4. TRANSFORM
    base_df = (
        raw_df
        .withColumn("date",  to_date(col("created_at")))
        .withColumn("month", month(col("date")))
        .withColumn("year",  year(col("date")))
        .withColumn("is_bot", col("actor.login").rlike("(?i).*\\[bot\\].*|.*-bot$"))
    )

    bots_df       = base_df.filter(col("is_bot") == True)
    clean_base_df = base_df.filter(col("is_bot") == False)

    # --- Table 1: Base Events (unchanged) ---
    events_df = clean_base_df.select(
        col("id").alias("event_id"),
        col("type"),
        col("created_at").cast("TIMESTAMP"),
        col("date"),
        col("month"),
        col("year"),
        col("repo.name").alias("repo_name"),
        col("actor.login").alias("actor_login"),
    )

    # --- Table 2: commits_df ---
    # Schema preserved exactly. Unavailable fields are substituted as documented above.
    # commit_sha     → payload.head  (resulting HEAD SHA; best available SHA in new schema)
    # commit_message → NULL          (no equivalent; label clearly so consumers know)
    # author_name    → actor.login   (the pusher; individual commit authors unavailable)
    commits_df = (
        clean_base_df
        .filter(col("type") == "PushEvent")
        .select(
            col("id").alias("event_id"),
            col("date"),
            col("month"),
            col("year"),
            col("repo.name").alias("repo_name"),
            col("actor.login").alias("actor_login"),
            col("payload.head").alias("commit_sha"),           # HEAD SHA after push
            lit(None).cast(StringType()).alias("commit_message"),  # not available
            col("actor.login").alias("author_name"),           # pusher as proxy
        )
    )

    # --- Table 3: developer_productivity_df ---
    # Schema preserved exactly. Unavailable fields substituted as documented above.
    # developer_login → actor.login         (PR actor; payload.pull_request.user gone)
    # lines_added     → NULL                (not available in new schema)
    # lines_deleted   → NULL                (not available in new schema)
    # files_changed   → NULL                (not available in new schema)
    # pr_state        → payload.action      (opened/closed/synchronize/reopened)
    # is_merged       → action == 'closed'  (proxy only; see note above)
    developer_productivity_df = (
        clean_base_df
        .filter(col("type") == "PullRequestEvent")
        .select(
            col("id").alias("event_id"),
            col("date"),
            col("month"),
            col("year"),
            col("repo.name").alias("repo_name"),
            col("actor.login").alias("developer_login"),
            lit(None).cast(LongType()).alias("lines_added"),    # not available
            lit(None).cast(LongType()).alias("lines_deleted"),  # not available
            lit(None).cast(LongType()).alias("files_changed"),  # not available
            col("payload.action").alias("pr_state"),
            # is_merged proxy: True when action is 'closed' BUT this is not equivalent
            # to payload.pull_request.merged — a closed PR may have been declined.
            # Enrich with GET /repos/{owner}/{repo}/pulls/{pr_number} if exactness needed.
            (col("payload.action") == "closed").alias("is_merged"),
        )
    )

    print("Writing data to GCS Staging...")

    # 5. LOAD TO GCS
    # Partitioning by date is highly recommended for time-series data

    events_df.repartition("year", "month", "date").write.mode("overwrite").partitionBy(
        "year", "month", "date"
    ).parquet(f"{dest_gcs_path}/events/")

    commits_df.repartition("year", "month", "date").write.mode("overwrite").partitionBy(
        "year", "month", "date"
    ).parquet(f"{dest_gcs_path}/commits/")

    developer_productivity_df.repartition("year", "month").write.mode(
        "overwrite"
    ).partitionBy("year", "month").parquet(f"{dest_gcs_path}/productivity/")

    bots_df.repartition("year", "month", "date").write.mode("overwrite").partitionBy(
        "year", "month", "date"
    ).parquet(f"{dest_gcs_path}/bot_data/")

    # events_df.repartition("year", "month", "date").write.mode("overwrite").partitionBy(
    #     "year", "month", "date"
    # ).parquet("/mnt/d/work/Aries/data/events")
    # commits_df.repartition(3).write.mode("overwrite").partitionBy("date").parquet("/mnt/d/work/Aries/data/commits")

    print("Writing data to BigQuery...")
    # 6. LOAD TO BIGQUERY
    events_df.write.format("bigquery").option(
        "table", f"{bq_dataset}.github_events"
    ).mode("append").save()

    commits_df.write.format("bigquery").option(
        "table", f"{bq_dataset}.github_commits"
    ).mode("append").save()

    developer_productivity_df.write.format("bigquery").option(
        "table", f"{bq_dataset}.developer_productivity"
    ).mode("append").save()
    #     "table", f"{bq_dataset}.github_events"
    # ).option("writeMethod", "direct").option("partitionField", "date").option(
    #     "clusterFields", "type,repo_name"
    # ).option(
    #     "partitionType", "DAY"
    # ).option(
    #     "allowFieldAddition", "true"
    # ).option(
    #     "allowFieldRelaxation", "true"
    # ).mode(
    #     "append"
    # ).save()

    # commits_df.write.format("bigquery").option(
    #     "table", f"{bq_dataset}.github_commits"
    # ).option("writeMethod", "direct").option("partitionField", "date").option(
    #     "clusterFields", "repo_name,actor_login"
    # ).option(
    #     "allowFieldAddition", "true"
    # ).option(
    #     "allowFieldRelaxation", "true"
    # ).mode(
    #     "overwrite"
    # ).save()

    # developer_productivity_df.write.format("bigquery").option(
    #     "table", f"{bq_dataset}.developer_productivity"
    # ).option("writeMethod", "direct").option("partitionField", "date").option(
    #     "partitionType", "MONTH"
    # ).option(
    #     "clusterFields", "repo_name,developer_login"
    # ).option(
    #     "allowFieldAddition", "true"
    # ).option(
    #     "allowFieldRelaxation", "true"
    # ).mode(
    #     "overwrite"
    # ).save()

    print("ETL Job Completed Successfully!")


if __name__ == "__main__":
    main()
