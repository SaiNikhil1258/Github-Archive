output "raw_bucket_name" {
  value = google_storage_bucket.raw_zone.name
}

output "processed_bucket_name" {
  value = google_storage_bucket.processed_zone.name
}

output "dataproc_cluster_name" {
  value = google_dataproc_cluster.spark_cluster.name
}

output "dataproc_service_account" {
  value = var.existing_sa_email
}