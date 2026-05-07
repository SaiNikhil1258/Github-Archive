terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  # impersonate_service_account = var.existing_sa_email
}

# ------------------------------------------------------------------------------
# 1. Cloud Storage Buckets (Fixed UBLA Constraint)
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "raw_zone" {
  name                        = "${var.project_id}-${var.env}-raw-zone"
  location                    = var.region
  force_destroy               = true 
  uniform_bucket_level_access = true  # FIX for the 412 Error
}

resource "google_storage_bucket" "processed_zone" {
  name                        = "${var.project_id}-${var.env}-processed-zone"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true  # FIX for the 412 Error
}

resource "google_storage_bucket" "dataproc_temp" {
  name                        = "${var.project_id}-${var.env}-dataproc-temp"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true  # FIX for the 412 Error
}

# ------------------------------------------------------------------------------
# 2. BigQuery Datasets
# ------------------------------------------------------------------------------
locals {
  bq_datasets = [
    "raw_layer",
    "staging_layer",
    "intermediate_layer",
    "reports_layer"
  ]
}

resource "google_bigquery_dataset" "datasets" {
  for_each                   = toset(local.bq_datasets)
  dataset_id                 = "${var.env}_${each.key}"
  location                   = var.location
  delete_contents_on_destroy = true
}

# ------------------------------------------------------------------------------
# 3. Dataproc Cluster (Cost-Optimized & Quota-Safe)
# ------------------------------------------------------------------------------
resource "google_dataproc_cluster" "spark_cluster" {
  name   = "${var.env}-gh-archive-cluster"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_temp.name

    endpoint_config {
      enable_http_port_access = true
    }

    lifecycle_config {
      idle_delete_ttl = "1800s" # Shuts down the cluster after 30 minutes of inactivity
    }

    gce_cluster_config {
      service_account = var.existing_sa_email
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
      ]
    }

    master_config {
      num_instances = 1
      machine_type  = "e2-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "e2-highmem-4"
      disk_config {
        boot_disk_size_gb = 200
      }
    }

    preemptible_worker_config {
      num_instances  = 4    # Changed from 6 to 4 to stay under GCP quota limits
      preemptibility = "SPOT" 
      disk_config {
        boot_disk_size_gb = 200
      }
    }

    software_config {
      image_version = "2.2-debian12"
    }
  }

  lifecycle {
    ignore_changes = [
      cluster_config[0].gce_cluster_config[0].service_account_scopes,
    ]
  }
}

# ------------------------------------------------------------------------------
# 6. Ingestion VM (Data Jump Box)
# ------------------------------------------------------------------------------
resource "google_compute_instance" "ingestion_vm" {
  name         = "${var.env}-ingestion-vm"
  machine_type = "e2-medium" # Cheap, but enough RAM to handle network streams
  zone         = "${var.region}-a" # Puts it in Mumbai right next to your buckets

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20 # We keep the disk small because we won't save files locally
    }
  }

  network_interface {
    network = "default"
    access_config {
      # This gives the VM a public IP so it can reach the internet
    }
  }

  # Attach your admin Service Account to the VM
  service_account {
    email  = var.existing_sa_email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}