variable "project_id" {
  description = "gcp_id"
  type        = string
}

variable "region" {
  description = "The GCP region for resources"
  type        = string
  default     = "asia-south1"
}

variable "location" {
  description = "The location for BigQuery datasets (can be a region or multi-region)"
  type        = string
  default     = "asia-south1"
}

variable "env" {
  description = "Environment prefix (e.g., dev, prod)"
  type        = string
  default     = "dev"
}

variable "existing_sa_email" {
  description = "The email of your existing highly privileged Service Account"
  type        = string
}