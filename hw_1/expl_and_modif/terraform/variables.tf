locals {
  data_lake_bucket = "de_zoomcamp_data_lake"
}

variable "project" {
  description = "GCP project id"
  default = "de-zoomcamp-khv"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west1"
  type = string
}

variable "credentials" {
  default = "/Users/yauhenikhvainitski/.gc/de-zoomcamp-khv-eb3f1259bf03.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}
