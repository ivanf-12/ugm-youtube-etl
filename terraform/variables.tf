locals {
    data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
    default = "ugm-yt-etl"
}

variable "region" {
  description = "asia-southeast2"
  default = "asia-southeast2"
  type = string
}

variable "storage_class" {
    default = "STANDARD"
}

variable "BQ_DATASET" {
    type = string
    default = "youtube_data_all"
}

variable "TABLE_NAME" {
    type = string
    default = "channel_data"
}