#Conections
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credential_file)
  project     = var.project
  region      = var.region
  zone        = var.zone
}

#Resources
#Storage
resource "google_storage_bucket" "epl_landing_files" {
  name          = var.epl_landing_files_name
  location      = var.region
  force_destroy = "true"
  labels = {
    deployment-tool = "terraform"
  }
}
resource "google_storage_bucket" "epl_sending_files" {
  name          = var.epl_sending_files_name
  location      = var.region
  force_destroy = "true"
  labels = {
    deployment-tool = "terraform"
  }
}

#Composer
resource "google_composer_environment" "epl_composer" {
  name   = var.composer_env_name
  region = var.region
  config {
    node_count = 4

    node_config {
      zone         = var.zone
      machine_type = var.machine_type_composer

      network    = google_compute_network.epl_network.id
      subnetwork = google_compute_subnetwork.epl_subnetwork.id

      service_account = var.service_account_env
    }
  }
}

resource "google_compute_network" "epl_network" {
  name                    = "epl-composer-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "epl_subnetwork" {
  name          = "epl-composer-subnetwork"
  ip_cidr_range = "10.2.0.0/24"
  region        = var.region
  network       = google_compute_network.epl_network.id
}

#Bigquery
resource "google_bigquery_dataset" "epl_dataset" {
  dataset_id  = "epl_dataset"
  description = "Dataset for ingest files from epl"
  location    = var.location
}