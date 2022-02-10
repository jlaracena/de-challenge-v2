#Conections
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
  backend "gcs" {
    prefix      = "bigquery-states"
    bucket      = "epl-terraform-states-31194445d0a1"
  }
}

provider "google" {
  credentials = file(var.credential_file)
  project = var.project
  region  = var.region
  zone    = var.zone
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
resource "google_storage_bucket" "epl_bucket_cloud_function" {
  name     = var.epl_bucket_cloud_function_name
  location      = var.region
  force_destroy = "true"
  labels = {
    deployment-tool = "terraform"
  }
}
#Function
resource "google_storage_bucket_object" "epl_archive_cloud_function" {
  name   = "file_arrival_notification.zip"
  bucket = var.epl_bucket_cloud_function_name
  source = "../../src/cf/code.py"
}

resource "google_cloudfunctions_function" "epl_file_arrival_notification" {
  name                  = var.epl_file_arrival_notification_name
  description           = "Cloud Function for trigger dag when a file arrives."
  runtime               = "python39"
  available_memory_mb   = 128
  source_archive_bucket = var.epl_bucket_cloud_function_name
  source_archive_object = google_storage_bucket_object.epl_archive_cloud_function.name
  timeout               = 60
  entry_point           = "caller"
  ingress_settings      = "ALLOW_INTERNAL_ONLY"
  labels = {
    deployment-tool = "terraform"
  }
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.epl_landing_files.name
    failure_policy {
      retry = false
    }
  }
  environment_variables = {
    PROJECT_NAME     = var.project
    LOCATION         = var.region
    COMPOSER_ENV     = var.composer_env_name
    DAG_NAME         = var.epl_dag_name
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

      service_account = google_service_account.test.name
    }

    database_config {
      machine_type = "db-n1-standard-2"
    }

    web_server_config {
      machine_type = "composer-n1-webserver-2"
    }
  }
}

resource "google_compute_network" "epl_network" {
  name                    = "epl_composer_network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "epl_subnetwork" {
  name          = "epl_composer_subnetwork"
  ip_cidr_range = "10.2.0.0/24"
  region        = var.region
  network       = google_compute_network.epl_network.id
}

resource "google_service_account" "epl_composer_sa" {
  account_id   = "composer-env-account"
  display_name = "Service Account for Composer Environment"
}

#Bigquery -> For tables try with operator in first place (without schema) or fix the schema and then ingest.

resource "google_bigquery_dataset" "epl_dataset" {
  dataset_id                  = "epl_dataset"
  description                 = "Dataset for ingest files from epl"
  location                    = var.location
}

#IAM
  #Cloud Function
  # IAM entry for all users to invoke the function
  resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.epl_file_arrival_notification.project
  region         = google_cloudfunctions_function.epl_file_arrival_notification.region
  cloud_function = google_cloudfunctions_function.epl_file_arrival_notification.name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}

  #Composer
  resource "google_project_iam_member" "composer-worker" {
  role   = "roles/composer.worker"
  member = var.sa_composer
}