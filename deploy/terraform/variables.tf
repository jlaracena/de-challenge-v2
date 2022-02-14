variable "project" {}

variable "credential_file" {}

variable "location" {
  default = "US"
}

variable "region" {}

variable "zone" {
  default = "us-central1-c"
}

variable "machine_type_composer" {
  default = "n1-standard-1"
}

#Storage
variable "epl_landing_files_name" {
  default = "epl-landing-file-5886940c92ed"
}
variable "epl_sending_files_name" {
  default = "epl-sending-file-b9e6d09605ea"
}
variable "epl_bucket_cloud_function_name" {
  default = "epl-bucket-cloud-function-405cfea8d0ed"
}

#Composer
variable "composer_env_name" {
  type    = string
  default = "epl-orquestator"

}
variable "epl_dag_name" {
  default = "ingest-epl-files"
}

#service accounts
variable "service_account_env" {
  type = string
  default = "terraform-test@peppy-oven-288419.iam.gserviceaccount.com"
}