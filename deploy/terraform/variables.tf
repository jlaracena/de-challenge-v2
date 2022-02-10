variable "project" { }

variable "credentials_file" { }

variable "location" {
  default = "US"
}

variable "region" {
  type = string
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-c"
}

variable "machine_type_composer"{
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

#Function
variable "epl_file_arrival_notification_name"{
  default = "epl-file-arrival-notification"
}

#Composer
variable "composer_env_name"{
  type = string
  default = "epl-orquestator"

}
variable "epl_dag_name" {
  default = "ingest-epl-files"
  
}
variable "sa_composer"{
  type = string
  defaul = "nombre del SA de composer"
}