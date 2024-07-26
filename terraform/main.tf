terraform {
    backend "gcs" {}
    required_providers {
      google = {
        version = "~> 4.84.0"
      }
    }
  }

provider "google" {
  project = var.projeto-pipeline
  region = var.regiao
}