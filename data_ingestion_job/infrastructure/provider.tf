terraform {
  required_providers {
    mongodbatlas = {
      source  = "mongodb/mongodbatlas"
      version = "=1.8.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "=3.4.3"
    }
  }

  backend "azurerm" {
    resource_group_name  = "<INSERT VALUE>"
    storage_account_name = "<INSERT VALUE>"
    container_name       = "<INSERT VALUE>"
    key                  = "data-ingestion-job/terraform.tfstate"
  }
}

provider "mongodbatlas" {
  public_key  = var.mongodbatlas_org_public_key
  private_key = var.mongodbatlas_org_private_key
}