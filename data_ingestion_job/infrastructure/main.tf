resource "mongodbatlas_project" "this" {
  name   = "data-ingestion"
  org_id = data.mongodbatlas_roles_org_id.this.org_id
}

resource "mongodbatlas_project_ip_access_list" "public" {
  project_id = mongodbatlas_project.this.id
  cidr_block = "0.0.0.0/0"
  comment    = "All IPs"
}

resource "random_password" "admin" {
  length  = 32
  special = false
}

resource "mongodbatlas_database_user" "admin" {
  username   = "admin"
  password   = random_password.admin.result
  project_id = mongodbatlas_project.this.id

  auth_database_name = "admin"
  roles {
    role_name     = "dbAdminAnyDatabase"
    database_name = "admin"
  }

  roles {
    role_name     = "readWriteAnyDatabase"
    database_name = "admin"
  }
}

resource "mongodbatlas_advanced_cluster" "this" {
  project_id   = mongodbatlas_project.this.id
  name         = "job-offers-ingestion-cluster"
  cluster_type = "REPLICASET"

  replication_specs {
    region_configs {
      electable_specs {
        instance_size = "M0"
      }
      provider_name         = "TENANT"
      backing_provider_name = "AZURE"
      region_name           = "EUROPE_WEST"
      priority              = 1
    }
  }
}