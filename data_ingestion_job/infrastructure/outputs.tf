output "connection_string" {
  value = replace(
    mongodbatlas_advanced_cluster.this.connection_strings[0].standard_srv,
    "mongodb+srv://",
    "mongodb+srv://${mongodbatlas_database_user.admin.username}:${random_password.admin.result}@"
  )
  sensitive = true
}