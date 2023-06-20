variable "mongodbatlas_org_public_key" {
  description = "A public API key provides access at the organizational level."
  type        = string
  sensitive   = true
}

variable "mongodbatlas_org_private_key" {
  description = "A private API key provides access at the organizational level."
  type        = string
  sensitive   = true
}