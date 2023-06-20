variable "owner" {
  description = "Owner's name and surname."
  type        = string
}

variable "subscription_id" {
  type = string
}

variable "client_id" {
  description = "Client ID (called Application as well) of the registered app."
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Client secret registered within the application."
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  type = string
}