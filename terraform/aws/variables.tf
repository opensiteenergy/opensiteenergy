variable "adminname" {
  type        = string
  description = "Enter a new administrator username that you will use for logging into your Open Wind Energy server:"
}

variable "password" {
  type        = string
  description = "Enter a password for your new administrator account:"
  sensitive   = true
}

