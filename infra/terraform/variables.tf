variable "region" {
  description = "AWS region. Must match the Databricks workspace region — cross-region transfer is the only way this project develops a real bill."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for the lakehouse."
  type        = string
}
