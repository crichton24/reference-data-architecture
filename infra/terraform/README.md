# Infrastructure

Terraform for the AWS side: the landing bucket, the IAM role Databricks
assumes to read it, and lifecycle rules.

Not yet applied — the resources were created by hand while prototyping and are
being brought under Terraform incrementally via `terraform import`. Until then
`main.tf` describes intent, not reality. Do not run `apply` against a live
account without reconciling state first.
