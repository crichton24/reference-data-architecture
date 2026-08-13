# 0007. Credential patterns by environment

Date: 2026-08-12
Status: Accepted

## Context

Three systems authenticate independently: Airflow to AWS, Databricks to AWS,
and Airflow to Databricks. Granting one access grants nothing to the others.

Airflow runs in Docker on a developer machine, which has no cloud-native
identity. The Databricks workspace is Free Edition, which has no account
console and therefore no service principals.

## Decision

- **Databricks to AWS:** an assumed IAM role with an external ID, wrapped in a
  Unity Catalog storage credential and external location.
- **Airflow to AWS:** an IAM user with long-lived keys, scoped to `GetObject`,
  `PutObject`, and `ListBucket` on one bucket.
- **Airflow to Databricks:** a personal access token with a finite expiry.

Unity Catalog grants are written as though a least-privilege service principal
were already in use, even though the token carries a human identity.

## Alternatives considered

**An IAM user for Databricks.** Rejected. It would place permanent credentials
inside a third-party system with no expiry and no revocation signal. Role
assumption hands over nothing secret — Databricks proves its identity to AWS
directly, and STS issues credentials valid for about an hour.

**An assumed role for Airflow.** Not available. Role assumption requires an
identity AWS already trusts — an EC2 instance, an EKS pod, a GitHub OIDC
token. Docker Desktop has none.

**A Databricks service principal.** Not available on Free Edition, which has no
account console or account-level APIs. Authentication fails before Unity
Catalog evaluates any grant.

## Consequences

Long-lived AWS keys exist in `.env` on the development machine, and a personal
access token carries full workspace permissions. Both are real weaknesses,
mitigated but not eliminated by single-bucket scoping, `.gitignore`, secret
scanning, and finite token expiry.

Because the Unity Catalog grants are already least-privilege, substituting a
service principal on a paid tier is a credential change rather than a redesign.

Moving Airflow to MWAA or EKS removes the IAM user entirely: attach a role and
leave the connection empty for boto3 to resolve.
