-- Gives Databricks its own path to S3. Airflow's credentials do not carry
-- over — the two systems authenticate independently.
--
-- The IAM role needs a trust policy allowing the Databricks account to assume
-- it. Generate that policy from the Databricks UI rather than hand-writing it;
-- a 403 on COPY INTO almost always traces back to here.

create storage credential if not exists nyc_transit_cred
    with iam role 'arn:aws:iam::REPLACE_ACCOUNT:role/databricks-s3-access'
    comment 'Read access to the NYC transit landing zone';

create external location if not exists nyc_transit_landing
    url 's3://REPLACE_BUCKET/landing/'
    with (storage credential nyc_transit_cred)
    comment 'S3 landing zone written by Airflow';

grant read files on external location nyc_transit_landing to `REPLACE_PRINCIPAL`;
