-- Gives Databricks its own path to S3. Airflow's credentials do not carry
-- over — the two systems authenticate independently.
--
-- The IAM role needs a trust policy allowing the Databricks account to assume
-- it. Generate that policy from the Databricks UI rather than hand-writing it;
-- a 403 on COPY INTO almost always traces back to here.

-- create storage credential if not exists nyc_tlc_raw_cred
--     with iam role 'arn:aws:iam::dbt_nyc_tlc_rw:role/databricks-s3-access'
--     comment 'Read access to the NYC transit landing zone';

-- create external location if not exists aws_s3_nyc_tlc-raw-data
--     url 's3://nyc-tlc-raw-data-105803061132-us-east-2-an/nyc_tlc/'
--     with (storage credential nyc_tlc_raw_cred)
--     comment 'S3 landing zone written by Airflow';

-- grant read files on external location nyc_tlc_raw to `REPLACE_PRINCIPAL`;


--needs to be done in cli


arn:aws:iam::105803061132:role/databricks-s3-nyc-tlc-raw-role