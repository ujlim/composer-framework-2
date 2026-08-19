-- Run once in BigQuery before testing the procedure Grape.
-- Replace YOUR_GCP_PROJECT.
CREATE OR REPLACE PROCEDURE
  `gcp-prod-edp-sa-ubi.BDPL199.sp_framework_test`(p_batch_date DATE)
BEGIN
  SELECT
    p_batch_date AS batch_date,
    CURRENT_TIMESTAMP() AS executed_at,
    'stored procedure call succeeded' AS message;
END;
