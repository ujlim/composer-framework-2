-- A harmless query used to verify SQL execution and named parameters.
SELECT
  @batch_date AS batch_date,
  CURRENT_TIMESTAMP() AS executed_at,
  'minimal_bq_vine' AS vine_id,
  'run_select_sql' AS grape_id;
