-- Daily sample: period_start and period_end should equal business_date.
SELECT
  @business_date AS business_date,
  @period_start AS period_start,
  @period_end AS period_end,
  @period_type AS period_type,
  CURRENT_TIMESTAMP() AS executed_at,
  'sample_business_daily_vine' AS vine_id;
