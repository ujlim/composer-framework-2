-- Yearly sample: business_date should normally be the prior year end.
SELECT
  @business_date AS business_date,
  @period_start AS period_start,
  @period_end AS period_end,
  @period_type AS period_type,
  CURRENT_TIMESTAMP() AS executed_at,
  'sample_business_yearly_vine' AS vine_id;
