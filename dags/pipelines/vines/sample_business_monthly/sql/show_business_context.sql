-- Monthly sample: validates configurable date_offsets including leap-year month ends.
SELECT
  @business_date AS business_date,
  @period_start AS period_start,
  @period_end AS period_end,
  @period_type AS period_type,
  @previous_month_end AS previous_month_end,
  @previous_year_month_end AS previous_year_month_end,
  @two_years_ago_month_end AS two_years_ago_month_end,
  @current_month_start AS current_month_start,
  @previous_year_end AS previous_year_end,
  CURRENT_TIMESTAMP() AS executed_at,
  'sample_business_monthly_vine' AS vine_id;
