SELECT
  DATE '{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}' AS BUSINESS_DATE,
  CURRENT_TIMESTAMP AS CURRENT_TS
