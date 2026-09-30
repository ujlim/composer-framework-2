SELECT
  TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD') AS "business_date",
  SYSTIMESTAMP AS "current_ts"
FROM DUAL
