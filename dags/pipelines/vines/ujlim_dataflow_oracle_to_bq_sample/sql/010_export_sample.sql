SELECT
  TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD') AS BUSINESS_DATE,
  SYSTIMESTAMP AS CURRENT_TS
FROM DUAL
