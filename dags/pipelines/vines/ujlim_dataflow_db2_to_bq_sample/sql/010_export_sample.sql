SELECT
  DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}') AS "business_date",
  CURRENT TIMESTAMP AS "current_ts"
FROM SYSIBM.SYSDUMMY1
