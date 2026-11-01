-- Author: 임의진 (LIM UI JIN)
-- Created: 2026-10-28
-- Updated: 2026-10-31
-- SAMPLE_* and columns are illustrative placeholders; replace before running.
SELECT
  CAST(EMP_ID AS VARCHAR2(64)) AS "emp_id",
  EMP_NAME AS "emp_name",
  TO_CHAR(HIRED_AT, 'YYYY-MM-DD HH24:MI:SS') AS "hired_at",
  '{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}' AS "business_date"
FROM SAMPLE_EMP
WHERE UPDATED_AT >= TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD')
  AND UPDATED_AT < TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD') + 1
