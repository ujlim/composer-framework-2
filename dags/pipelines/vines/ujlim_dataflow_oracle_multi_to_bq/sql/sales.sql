-- Author: 임의진 (LIM UI JIN)
-- Created: 2026-10-28
-- Updated: 2026-10-31
-- SAMPLE_* and columns are illustrative placeholders; replace before running.
SELECT
  CAST(SALE_ID AS VARCHAR2(64)) AS "sale_id",
  TO_CHAR(SALE_AMOUNT, 'FM9999999999999990D00',
          'NLS_NUMERIC_CHARACTERS=''.,''') AS "sale_amount",
  TO_CHAR(SYS_EXTRACT_UTC(SOLD_AT), 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"') AS "sold_at",
  '{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}' AS "business_date"
FROM SAMPLE_SALES
WHERE UPDATED_AT >= TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD')
  AND UPDATED_AT < TO_DATE('{{ ti.xcom_pull(task_ids="resolve_business_date")["business_date"] }}', 'YYYY-MM-DD') + 1
