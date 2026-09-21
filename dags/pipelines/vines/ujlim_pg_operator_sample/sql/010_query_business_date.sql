SELECT
    CAST(%(business_date)s AS date) AS business_date,
    current_database() AS database_name,
    current_user AS database_user,
    current_timestamp AS executed_at;
