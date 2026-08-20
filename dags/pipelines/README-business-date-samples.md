# Business Date Sample Test Guide

The sample pipelines validate the framework business context independently from Airflow logical/execution dates.

## Context fields

Every Root/Vine execution resolves:

- `business_date`: business anchor date
- `period_type`: `daily`, `monthly`, or `yearly`
- `period_start`: first date of the resolved business period
- `period_end`: last date of the resolved business period

## Sample DAGs

| Period | Root DAG | Vine DAG | Schedule |
|---|---|---|---|
| Daily | `sample_business_daily_root` | `sample_business_daily_vine` | `0 2 * * *` |
| Monthly | `sample_business_monthly_root` | `sample_business_monthly_vine` | `0 2 1 * *` |
| Yearly | `sample_business_yearly_root` | `sample_business_yearly_vine` | `0 2 1 1 *` |

## Manual Root tests

Trigger each Root from the Airflow UI and enter `Business Date`.

### Daily

Input: `2026-08-19`

Expected BigQuery result:

- business_date = `2026-08-19`
- period_type = `daily`
- period_start = `2026-08-19`
- period_end = `2026-08-19`

### Monthly

Input: `2026-08-31`

Expected BigQuery result:

- business_date = `2026-08-31`
- period_type = `monthly`
- period_start = `2026-08-01`
- period_end = `2026-08-31`

### Yearly

Input: `2026-12-31`

Expected BigQuery result:

- business_date = `2026-12-31`
- period_type = `yearly`
- period_start = `2026-01-01`
- period_end = `2026-12-31`

## Manual Vine tests

The same inputs can be entered directly when triggering each sample Vine. A direct Vine run uses the Vine's configured `business_date.type`.

## Scheduled Root examples

All sample Roots use `source: data_interval_end` and `offset_days: -1`.

- Daily Root run ending `2026-08-20 02:00 Asia/Seoul` -> `business_date=2026-08-19`
- Monthly Root run ending `2026-09-01 02:00 Asia/Seoul` -> `business_date=2026-08-31`, period `2026-08-01..2026-08-31`
- Yearly Root run ending `2027-01-01 02:00 Asia/Seoul` -> `business_date=2026-12-31`, period `2026-01-01..2026-12-31`

## Failure test

Trigger a Root or Vine manually without `Business Date`.

Expected behavior: `resolve_business_date` fails before any downstream Vine/Grape workload starts. There is no fallback to `ds`, `logical_date`, or `execution_date`.
