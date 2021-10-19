with source_data as (
    SELECT
  FORMAT_DATE('%F', d) as full_date,
  d AS date_id,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(WEEK FROM d) AS year_week,
  EXTRACT(DAY FROM d) AS year_day,
  EXTRACT(YEAR FROM d) AS fiscal_year,
  FORMAT_DATE('%Q', d) as fiscal_qtr,
  EXTRACT(MONTH FROM d) AS month,
  concat(EXTRACT(YEAR FROM d), '/', lpad(cast(EXTRACT(MONTH FROM d) as string), 2, '0')) as year_month,
  cast(concat(EXTRACT(YEAR FROM d), lpad(cast(EXTRACT(MONTH FROM d) as string), 2, '0')) as int) as pbi_sort_year_month,
  lpad(cast(EXTRACT(MONTH FROM d) as string), 2, '0') as test2,
  FORMAT_DATE('%B', d) as month_name,
  FORMAT_DATE('%w', d) AS week_day,
  FORMAT_DATE('%A', d) AS day_name,
  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS day_is_weekday
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2018-01-01', '2030-01-01', INTERVAL 1 DAY)) AS d )
),


final as (
    select *
    from source_data
)


select *
from final