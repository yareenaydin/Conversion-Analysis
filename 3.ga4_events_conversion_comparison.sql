--Conversion comparison between landing pages

WITH CTE AS(
SELECT event_name, user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS string) AS user_session_id, REGEXP_EXTRACT((SELECT value.string_value FROM UNNEST (event_params) where key='page_location'),r'^https?://[^/]+(/[^?]*)') as page_path
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
AND (SELECT value.int_value FROM UNNEST (event_params) where key='ga_session_id') IS NOT NULL
AND event_name IN ( 'session_start', 'purchase')
)

SELECT page_path, 
count(distinct user_session_id) as user_session_count,
sum(case when event_name= 'purchase' then 1 else 0 end) as purchased_count,
safe_divide(
  sum(case when event_name= 'purchase' then 1 else 0 end),count(distinct user_session_id)
  ) AS visit_to_purchase
from CTE
group by 1
order by 4 desc