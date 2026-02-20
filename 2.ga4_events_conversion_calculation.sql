--Conversion calculation based on traffic channels and dates

WITH CTE1 AS (
  SELECT 
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS string) AS user_session_id,
    event_name, traffic_source.source, traffic_source.medium, traffic_source.name AS campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
  WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') IS NOT NULL
    AND event_name IN ('session_start', 'add_to_cart', 'begin_checkout', 'purchase')
),

CTE2 AS (
  SELECT 
    MIN(DATE(event_timestamp)) AS event_date, 
    user_session_id,source, medium, campaign,
    MAX(IF(event_name = 'add_to_cart', 1, 0)) AS added_to_cart,
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS began_checkout,
    MAX(IF(event_name = 'purchase', 1, 0)) AS purchased
  FROM CTE1
  GROUP BY 2, 3, 4, 5
)

SELECT 
  event_date,source, medium, campaign,
  COUNT(DISTINCT user_session_id) AS user_session_count,
  ROUND(SUM(added_to_cart) / COUNT(DISTINCT user_session_id), 4) AS visit_to_cart,
  ROUND(SUM(began_checkout) / COUNT(DISTINCT user_session_id), 4) AS visit_to_checkout,
  ROUND(SUM(purchased) / COUNT(DISTINCT user_session_id), 4) AS visit_to_purchase
FROM CTE2
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC