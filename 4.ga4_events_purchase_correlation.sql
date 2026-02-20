--Correlation analysis between user engagement and purchase

WITH user_sessions AS (
  SELECT
    user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS string) AS user_session_id,
    SUM( COALESCE( (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'engagement_time_msec'), 0)) AS total_engagement_time,
    CASE WHEN SUM( COALESCE( SAFE_CAST( ( SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'session_engaged') AS integer), 0) ) > 0 THEN 1 ELSE 0 END AS is_session_engaged
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  GROUP BY 1 ),

  purchases AS (
  SELECT
    user_pseudo_id || CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS string) AS user_session_id,
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE event_name = 'purchase'
  GROUP BY user_session_id )
  
SELECT
  CORR(s.total_engagement_time, CASE WHEN p.user_session_id IS NOT NULL THEN 1 ELSE 0 END) AS engagement_time_to_purchase_corr,
  CORR(s.is_session_engaged,CASE WHEN p.user_session_id IS NOT NULL THEN 1 ELSE 0 END) AS engaged_session_to_purchase_corr,
FROM
  user_sessions s LEFT JOIN purchases p
USING
  (user_session_id)