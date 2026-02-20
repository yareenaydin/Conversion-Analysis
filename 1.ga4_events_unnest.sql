--Data preparation for BI reports

SELECT 
TIMESTAMP(timestamp_micros(event_timestamp)) as event_timestamp, 
user_pseudo_id, 
(SELECT value.int_value FROM UNNEST (event_params) where key='ga_session_id') as session_id, 
event_name, 
geo.country, 
device.category, 
traffic_source.source, 
traffic_source.medium, 
traffic_source.name as campaign
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
AND (SELECT value.int_value FROM UNNEST (event_params) where key='ga_session_id') IS NOT NULL
AND event_name IN ( 'session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')