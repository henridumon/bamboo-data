

  create or replace view `bamboodata-project`.`dbt_hdumon`.`bbd_events_all_days`
  OPTIONS()
  as SELECT
  *,
  extract(hour from timestamp_micros(event_timestamp)) event_hour,
        case when extract(dayofweek from timestamp_micros(event_timestamp)) = 1 then '1. Monday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 2 then '2. Tuesday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 3 then '3. Wednesday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 4 then '4. Thursday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 5 then '5. Friday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 6 then '6. Saturday'
             when extract(dayofweek from timestamp_micros(event_timestamp)) = 7 then '7. Sunday'
        end as event_weekday_name,
  case when traffic_source.name = '(direct)' or traffic_source.name = '(organic)' or traffic_source.name is null then 'Organic' else traffic_source.name end as acquisition_channel,
  FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S",TIMESTAMP_MICROS(event_timestamp)) event_time,
  coalesce(device.vendor_id,
    device.advertising_id) vendor_advertising_id,
  DATE_DIFF(EXTRACT(DATE
    FROM
      TIMESTAMP_MICROS(event_timestamp)), EXTRACT(DATE
    FROM
      TIMESTAMP_MICROS(user_first_touch_timestamp)),DAY) AS days_since_first_touch,
  CASE
    WHEN MAX(CASE
      WHEN user_id IS NOT NULL THEN 1
    ELSE
    0
  END
    ) OVER (PARTITION BY user_pseudo_id) = 1 THEN 'Registered User'
  ELSE
  'Non Registered User'
END
  AS registered_user_flag,
  CASE
    WHEN MAX(CASE
      WHEN event_name = 'in_app_purchase' THEN 1
    ELSE
    0
  END
    ) OVER (PARTITION BY user_pseudo_id) = 1 THEN 'Paying User'
  ELSE
  'Non Paying User'
END
  AS paying_user_flag
FROM
  `bamboodata-project`.`dbt_hdumon`.`events`;

