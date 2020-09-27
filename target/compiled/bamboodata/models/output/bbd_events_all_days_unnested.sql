with data as
(
SELECT
  *,
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
  `bamboodata-project`.`dbt_hdumon`.`events`
)

SELECT
  event_time, event_date, days_since_first_touch, event_timestamp, user_id, vendor_advertising_id, event_name, key, value.int_value, value.string_value
FROM
  data ,
  UNNEST(event_params)