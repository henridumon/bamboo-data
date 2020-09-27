WITH
  users AS ( 
  SELECT
    DISTINCT user_pseudo_id,
    geo.country
  FROM
    `bamboodata-project.analytics_220006363.events_` /* INSERT NAME OF YOUR EVENTS TABLE HERE */
  WHERE
    event_timestamp = user_first_touch_timestamp),
  registrations AS (
  SELECT
    DISTINCT user_pseudo_id,
    MAX(CASE
        WHEN user_id IS NOT NULL THEN 1
      ELSE
      0
    END
      ) OVER (PARTITION BY user_pseudo_id) AS registered_user_flag
  FROM
    `bamboodata-project`.`dbt_hdumon`.`events`), /* INSERT NAME OF YOUR EVENTS TABLE HERE */
  paying_users AS (
  SELECT
    DISTINCT user_pseudo_id,
    1 AS paying_user_flag
  FROM
    `bamboodata-project.analytics_220006363.events_` /* INSERT NAME OF YOUR EVENTS TABLE HERE */
  WHERE
    event_name = 'in_app_purchase'),
  by_week AS (
  SELECT
    DISTINCT TIMESTAMP_TRUNC(TIMESTAMP_MICROS(user_first_touch_timestamp), WEEK(MONDAY), 'UTC') AS first_touch_week,
    TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), WEEK(MONDAY), 'UTC') AS event_week,
    event_name,
    device.category device_category,
    app_info.id app_info_id,
    user_pseudo_id
  FROM
    `bamboodata-project`.`dbt_hdumon`.`events` /* INSERT NAME OF YOUR EVENTS TABLE HERE */
--   WHERE
--    _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)) /* FILTER OUT EVENTS TABLES THAT ARE MORE THAN 3 MONTHS OLD */
--     AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
  GROUP BY
    first_touch_week,
    event_week,
    event_name,
    user_pseudo_id,
    device.category,
    app_info.id),
  with_week_number AS (
  SELECT
    bw.user_pseudo_id,
    CASE
      WHEN pu.paying_user_flag = 1 THEN 'Paying user'
    ELSE
    'Non paying user'
  END
    paying_user_flag,
    users.country,
    reg.registered_user_flag,
    bw.first_touch_week,
    bw.event_week,
    device_category,
    app_info_id,
    bw.event_name,
    FLOOR( TIMESTAMP_DIFF(bw.event_week, bw.first_touch_week, DAY)/7 ) AS weeks_since_first_touch
  FROM
    by_week bw
  LEFT JOIN
    users
  ON
    bw.user_pseudo_id = users.user_pseudo_id
  LEFT JOIN
    registrations reg
  ON
    bw.user_pseudo_id = reg.user_pseudo_id
  LEFT JOIN
    paying_users pu
  ON
    bw.user_pseudo_id = pu.user_pseudo_id )
SELECT
  country,
  registered_user_flag,
  paying_user_flag,
  first_touch_week,
  device_category,
  app_info_id,
  event_name,
  user_pseudo_id,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 0 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_0,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 1 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_1,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 2 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_2,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 3 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_3,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 4 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_4,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 5 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_5,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 6 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_6,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 7 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_7,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 8 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_8,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 9 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_9,
  CASE
    WHEN MAX(CASE
      WHEN weeks_since_first_touch = 10 THEN 1
    ELSE
    0
  END
    ) = 1 THEN user_pseudo_id
  ELSE
  NULL
END
  week_10
FROM
  with_week_number
WHERE
  DATE(first_touch_week) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 10 WEEK),WEEK(MONDAY))
GROUP BY
  user_pseudo_id,
  country,
  registered_user_flag,
  paying_user_flag,
  first_touch_week,
  app_info_id,
  device_category,
  event_name,
  user_pseudo_id