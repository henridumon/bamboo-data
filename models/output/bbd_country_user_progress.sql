with yesterday_new_users as 
(
select geo.country as country, app_info.id, count(distinct user_pseudo_id) yesterday_new_users
FROM {{ref('events')}}
WHERE event_date = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      and event_timestamp = user_first_touch_timestamp
group by geo.country, app_info.id
),

prev_day_new_users as
(
select geo.country as country, app_info.id, count(distinct user_pseudo_id) prev_day_new_users
FROM {{ref('events')}}
WHERE event_date = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      and event_timestamp = user_first_touch_timestamp
group by geo.country, app_info.id
),

three_day_ago_new_users as
(
select geo.country as country, app_info.id, count(distinct user_pseudo_id) three_day_ago_new_users
FROM {{ref('events')}}
WHERE event_date = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
      and event_timestamp = user_first_touch_timestamp
group by geo.country, app_info.id
),

last_week_users as
(
select geo.country as country, app_info.id, count(distinct user_pseudo_id) last_week_users
FROM {{ref('events')}}
WHERE event_date <= FORMAT_DATE("%Y%m%d", DATE_TRUNC(CURRENT_DATE(), WEEK))
      AND event_date > FORMAT_DATE("%Y%m%d", DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 WEEK))
      and event_timestamp = user_first_touch_timestamp
group by geo.country,app_info.id),

previous_week_users as
(
select geo.country as country, app_info.id, count(distinct user_pseudo_id) prev_week_users
FROM {{ref('events')}}
WHERE event_date <= FORMAT_DATE("%Y%m%d", DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 1 WEEK))
      AND event_date > FORMAT_DATE("%Y%m%d", DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 2 WEEK))
      and event_timestamp = user_first_touch_timestamp
group by geo.country, app_info.id)


select lwu.country, struct(lwu.id) as app_info, ynu.yesterday_new_users, pdnu.prev_day_new_users, tdnu.three_day_ago_new_users ,lwu.last_week_users, pwu.prev_week_users
from last_week_users lwu
left join yesterday_new_users ynu
on lwu.country = ynu.country
and lwu.id = ynu.id
left join prev_day_new_users pdnu
on lwu.country = pdnu.country
and lwu.id = pdnu.id
left join three_day_ago_new_users tdnu
on lwu.country = tdnu.country
and lwu.id = tdnu.id
left join previous_week_users pwu
on lwu.country = pwu.country
and lwu.id = pwu.id