SELECT format_date( "%Y%m%d" ,date_add(PARSE_DATE("%Y%m%d",event_date), interval (select date_diff(current_date, PARSE_DATE("%Y%m%d",max(event_date)),day) FROM `bamboodata-project.analytics_220006363.events_an_purchases`) day)) event_date 
       ,event_timestamp + (select date_diff(current_date, PARSE_DATE("%Y%m%d",max(event_date)),day) FROM `bamboodata-project.analytics_220006363.events_an_purchases`) * 3600*24*1000000 as event_timestamp
       ,user_first_touch_timestamp + (select date_diff(current_date, PARSE_DATE("%Y%m%d",max(event_date)),day) FROM `bamboodata-project.analytics_220006363.events_an_purchases`) * 3600*24*1000000 as user_first_touch_timestamp,

* EXCEPT(event_date, event_timestamp, user_first_touch_timestamp)
FROM `bamboodata-project.analytics_220006363.events_an_purchases`
order by event_timestamp desc