- view: tracks
  sql_table_name: hoodie.tracks
  fields:

  - dimension: anonymous_id
    sql: ${TABLE}.anonymous_id

  - dimension: context_app_build
    sql: ${TABLE}.context_app_build

  - dimension: context_app_release_version
    sql: ${TABLE}.context_app_release_version

  - dimension: context_app_version
    sql: ${TABLE}.context_app_version

  - dimension: context_carrier
    sql: ${TABLE}.context_carrier

  - dimension: context_device_idfa
    sql: ${TABLE}.context_device_idfa

  - dimension: context_device_manufacturer
    sql: ${TABLE}.context_device_manufacturer

  - dimension: context_device_model
    sql: ${TABLE}.context_device_model

  - dimension: context_device_type
    sql: ${TABLE}.context_device_type

  - dimension: context_ip
    sql: ${TABLE}.context_ip

  - dimension: context_library_name
    sql: ${TABLE}.context_library_name

  - dimension: context_library_version
    sql: ${TABLE}.context_library_version

  - dimension: context_os
    sql: ${TABLE}.context_os

  - dimension: context_os_name
    sql: ${TABLE}.context_os_name

  - dimension: context_os_version
    sql: ${TABLE}.context_os_version

  - dimension: context_screen_height
    type: number
    sql: ${TABLE}.context_screen_height

  - dimension: context_screen_width
    type: number
    sql: ${TABLE}.context_screen_width

  - dimension: context_user_agent
    sql: ${TABLE}.context_user_agent

  - dimension: event
    sql: ${TABLE}.event

  - dimension: event_id
    primary_key: true
    sql: ${TABLE}.event_id

  - dimension: event_text
    sql: ${TABLE}.event_text

  - dimension_group: send
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.send_at

  - dimension_group: sent
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}.sent_at
  
  - dimension_group: weeks_since_first_visit
    type: number
    sql: FLOOR(DATEDIFF(day,${user_track_facts.first_track_date}, ${sent_date})/7)
  
  - dimension: user_id
    sql: ${TABLE}.user_id
  
  - dimension: is_new_user
    sql:  |
        CASE 
        WHEN ${sent_date} = ${user_track_facts.first_track_date} THEN 'New User'
        ELSE 'Returning User' END

  - measure: count
    type: count
    drill_fields: [context_library_name, context_os_name]
  
  - measure: count_users
    type: count_distinct
    sql: ${user_id}