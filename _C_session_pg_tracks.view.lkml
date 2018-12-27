view: sessions_pg_trk {
  derived_table: {
    sortkeys: ["session_start_at"]
    distribution: "tenantbase_visitor_id"
    sql_trigger_value: SELECT COUNT(*) FROM ${mapped_events.SQL_TABLE_NAME} ;;
    sql: SELECT
             ROW_NUMBER() OVER(PARTITION BY tenantbase_visitor_id ORDER BY "timestamp") || ' - '||  tenantbase_visitor_id as session_id
           , tenantbase_visitor_id
           , "timestamp" AS session_start_at
           , ROW_NUMBER() OVER(PARTITION BY tenantbase_visitor_id ORDER BY "timestamp") AS session_sequence_number
           -- Default offset of lead is 1 (gives the next possible value)
           , LEAD("timestamp") OVER(PARTITION BY tenantbase_visitor_id ORDER BY "timestamp") AS next_session_start_at
         FROM ${mapped_events.SQL_TABLE_NAME}
         -- Where clause indicates either end of session or first session
         WHERE (idle_time_minutes > 30 OR idle_time_minutes is NULL)
 ;;
  }

  dimension: session_id {
    hidden: yes
    sql: ${TABLE}.session_id ;;
  }

  dimension: tenantbase_visitor_id {
    type: string
    sql: ${TABLE}.tenantbase_visitor_id ;;
  }

  dimension_group: start {
    type: time
    timeframes: [time, date, week, month, raw]
    sql: ${TABLE}.session_start_at ;;
  }

  dimension: session_sequence_number {
    type: number
    sql: ${TABLE}.session_sequence_number ;;
  }

  dimension_group: next_session_start_at {
    type: time
    timeframes: [time, date, week, month, raw]
    sql: ${TABLE}.next_session_start_at ;;
  }

  dimension: is_first_session {
    #     type: yesno
    sql: CASE WHEN ${session_sequence_number} = 1 THEN 'First Session'
           ELSE 'Repeat Session'
      END
       ;;
  }

  dimension: session_duration_minutes {
    type: number
    sql: DATEDIFF(minutes, ${start_time}::timestamp, ${session_pg_trk_facts.end_time}::timestamp) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: percent_of_total_count {
    type: percent_of_total
    sql: ${count} ;;
  }

  measure: count_visitors {
    type: count_distinct
    sql: ${tenantbase_visitor_id} ;;
    drill_fields: [tenantbase_visitor_id]
  }

  measure: avg_sessions_per_user {
    type: number
    value_format_name: decimal_2
    sql: ${count}::numeric / nullif(${count_visitors}, 0) ;;
  }

  measure: avg_session_duration_minutes {
    type: average
    sql: ${session_duration_minutes} ;;
    value_format_name: decimal_1
  }

  set: detail {
    fields: [session_id, tenantbase_visitor_id, start_date, session_sequence_number, next_session_start_at_date]
  }
}
