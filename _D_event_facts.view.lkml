view: event_facts {
  derived_table: {
    # Rebuilds after sessions rebuilds
    sql_trigger_value: SELECT COUNT(*) FROM ${sessions_pg_trk.SQL_TABLE_NAME} ;;
    sortkeys: ["event_id"]
    distribution: "event_id"
    sql: SELECT
             t."timestamp"
           , t.anonymous_id
           , t.event_id
           , t.uuid AS uuid
           , t.event_source
           , s.session_id
           , t.tenantbase_visitor_id
           , t.referrer AS referrer
           -- tracked events sequence number
           , ROW_NUMBER()
               OVER(PARTITION BY s.session_id ORDER BY t."timestamp") AS track_sequence_number
           -- incorporates source (pages or tracks in the sequence number)
           , ROW_NUMBER()
               OVER(PARTITION BY s.session_id, t.event_source order by t."timestamp") as source_sequence_number
           , FIRST_VALUE(t.referrer IGNORE NULLS)
               OVER (PARTITION BY s.session_id ORDER BY t."timestamp"
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_referrer
         FROM ${mapped_events.SQL_TABLE_NAME} AS t
         LEFT JOIN ${sessions_pg_trk.SQL_TABLE_NAME} AS s
           ON t.tenantbase_visitor_id = s.tenantbase_visitor_id
           AND t."timestamp" >= s.session_start_at
           AND (t."timestamp" < s.next_session_start_at or s.next_session_start_at is null)
       ;;
  }

  dimension: event_id {
    primary_key: yes
    #     hidden: true
    sql: ${TABLE}.event_id ;;
  }

  dimension: uuid {
    type: string
    sql: ${TABLE}.uuid ;;
  }

  dimension: session_id {
    sql: ${TABLE}.session_id ;;
  }

  dimension: first_referrer {
    sql: ${TABLE}.first_referrer ;;
  }

  dimension: first_referrer_domain {
    sql: split_part(${first_referrer},'/',3) ;;
  }

  dimension: first_referrer_domain_mapped {
    sql:
      CASE
        WHEN ${first_referrer_domain} like '%facebook%' THEN 'facebook'
        WHEN ${first_referrer_domain} like '%google%' THEN 'google'
        ELSE ${first_referrer_domain}
      END ;;
  }

  dimension: tenantbase_visitor_id {
    type: string
    sql: ${TABLE}.tenantbase_visitor_id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: sequence_number {
    type: number
    sql: ${TABLE}.track_sequence_number ;;
  }

  dimension: source_sequence_number {
    type: number
    sql: ${TABLE}.source_sequence_number ;;
  }

  measure: count_visitors {
    type: count_distinct
    sql: ${tenantbase_visitor_id} ;;
    drill_fields: [page_facts.tenantbase_visitor_id]
  }
}
