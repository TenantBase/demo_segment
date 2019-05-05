view: mapped_events {
  derived_table: {
    sortkeys: ["event_id"]
    distribution: "tenantbase_visitor_id"
    sql_trigger_value: SELECT current_date ;;
    sql: SELECT *
        , datediff(minutes, lag("timestamp") OVER(PARTITION BY tenantbase_visitor_id ORDER BY "timestamp"), "timestamp") AS idle_time_minutes
         FROM (
           SELECT
               CONCAT(t."timestamp", t.uuid) || '-t' AS event_id
             , COALESCE(a2v.tenantbase_visitor_id, a2v.alias) AS tenantbase_visitor_id
             , t.anonymous_id
             , t.uuid
             , t."timestamp"
             , NULL AS referrer
             , 'tracks' AS event_source
           FROM production.tracks AS t
           INNER JOIN ${page_aliases_mapping.SQL_TABLE_NAME} AS a2v
             ON a2v.alias = COALESCE(t.user_id, t.anonymous_id)

           UNION ALL

           SELECT
               CONCAT(t."timestamp", t.uuid) || '-p' AS event_id
             , COALESCE(a2v.tenantbase_visitor_id, a2v.alias) AS tenantbase_visitor_id
             , t.anonymous_id
             , t.uuid
             , t."timestamp"
             , t.referrer AS referrer
             , 'pages' AS event_source
           FROM production.pages AS t
           INNER JOIN ${page_aliases_mapping.SQL_TABLE_NAME} AS a2v
             ON a2v.alias = coalesce(t.user_id, t.anonymous_id)
         ) AS e
       ;;
  }

  dimension: event_id {
    sql: ${TABLE}.event_id ;;
  }

  dimension: tenantbase_visitor_id {
    type: string
    sql: ${TABLE}.tenantbase_visitor_id ;;
  }

  dimension: anonymous_id {
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: uuid {
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [time, date, week, month]
    sql: ${TABLE}."timestamp" ;;
  }

  dimension: event {
    sql: ${TABLE}.event ;;
  }

  dimension: referrer {
    sql: ${TABLE}.referrer ;;
  }

  dimension: event_source {
    sql: ${TABLE}.event_source ;;
  }

  dimension: idle_time_minutes {
    type: number
    sql: ${TABLE}.idle_time_minutes ;;
  }

  set: detail {
    fields: [
      event_id,
      tenantbase_visitor_id,
      timestamp_date,
      event,
      referrer,
      event_source,
      idle_time_minutes
    ]
  }
}
