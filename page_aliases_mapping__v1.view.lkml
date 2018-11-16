view: page_aliases_mapping__v1 {
  derived_table: {
    sql_trigger_value: select current_date ;;
    sortkeys: ["tenantbase_visitor_id", "alias"]
    distribution: "alias"
    sql: WITH
      all_mappings AS (
        SELECT
          anonymous_id,
          user_id,
          received_at
        FROM production.pages
        UNION
        SELECT
          user_id AS anonymous_id,
          NULL AS user_id,
          received_at
        FROM production.pages
      )
SELECT
  DISTINCT anonymous_id AS alias,
  COALESCE(FIRST_VALUE(user_id IGNORE NULLS)
    OVER(
      PARTITION BY anonymous_id
      ORDER BY received_at
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),anonymous_id) AS tenantbase_visitor_id
FROM all_mappings
 ;;
  }

  measure: count_tenantbase_visitor {
    label: "Number of TenantBase Visitors"
    type: count_distinct
    sql: ${tenantbase_visitor_id} ;;
    drill_fields: [tenantbase_visitor_id]
  }

  dimension: alias {
    primary_key: yes
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: tenantbase_visitor_id {
    type: string
    sql: ${TABLE}.tenantbase_visitor_id ;;
  }
}
