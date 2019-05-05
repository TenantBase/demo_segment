view: page_aliases_mapping {
  derived_table: {
    sql_trigger_value: select current_date ;;
    sortkeys: ["tenantbase_visitor_id", "alias"]
    distribution: "alias"
    sql: with

      -- Establish all child-to-parent edges from tables (tracks, pages, aliases)
      all_mappings AS (
        SELECT anonymous_id AS anonymous_id
        , user_id AS user_id
        , "timestamp" AS "timestamp"
        FROM production.tracks

        UNION

        SELECT user_id AS anonymous_id
          , NULL AS user_id
          , "timestamp" AS "timestamp"
        FROM production.tracks

        UNION

        SELECT anonymous_id AS anonymous_id
          , user_id AS user_id
          , "timestamp"
        FROM production.pages

        UNION

        SELECT user_id AS anonymous_id
        , NULL AS user_id
        , "timestamp"
        FROM production.pages
      )

      SELECT
                  DISTINCT anonymous_id AS alias
                  , COALESCE(FIRST_VALUE(user_id IGNORE NULLS)
                  OVER(
                    PARTITION BY anonymous_id
                    ORDER BY "timestamp"
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),anonymous_id) AS tenantbase_visitor_id
      FROM all_mappings
       ;;
  }

  # Anonymous ID
  dimension: alias {
    primary_key: yes
    sql: ${TABLE}.alias ;;
  }

  # User ID
  dimension: tenantbase_visitor_id {
    sql: ${TABLE}.tenantbase_visitor_id ;;
  }

  measure: count {
    type: count
  }

  measure: count_unique_visitor {
    type: count_distinct
    sql: ${tenantbase_visitor_id} ;;
  }
}

### More Complex Aliasing Using Alias Table ###

#     sql: |
#        with
#
#             -- Establish all child-to-parent edges from tables (tracks, pages, aliases)
#             all_mappings as (
#               select anonymous_id
#                 , user_id
#                 , "timestamp"
#               from hoodie.tracks
#
#               union
#
#               select user_id
#                 , null
#                 , "timestamp"
#               from hoodie.tracks
#
#                union
#
#                select previous_id
#                 , user_id
#                 , "timestamp"
#                from hoodie.aliases
#
#                union
#
#                select user_id
#                  , null
#                  , "timestamp"
#                from hoodie.aliases
#
#                union
#
#                select anonymous_id
#                   , user_id
#                   , "timestamp"
#                from hoodie.pages
#
#                union
#
#                select user_id
#                   , null
#                   , "timestamp"
#                from hoodie.pages
#             ),
#
#             -- Only keep the oldest non-null parent for each child
#             realiases as (
#               select distinct alias
#                 , first_value(next_alias ignore nulls) over(partition by alias order by realiased_at rows between unbounded preceding and unbounded following) as next_alias
#               from all_mappings
#             )
#
#             -- Traverse the tree upwards and point every node at its root
#             select distinct r0.alias
#               , coalesce(r9.next_alias
#                   , r9.alias
#                   , r8.alias
#                   , r7.alias
#                   , r6.alias
#                   , r5.alias
#                   , r4.alias
#                   , r3.alias
#                   , r2.alias
#                   , r1.alias
#                   , r0.alias
#                 ) as tenantbase_visitor_id
#             from realiases r0
#               left join realiases r1 on r0.next_alias = r1.alias
#               left join realiases r2 on r1.next_alias = r2.alias
#               left join realiases r3 on r2.next_alias = r3.alias
#               left join realiases r4 on r3.next_alias = r4.alias
#               left join realiases r5 on r4.next_alias = r5.alias
#               left join realiases r6 on r5.next_alias = r6.alias
#               left join realiases r7 on r6.next_alias = r7.alias
#               left join realiases r8 on r7.next_alias = r8.alias
#               left join realiases r9 on r8.next_alias = r9.alias
#