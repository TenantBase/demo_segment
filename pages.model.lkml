connection: "redshift"

# include all views in this project
# - include: "*.dashboard.lookml"  # include all dashboards in this project
include: "*.view"

explore: pages {
  hidden: yes
  fields: [
    ALL_FIELDS*,
    -pages.avg_page_view_duration_minutes,
    -pages.count_distinct_pageviews,
    -pages.count_visitors]
  join: page_aliases_mapping__v1 {
    type: inner
    relationship: many_to_one
    sql_on: COALESCE(${pages.user_id},${pages.anonymous_id}) = ${page_aliases_mapping__v1.alias} ;;
  }
}

explore: event_facts {
  hidden: yes
  view_label: "Events"
  label: "Events"

  join: pages {
    view_label: "Events"
    fields: [
        pages.context_campaign_content
      , pages.context_campaign_medium
      , pages.context_campaign_name
      , pages.name
      , pages.received_date
      , pages.title
      , pages.url
      , pages.user_id
      , pages.count
      , pages.avg_page_view_duration_minutes
      , pages.count_distinct_pageviews
      , pages.count_pageviews
    ]
    type: left_outer
    sql_on: event_facts.uuid = pages.uuid
      and event_facts."timestamp" = pages."timestamp"
      and event_facts.anonymous_id = pages.anonymous_id
       ;;
    relationship: one_to_one
  }

  join: tracks {
    view_label: "Events"
    type: left_outer
    sql_on: ${event_facts.event_id} = concat(${tracks.event_id}, '-t')
      and event_facts."timestamp" = tracks."timestamp"
      and event_facts.tenantbase_visitor_id = COALESCE(tracks.user_id, tracks.anonymous_id)
       ;;
    relationship: one_to_one
    fields: [event]
  }

  join: page_facts {
    view_label: "Events"
    type: left_outer
    sql_on: event_facts.event_id = page_facts.event_id and
      event_facts."timestamp" = page_facts."timestamp" and
      event_facts.tenantbase_visitor_id = page_facts.tenantbase_visitor_id
       ;;
    relationship: one_to_one
  }

  join: sessions_pg_trk {
    view_label: "Sessions"
    type: left_outer
    sql_on: ${event_facts.session_id} = ${sessions_pg_trk.session_id} ;;
    relationship: many_to_one
  }

  join: session_pg_trk_facts {
    view_label: "Sessions"
    type: left_outer
    sql_on: ${event_facts.session_id} = ${session_pg_trk_facts.session_id} ;;
    relationship: many_to_one
  }
}
