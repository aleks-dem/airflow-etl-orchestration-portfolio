SELECT
  s1.event_id,
  s1.source_id,
  src.source_name,
  s1.category_id,
  cat.category_name,
  s1.actor_id,
  s1.metric_value,
  s1.metric_unit,
  s1.status_code,
  s1.created_at,
  s1.updated_at
FROM analytics_demo.unified_records_stage_1 s1
LEFT JOIN analytics_demo.dim_source src
  ON src.source_id = s1.source_id
LEFT JOIN analytics_demo.dim_category cat
  ON cat.category_id = s1.category_id
