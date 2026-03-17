SELECT
  event_id,
  source_id,
  source_name,
  category_id,
  category_name,
  actor_id,
  metric_value,
  metric_unit,
  status_code,
  created_at,
  updated_at,
  now() AS snapshot_loaded_at
FROM analytics_demo.unified_records_stage_2
