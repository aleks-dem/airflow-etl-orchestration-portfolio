SELECT
  event_id,
  source_id,
  category_id,
  actor_id,
  metric_value,
  metric_unit,
  status_code,
  created_at,
  updated_at
FROM analytics_demo_raw.source_events
WHERE updated_at >= now() - INTERVAL 2 HOUR
