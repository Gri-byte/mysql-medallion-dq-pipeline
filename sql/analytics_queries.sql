-- Example analytics on the GOLD layer. Open this file in DBeaver and run each query with Ctrl+Enter.
-- (Default database names assumed: hotel_gold / hotel_dq / hotel_silver. Change them if you set DB_PREFIX.)

-- 1) Monthly revenue and month-over-month growth, per currency (currencies are never mixed).
SELECT stay_month, currency, revenue,
       LAG(revenue) OVER (PARTITION BY currency ORDER BY stay_month) AS prev_month_revenue,
       ROUND(100 * (revenue / NULLIF(LAG(revenue) OVER (PARTITION BY currency ORDER BY stay_month), 0) - 1), 1) AS mom_growth_pct
FROM (
  SELECT stay_month, currency, SUM(revenue) AS revenue
  FROM hotel_gold.agg_monthly_property_kpis
  GROUP BY stay_month, currency
) m
ORDER BY currency, stay_month;

-- 2) Top 3 properties by revenue within each currency (window function: RANK).
SELECT currency, property, city, revenue, revenue_rank
FROM (
  SELECT f.currency, p.name AS property, p.city, SUM(f.net_value) AS revenue,
         RANK() OVER (PARTITION BY f.currency ORDER BY SUM(f.net_value) DESC) AS revenue_rank
  FROM hotel_gold.fact_bookings f
  JOIN hotel_gold.dim_property p ON p.property_key = f.property_key
  GROUP BY f.currency, p.name, p.city
) ranked
WHERE revenue_rank <= 3
ORDER BY currency, revenue_rank;

-- 3) Cancellation rate by property.
SELECT p.name AS property, p.city,
       COUNT(*) AS bookings,
       SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
       ROUND(100 * SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 1) AS cancellation_pct
FROM hotel_gold.fact_bookings f
JOIN hotel_gold.dim_property p ON p.property_key = f.property_key
GROUP BY p.name, p.city
ORDER BY cancellation_pct DESC;

-- 4) Does booking earlier mean more cancellations? Lead-time buckets.
SELECT CASE WHEN lead_time_days < 14 THEN '0-13 days'
            WHEN lead_time_days < 45 THEN '14-44 days'
            WHEN lead_time_days < 90 THEN '45-89 days'
            ELSE '90+ days' END AS lead_time_bucket,
       COUNT(*) AS bookings,
       ROUND(100 * AVG(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 1) AS cancellation_pct
FROM hotel_gold.fact_bookings
GROUP BY 1
ORDER BY MIN(lead_time_days);

-- 5) Weekend vs weekday check-ins: volume and average nightly rate.
SELECT CASE WHEN d.is_weekend = 1 THEN 'weekend check-in' ELSE 'weekday check-in' END AS check_in_type,
       f.currency, COUNT(*) AS bookings, ROUND(AVG(f.nightly_rate), 2) AS avg_nightly_rate
FROM hotel_gold.fact_bookings f
JOIN hotel_gold.dim_date d ON d.date_key = f.check_in_date_key
WHERE f.status IN ('confirmed', 'completed')
GROUP BY check_in_type, f.currency
ORDER BY f.currency, check_in_type;

-- 6) Data-quality view: what was rejected, and why (latest data in the quarantine).
SELECT source_table, reason, COUNT(*) AS rejected_rows
FROM hotel_silver.quarantine
GROUP BY source_table, reason
ORDER BY source_table, rejected_rows DESC;

-- 7) Data-quality view: result of every check in the most recent pipeline run.
SELECT c.layer, c.check_name, c.severity, c.status, c.failed_rows, c.total_rows
FROM hotel_dq.check_results c
WHERE c.run_id = (SELECT run_id FROM hotel_dq.pipeline_runs ORDER BY started_at DESC LIMIT 1)
ORDER BY c.result_id;
