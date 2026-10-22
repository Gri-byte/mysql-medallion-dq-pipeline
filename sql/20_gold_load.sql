-- SILVER -> GOLD. Full refresh of the star schema and the KPI aggregate.
-- Only runs when the silver data-quality gate has passed.

TRUNCATE TABLE `${GOLD}`.agg_monthly_property_kpis;
TRUNCATE TABLE `${GOLD}`.fact_bookings;
TRUNCATE TABLE `${GOLD}`.dim_date;
TRUNCATE TABLE `${GOLD}`.dim_guest;
TRUNCATE TABLE `${GOLD}`.dim_property;

INSERT INTO `${GOLD}`.dim_property (property_id, name, city, stars, total_rooms)
SELECT property_id, name, city, stars, total_rooms
FROM `${SILVER}`.properties
ORDER BY property_id;

INSERT INTO `${GOLD}`.dim_guest (guest_id, full_name, email, country)
SELECT guest_id, full_name, email, country
FROM `${SILVER}`.guests
ORDER BY guest_id;

-- Calendar covering every stay. The numbers table is built from digits (0..9999 days) so the
-- script does not depend on recursive CTE settings, which differ between MySQL and MariaDB.
INSERT INTO `${GOLD}`.dim_date
  (date_key, full_date, year_num, quarter_num, month_num, month_name, day_of_month, day_name, is_weekend)
SELECT CAST(DATE_FORMAT(x.dt, '%Y%m%d') AS UNSIGNED),
       x.dt, YEAR(x.dt), QUARTER(x.dt), MONTH(x.dt), MONTHNAME(x.dt), DAY(x.dt), DAYNAME(x.dt),
       CASE WHEN DAYOFWEEK(x.dt) IN (1, 7) THEN 1 ELSE 0 END
FROM (
  SELECT DATE_ADD(r.min_d, INTERVAL n.i DAY) AS dt
  FROM (SELECT MIN(check_in) AS min_d, MAX(check_out) AS max_d FROM `${SILVER}`.bookings) r
  JOIN (
    SELECT a.n + 10 * b.n + 100 * c.n + 1000 * d.n AS i
    FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
  ) n ON DATE_ADD(r.min_d, INTERVAL n.i DAY) <= r.max_d
) x;

INSERT INTO `${GOLD}`.fact_bookings
  (booking_id, property_key, guest_key, check_in_date_key, check_out_date_key, room_type, status, currency,
   nightly_rate, nights, gross_value, net_value, paid_amount, balance_due, booked_at, lead_time_days)
SELECT b.booking_id,
       dp.property_key,
       dg.guest_key,
       CAST(DATE_FORMAT(b.check_in,  '%Y%m%d') AS UNSIGNED),
       CAST(DATE_FORMAT(b.check_out, '%Y%m%d') AS UNSIGNED),
       b.room_type,
       b.status,
       b.currency,
       b.nightly_rate,
       DATEDIFF(b.check_out, b.check_in),
       DATEDIFF(b.check_out, b.check_in) * b.nightly_rate,
       CASE WHEN b.status IN ('confirmed', 'completed')
            THEN DATEDIFF(b.check_out, b.check_in) * b.nightly_rate ELSE 0 END,
       COALESCE(pay.paid, 0),
       CASE WHEN b.status IN ('confirmed', 'completed')
            THEN DATEDIFF(b.check_out, b.check_in) * b.nightly_rate - COALESCE(pay.paid, 0) ELSE 0 END,
       b.booked_at,
       DATEDIFF(b.check_in, DATE(b.booked_at))
FROM `${SILVER}`.bookings b
JOIN `${GOLD}`.dim_property dp ON dp.property_id = b.property_id
JOIN `${GOLD}`.dim_guest dg    ON dg.guest_id    = b.guest_id
LEFT JOIN (
  SELECT booking_id, SUM(amount) AS paid
  FROM `${SILVER}`.payments
  GROUP BY booking_id
) pay ON pay.booking_id = b.booking_id;

INSERT INTO `${GOLD}`.agg_monthly_property_kpis
  (property_key, stay_month, currency, bookings, cancelled_bookings, cancellation_rate,
   room_nights_sold, revenue, adr)
SELECT f.property_key,
       DATE_FORMAT(d.full_date, '%Y-%m'),
       f.currency,
       COUNT(*),
       SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END),
       ROUND(SUM(CASE WHEN f.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 4),
       SUM(CASE WHEN f.status IN ('confirmed', 'completed') THEN f.nights ELSE 0 END),
       SUM(f.net_value),
       CASE WHEN SUM(CASE WHEN f.status IN ('confirmed', 'completed') THEN f.nights ELSE 0 END) > 0
            THEN ROUND(SUM(f.net_value) / SUM(CASE WHEN f.status IN ('confirmed', 'completed') THEN f.nights ELSE 0 END), 2)
            ELSE NULL END
FROM `${GOLD}`.fact_bookings f
JOIN `${GOLD}`.dim_date d ON d.date_key = f.check_in_date_key
GROUP BY f.property_key, DATE_FORMAT(d.full_date, '%Y-%m'), f.currency;
