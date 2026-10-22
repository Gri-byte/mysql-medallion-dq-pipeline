-- BRONZE -> SILVER. Full refresh, so re-running the pipeline is idempotent.
-- Pattern for every table:
--   1. de-duplicate: keep the latest ingested row per business key
--   2. classify: compute ONE reject_reason per row (first rule that fails wins; NULL = valid)
--   3. quarantine rejected rows, keeping the raw record
--   4. load valid rows, applying standardization (TRIM / case / types)
-- Validation is explicit (regex + range checks) so bad values are classified instead of
-- aborting the whole batch. The Python caller relaxes STRICT mode for this script only.
-- Calendar validity is checked explicitly too, because engines disagree on impossible dates:
-- MySQL 8 turns '2025-02-30' into NULL, MariaDB keeps it. A parsed date d is valid only if
--   d IS NOT NULL AND DATEDIFF(d, '1970-01-01') IS NOT NULL AND DAYOFMONTH(d) <= DAY(LAST_DAY(d))

-- ---------------------------------------------------------------- reset (children first)
DELETE FROM `${SILVER}`.payments;
DELETE FROM `${SILVER}`.bookings;
DELETE FROM `${SILVER}`.guests;
DELETE FROM `${SILVER}`.properties;
DELETE FROM `${SILVER}`.quarantine;

-- ================================================================ PROPERTIES
INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'properties', NULL, 'MISSING_KEY',
       JSON_OBJECT('property_id', r.property_id, 'name', r.name, 'city', r.city, 'stars', r.stars, 'total_rooms', r.total_rooms)
FROM `${BRONZE}`.raw_properties r
WHERE r.property_id IS NULL OR TRIM(r.property_id) = '';

DROP TEMPORARY TABLE IF EXISTS tmp_properties;
CREATE TEMPORARY TABLE tmp_properties AS
SELECT d.* FROM (
  SELECT r.*, TRIM(r.property_id) AS k,
         ROW_NUMBER() OVER (PARTITION BY TRIM(r.property_id) ORDER BY r._row_id DESC) AS rn
  FROM `${BRONZE}`.raw_properties r
  WHERE r.property_id IS NOT NULL AND TRIM(r.property_id) <> ''
) d
WHERE d.rn = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_properties_c;
CREATE TEMPORARY TABLE tmp_properties_c AS
SELECT t.*,
  CASE
    WHEN t.name IS NULL OR TRIM(t.name) = '' THEN 'MISSING_NAME'
    WHEN t.stars IS NULL OR TRIM(t.stars) NOT REGEXP '^[1-5]$' THEN 'INVALID_STARS'
    WHEN t.total_rooms IS NULL OR TRIM(t.total_rooms) NOT REGEXP '^[0-9]+$' OR CAST(TRIM(t.total_rooms) AS UNSIGNED) = 0 THEN 'INVALID_ROOMS'
    ELSE NULL
  END AS reject_reason
FROM tmp_properties t;

INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'properties', c.k, c.reject_reason,
       JSON_OBJECT('property_id', c.property_id, 'name', c.name, 'city', c.city, 'stars', c.stars, 'total_rooms', c.total_rooms)
FROM tmp_properties_c c
WHERE c.reject_reason IS NOT NULL;

INSERT INTO `${SILVER}`.properties (property_id, name, city, stars, total_rooms)
SELECT c.k, TRIM(c.name), COALESCE(NULLIF(TRIM(c.city), ''), 'Unknown'),
       CAST(TRIM(c.stars) AS UNSIGNED), CAST(TRIM(c.total_rooms) AS UNSIGNED)
FROM tmp_properties_c c
WHERE c.reject_reason IS NULL;

-- ================================================================ GUESTS
INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'guests', NULL, 'MISSING_KEY',
       JSON_OBJECT('guest_id', r.guest_id, 'full_name', r.full_name, 'email', r.email, 'country', r.country, 'created_at', r.created_at)
FROM `${BRONZE}`.raw_guests r
WHERE r.guest_id IS NULL OR TRIM(r.guest_id) = '';

DROP TEMPORARY TABLE IF EXISTS tmp_guests;
CREATE TEMPORARY TABLE tmp_guests AS
SELECT d.* FROM (
  SELECT r.*, TRIM(r.guest_id) AS k, STR_TO_DATE(TRIM(r.created_at), '%Y-%m-%d') AS cr_d,
         ROW_NUMBER() OVER (PARTITION BY TRIM(r.guest_id) ORDER BY r._row_id DESC) AS rn
  FROM `${BRONZE}`.raw_guests r
  WHERE r.guest_id IS NOT NULL AND TRIM(r.guest_id) <> ''
) d
WHERE d.rn = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_guests_c;
CREATE TEMPORARY TABLE tmp_guests_c AS
SELECT t.*,
  CASE
    WHEN t.full_name IS NULL OR TRIM(t.full_name) = '' THEN 'MISSING_NAME'
    WHEN t.email IS NULL OR LOWER(TRIM(t.email)) NOT REGEXP '^[^@ ]+@[^@ ]+\\.[a-z]{2,}$' THEN 'INVALID_EMAIL'
    WHEN t.country IS NULL OR UPPER(TRIM(t.country)) NOT REGEXP '^[A-Z]{2}$' THEN 'INVALID_COUNTRY'
    ELSE NULL
  END AS reject_reason
FROM tmp_guests t;

INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'guests', c.k, c.reject_reason,
       JSON_OBJECT('guest_id', c.guest_id, 'full_name', c.full_name, 'email', c.email, 'country', c.country, 'created_at', c.created_at)
FROM tmp_guests_c c
WHERE c.reject_reason IS NOT NULL;

INSERT INTO `${SILVER}`.guests (guest_id, full_name, email, country, created_at)
SELECT c.k, TRIM(c.full_name), LOWER(TRIM(c.email)), UPPER(TRIM(c.country)),
       CASE WHEN c.cr_d IS NOT NULL AND DATEDIFF(c.cr_d, '1970-01-01') IS NOT NULL
                 AND DAYOFMONTH(c.cr_d) <= DAY(LAST_DAY(c.cr_d))
            THEN c.cr_d ELSE NULL END
FROM tmp_guests_c c
WHERE c.reject_reason IS NULL;

-- ================================================================ BOOKINGS
-- Needs silver.properties and silver.guests (referential integrity is checked against SILVER,
-- so a booking of a quarantined guest is quarantined too, instead of becoming an orphan).
INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'bookings', NULL, 'MISSING_KEY',
       JSON_OBJECT('booking_id', r.booking_id, 'property_id', r.property_id, 'guest_id', r.guest_id, 'room_type', r.room_type,
                   'check_in', r.check_in, 'check_out', r.check_out, 'nightly_rate', r.nightly_rate,
                   'currency', r.currency, 'status', r.status, 'booked_at', r.booked_at)
FROM `${BRONZE}`.raw_bookings r
WHERE r.booking_id IS NULL OR TRIM(r.booking_id) = '';

DROP TEMPORARY TABLE IF EXISTS tmp_bookings;
CREATE TEMPORARY TABLE tmp_bookings AS
SELECT d.* FROM (
  SELECT r.*, TRIM(r.booking_id) AS k,
         STR_TO_DATE(TRIM(r.check_in),  '%Y-%m-%d')          AS ci_d,
         STR_TO_DATE(TRIM(r.check_out), '%Y-%m-%d')          AS co_d,
         STR_TO_DATE(TRIM(r.booked_at), '%Y-%m-%d %H:%i:%s') AS ba_d,
         ROW_NUMBER() OVER (PARTITION BY TRIM(r.booking_id) ORDER BY r._row_id DESC) AS rn
  FROM `${BRONZE}`.raw_bookings r
  WHERE r.booking_id IS NOT NULL AND TRIM(r.booking_id) <> ''
) d
WHERE d.rn = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_bookings_c;
CREATE TEMPORARY TABLE tmp_bookings_c AS
SELECT t.*,
  CASE
    WHEN t.check_in IS NULL OR t.check_out IS NULL OR t.booked_at IS NULL
      OR TRIM(t.check_in)  NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      OR TRIM(t.check_out) NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      OR TRIM(t.booked_at) NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
      OR t.ci_d IS NULL OR DATEDIFF(t.ci_d, '1970-01-01') IS NULL OR DAYOFMONTH(t.ci_d) > DAY(LAST_DAY(t.ci_d))
      OR t.co_d IS NULL OR DATEDIFF(t.co_d, '1970-01-01') IS NULL OR DAYOFMONTH(t.co_d) > DAY(LAST_DAY(t.co_d))
      OR t.ba_d IS NULL OR DATEDIFF(t.ba_d, '1970-01-01') IS NULL OR DAYOFMONTH(t.ba_d) > DAY(LAST_DAY(t.ba_d))
      THEN 'INVALID_DATE'
    WHEN TRIM(t.check_out) <= TRIM(t.check_in) THEN 'CHECKOUT_BEFORE_CHECKIN'
    WHEN t.nightly_rate IS NULL
      OR TRIM(t.nightly_rate) NOT REGEXP '^[0-9]+(\\.[0-9]{1,2})?$'
      OR CAST(TRIM(t.nightly_rate) AS DECIMAL(10,2)) <= 0
      THEN 'INVALID_RATE'
    WHEN t.currency IS NULL OR UPPER(TRIM(t.currency)) NOT IN ('USD','ARS','EUR') THEN 'INVALID_CURRENCY'
    WHEN t.status IS NULL OR LOWER(TRIM(t.status)) NOT IN ('confirmed','completed','cancelled','no_show') THEN 'INVALID_STATUS'
    WHEN p.property_id IS NULL THEN 'ORPHAN_PROPERTY'
    WHEN g.guest_id IS NULL THEN 'ORPHAN_GUEST'
    ELSE NULL
  END AS reject_reason
FROM tmp_bookings t
LEFT JOIN `${SILVER}`.properties p ON p.property_id = TRIM(t.property_id)
LEFT JOIN `${SILVER}`.guests g     ON g.guest_id    = TRIM(t.guest_id);

INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'bookings', c.k, c.reject_reason,
       JSON_OBJECT('booking_id', c.booking_id, 'property_id', c.property_id, 'guest_id', c.guest_id, 'room_type', c.room_type,
                   'check_in', c.check_in, 'check_out', c.check_out, 'nightly_rate', c.nightly_rate,
                   'currency', c.currency, 'status', c.status, 'booked_at', c.booked_at)
FROM tmp_bookings_c c
WHERE c.reject_reason IS NOT NULL;

INSERT INTO `${SILVER}`.bookings
  (booking_id, property_id, guest_id, room_type, check_in, check_out, nightly_rate, currency, status, booked_at)
SELECT c.k, TRIM(c.property_id), TRIM(c.guest_id),
       COALESCE(NULLIF(LOWER(TRIM(c.room_type)), ''), 'standard'),
       c.ci_d, c.co_d,
       CAST(TRIM(c.nightly_rate) AS DECIMAL(10,2)),
       UPPER(TRIM(c.currency)),
       LOWER(TRIM(c.status)),
       c.ba_d
FROM tmp_bookings_c c
WHERE c.reject_reason IS NULL;

-- ================================================================ PAYMENTS
INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'payments', NULL, 'MISSING_KEY',
       JSON_OBJECT('payment_id', r.payment_id, 'booking_id', r.booking_id, 'amount', r.amount,
                   'currency', r.currency, 'method', r.method, 'paid_at', r.paid_at)
FROM `${BRONZE}`.raw_payments r
WHERE r.payment_id IS NULL OR TRIM(r.payment_id) = '';

DROP TEMPORARY TABLE IF EXISTS tmp_payments;
CREATE TEMPORARY TABLE tmp_payments AS
SELECT d.* FROM (
  SELECT r.*, TRIM(r.payment_id) AS k,
         STR_TO_DATE(TRIM(r.paid_at), '%Y-%m-%d %H:%i:%s') AS pd_d,
         ROW_NUMBER() OVER (PARTITION BY TRIM(r.payment_id) ORDER BY r._row_id DESC) AS rn
  FROM `${BRONZE}`.raw_payments r
  WHERE r.payment_id IS NOT NULL AND TRIM(r.payment_id) <> ''
) d
WHERE d.rn = 1;

DROP TEMPORARY TABLE IF EXISTS tmp_payments_c;
CREATE TEMPORARY TABLE tmp_payments_c AS
SELECT t.*,
  CASE
    WHEN t.amount IS NULL
      OR TRIM(t.amount) NOT REGEXP '^[0-9]+(\\.[0-9]{1,2})?$'
      OR CAST(TRIM(t.amount) AS DECIMAL(12,2)) <= 0
      THEN 'INVALID_AMOUNT'
    WHEN t.currency IS NULL OR UPPER(TRIM(t.currency)) NOT IN ('USD','ARS','EUR') THEN 'INVALID_CURRENCY'
    WHEN t.paid_at IS NULL
      OR TRIM(t.paid_at) NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
      OR t.pd_d IS NULL OR DATEDIFF(t.pd_d, '1970-01-01') IS NULL OR DAYOFMONTH(t.pd_d) > DAY(LAST_DAY(t.pd_d))
      THEN 'INVALID_DATE'
    WHEN t.method IS NULL OR LOWER(TRIM(t.method)) NOT IN ('card','transfer','cash','other') THEN 'INVALID_METHOD'
    WHEN b.booking_id IS NULL THEN 'ORPHAN_BOOKING'
    WHEN UPPER(TRIM(t.currency)) <> b.currency THEN 'CURRENCY_MISMATCH'
    ELSE NULL
  END AS reject_reason
FROM tmp_payments t
LEFT JOIN `${SILVER}`.bookings b ON b.booking_id = TRIM(t.booking_id);

INSERT INTO `${SILVER}`.quarantine (source_table, record_key, reason, raw_record)
SELECT 'payments', c.k, c.reject_reason,
       JSON_OBJECT('payment_id', c.payment_id, 'booking_id', c.booking_id, 'amount', c.amount,
                   'currency', c.currency, 'method', c.method, 'paid_at', c.paid_at)
FROM tmp_payments_c c
WHERE c.reject_reason IS NOT NULL;

INSERT INTO `${SILVER}`.payments (payment_id, booking_id, amount, currency, method, paid_at)
SELECT c.k, TRIM(c.booking_id), CAST(TRIM(c.amount) AS DECIMAL(12,2)), UPPER(TRIM(c.currency)),
       LOWER(TRIM(c.method)), c.pd_d
FROM tmp_payments_c c
WHERE c.reject_reason IS NULL;

DROP TEMPORARY TABLE IF EXISTS tmp_properties;
DROP TEMPORARY TABLE IF EXISTS tmp_properties_c;
DROP TEMPORARY TABLE IF EXISTS tmp_guests;
DROP TEMPORARY TABLE IF EXISTS tmp_guests_c;
DROP TEMPORARY TABLE IF EXISTS tmp_bookings;
DROP TEMPORARY TABLE IF EXISTS tmp_bookings_c;
DROP TEMPORARY TABLE IF EXISTS tmp_payments;
DROP TEMPORARY TABLE IF EXISTS tmp_payments_c;
