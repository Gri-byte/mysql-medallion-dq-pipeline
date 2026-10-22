-- SILVER: typed, validated, de-duplicated, standardized.
-- Integrity is enforced by the engine (PK / FK / CHECK), so a bug in the transformation
-- cannot silently write an impossible row. Rows that fail validation go to `quarantine`
-- with the reason, instead of being dropped.

CREATE TABLE IF NOT EXISTS `${SILVER}`.properties (
  property_id  VARCHAR(20)  NOT NULL,
  name         VARCHAR(120) NOT NULL,
  city         VARCHAR(80)  NOT NULL,
  stars        TINYINT      NOT NULL,
  total_rooms  SMALLINT     NOT NULL,
  PRIMARY KEY (property_id),
  CONSTRAINT chk_properties_stars CHECK (stars BETWEEN 1 AND 5),
  CONSTRAINT chk_properties_rooms CHECK (total_rooms > 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${SILVER}`.guests (
  guest_id    VARCHAR(20)  NOT NULL,
  full_name   VARCHAR(120) NOT NULL,
  email       VARCHAR(160) NOT NULL,
  country     CHAR(2)      NOT NULL,
  created_at  DATE         NULL,
  PRIMARY KEY (guest_id),
  KEY idx_guests_country (country)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${SILVER}`.bookings (
  booking_id    VARCHAR(20)   NOT NULL,
  property_id   VARCHAR(20)   NOT NULL,
  guest_id      VARCHAR(20)   NOT NULL,
  room_type     VARCHAR(30)   NOT NULL,
  check_in      DATE          NOT NULL,
  check_out     DATE          NOT NULL,
  nightly_rate  DECIMAL(10,2) NOT NULL,
  currency      CHAR(3)       NOT NULL,
  status        VARCHAR(20)   NOT NULL,
  booked_at     DATETIME      NOT NULL,
  PRIMARY KEY (booking_id),
  KEY idx_bookings_property (property_id),
  KEY idx_bookings_guest (guest_id),
  KEY idx_bookings_check_in (check_in),
  CONSTRAINT fk_bookings_property FOREIGN KEY (property_id) REFERENCES properties (property_id),
  CONSTRAINT fk_bookings_guest    FOREIGN KEY (guest_id)    REFERENCES guests (guest_id),
  CONSTRAINT chk_bookings_dates   CHECK (check_out > check_in),
  CONSTRAINT chk_bookings_range   CHECK (check_in >= '2000-01-01' AND check_out < '2100-01-01'),
  CONSTRAINT chk_bookings_rate    CHECK (nightly_rate > 0),
  CONSTRAINT chk_bookings_status  CHECK (status IN ('confirmed','completed','cancelled','no_show')),
  CONSTRAINT chk_bookings_ccy     CHECK (currency IN ('USD','ARS','EUR'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${SILVER}`.payments (
  payment_id  VARCHAR(20)   NOT NULL,
  booking_id  VARCHAR(20)   NOT NULL,
  amount      DECIMAL(12,2) NOT NULL,
  currency    CHAR(3)       NOT NULL,
  method      VARCHAR(20)   NOT NULL,
  paid_at     DATETIME      NOT NULL,
  PRIMARY KEY (payment_id),
  KEY idx_payments_booking (booking_id),
  CONSTRAINT fk_payments_booking FOREIGN KEY (booking_id) REFERENCES bookings (booking_id),
  CONSTRAINT chk_payments_amount CHECK (amount > 0),
  CONSTRAINT chk_payments_method CHECK (method IN ('card','transfer','cash','other'))
) ENGINE=InnoDB;

-- Rejected rows. raw_record keeps the original values so a data owner can fix the source.
CREATE TABLE IF NOT EXISTS `${SILVER}`.quarantine (
  quarantine_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_table    VARCHAR(40)  NOT NULL,
  record_key      VARCHAR(64)  NULL,
  reason          VARCHAR(60)  NOT NULL,
  raw_record      LONGTEXT     NULL,
  quarantined_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_quarantine_table_reason (source_table, reason)
) ENGINE=InnoDB;
