-- BRONZE: raw landing zone. Every business column is stored as text exactly as received
-- (empty strings become NULL at ingestion). Nothing is rejected here: bad data must be
-- visible and traceable, not silently dropped. Lineage columns start with an underscore.

CREATE TABLE IF NOT EXISTS `${BRONZE}`.raw_properties (
  _row_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  property_id   VARCHAR(255),
  name          VARCHAR(255),
  city          VARCHAR(255),
  stars         VARCHAR(255),
  total_rooms   VARCHAR(255),
  _batch_id     VARCHAR(40)  NOT NULL,
  _source_file  VARCHAR(255) NOT NULL,
  _ingested_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_source_file (_source_file)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${BRONZE}`.raw_guests (
  _row_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  guest_id      VARCHAR(255),
  full_name     VARCHAR(255),
  email         VARCHAR(255),
  country       VARCHAR(255),
  created_at    VARCHAR(255),
  _batch_id     VARCHAR(40)  NOT NULL,
  _source_file  VARCHAR(255) NOT NULL,
  _ingested_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_source_file (_source_file),
  KEY idx_guest_id (guest_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${BRONZE}`.raw_bookings (
  _row_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  booking_id    VARCHAR(255),
  property_id   VARCHAR(255),
  guest_id      VARCHAR(255),
  room_type     VARCHAR(255),
  check_in      VARCHAR(255),
  check_out     VARCHAR(255),
  nightly_rate  VARCHAR(255),
  currency      VARCHAR(255),
  status        VARCHAR(255),
  booked_at     VARCHAR(255),
  _batch_id     VARCHAR(40)  NOT NULL,
  _source_file  VARCHAR(255) NOT NULL,
  _ingested_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_source_file (_source_file),
  KEY idx_booking_id (booking_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${BRONZE}`.raw_payments (
  _row_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  payment_id    VARCHAR(255),
  booking_id    VARCHAR(255),
  amount        VARCHAR(255),
  currency      VARCHAR(255),
  method        VARCHAR(255),
  paid_at       VARCHAR(255),
  _batch_id     VARCHAR(40)  NOT NULL,
  _source_file  VARCHAR(255) NOT NULL,
  _ingested_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_source_file (_source_file),
  KEY idx_payment_id (payment_id)
) ENGINE=InnoDB;
