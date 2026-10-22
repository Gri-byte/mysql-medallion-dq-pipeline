-- GOLD: analytics-ready star schema (Kimball style) + one pre-aggregated KPI table.

CREATE TABLE IF NOT EXISTS `${GOLD}`.dim_property (
  property_key  INT AUTO_INCREMENT PRIMARY KEY,
  property_id   VARCHAR(20)  NOT NULL,
  name          VARCHAR(120) NOT NULL,
  city          VARCHAR(80)  NOT NULL,
  stars         TINYINT      NOT NULL,
  total_rooms   SMALLINT     NOT NULL,
  UNIQUE KEY uq_dim_property_id (property_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${GOLD}`.dim_guest (
  guest_key   INT AUTO_INCREMENT PRIMARY KEY,
  guest_id    VARCHAR(20)  NOT NULL,
  full_name   VARCHAR(120) NOT NULL,
  email       VARCHAR(160) NOT NULL,
  country     CHAR(2)      NOT NULL,
  UNIQUE KEY uq_dim_guest_id (guest_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${GOLD}`.dim_date (
  date_key      INT          NOT NULL PRIMARY KEY,   -- yyyymmdd
  full_date     DATE         NOT NULL,
  year_num      SMALLINT     NOT NULL,
  quarter_num   TINYINT      NOT NULL,
  month_num     TINYINT      NOT NULL,
  month_name    VARCHAR(12)  NOT NULL,
  day_of_month  TINYINT      NOT NULL,
  day_name      VARCHAR(12)  NOT NULL,
  is_weekend    TINYINT(1)   NOT NULL,
  UNIQUE KEY uq_dim_date_full (full_date)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${GOLD}`.fact_bookings (
  booking_id          VARCHAR(20)   NOT NULL PRIMARY KEY,
  property_key        INT           NOT NULL,
  guest_key           INT           NOT NULL,
  check_in_date_key   INT           NOT NULL,
  check_out_date_key  INT           NOT NULL,
  room_type           VARCHAR(30)   NOT NULL,
  status              VARCHAR(20)   NOT NULL,
  currency            CHAR(3)       NOT NULL,
  nightly_rate        DECIMAL(10,2) NOT NULL,
  nights              SMALLINT      NOT NULL,
  gross_value         DECIMAL(12,2) NOT NULL,   -- nights * rate, whatever the status
  net_value           DECIMAL(12,2) NOT NULL,   -- revenue: only confirmed + completed stays
  paid_amount         DECIMAL(12,2) NOT NULL,
  balance_due         DECIMAL(12,2) NOT NULL,   -- net_value - paid_amount
  booked_at           DATETIME      NOT NULL,
  lead_time_days      INT           NOT NULL,
  KEY idx_fact_property (property_key),
  KEY idx_fact_guest (guest_key),
  KEY idx_fact_check_in (check_in_date_key)
) ENGINE=InnoDB;

-- Grain: property x stay month (month of check-in) x currency.
-- Currencies are never summed together: no FX rates are assumed.
CREATE TABLE IF NOT EXISTS `${GOLD}`.agg_monthly_property_kpis (
  property_key        INT           NOT NULL,
  stay_month          CHAR(7)       NOT NULL,   -- yyyy-mm
  currency            CHAR(3)       NOT NULL,
  bookings            INT           NOT NULL,
  cancelled_bookings  INT           NOT NULL,
  cancellation_rate   DECIMAL(5,4)  NOT NULL,
  room_nights_sold    INT           NOT NULL,
  revenue             DECIMAL(14,2) NOT NULL,
  adr                 DECIMAL(10,2) NULL,       -- average daily rate = revenue / room nights sold
  PRIMARY KEY (property_key, stay_month, currency)
) ENGINE=InnoDB;
