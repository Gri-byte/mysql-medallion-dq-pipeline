-- DQ: pipeline run history and the result of every data-quality check, so quality is
-- observable over time (not just a pass/fail in a CI log).

CREATE TABLE IF NOT EXISTS `${DQ}`.pipeline_runs (
  run_id       VARCHAR(40)  NOT NULL PRIMARY KEY,
  started_at   DATETIME     NOT NULL,
  finished_at  DATETIME     NULL,
  status       VARCHAR(20)  NOT NULL,      -- RUNNING / SUCCESS / FAILED
  failed_stage VARCHAR(20)  NULL,
  message      VARCHAR(500) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `${DQ}`.check_results (
  result_id    BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id       VARCHAR(40)  NOT NULL,
  layer        VARCHAR(10)  NOT NULL,      -- bronze / silver / gold
  check_name   VARCHAR(120) NOT NULL,
  severity     VARCHAR(10)  NOT NULL,      -- ERROR blocks the pipeline, WARN only reports
  status       VARCHAR(10)  NOT NULL,      -- PASS / FAIL
  failed_rows  BIGINT       NOT NULL,
  total_rows   BIGINT       NULL,
  max_ratio    DECIMAL(6,4) NOT NULL DEFAULT 0,
  description  VARCHAR(300) NULL,
  checked_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_check_results_run (run_id),
  KEY idx_check_results_status (status, severity)
) ENGINE=InnoDB;
