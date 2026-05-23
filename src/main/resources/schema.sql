CREATE TABLE IF NOT EXISTS static_enriched_events (
    id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id        VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(64),
    user_id         VARCHAR(64),
    event_timestamp TIMESTAMP(6) NULL,
    source_ip       VARCHAR(64),
    domain          VARCHAR(255),
    department      VARCHAR(128),
    role            VARCHAR(128),
    site            VARCHAR(128),
    manager_email   VARCHAR(255),
    created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_static_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dynamic_enriched_events (
    id                       BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id                 VARCHAR(64)  NOT NULL,
    event_type               VARCHAR(64),
    user_id                  VARCHAR(64),
    event_timestamp          TIMESTAMP(6) NULL,
    source_ip                VARCHAR(64),
    domain                   VARCHAR(255),
    risk_score               INT,
    session_active           BOOLEAN,
    policy_violation         BOOLEAN,
    last_seen_minutes_ago    INT,
    created_at               TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_dynamic_event_id (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
