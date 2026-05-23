CREATE TABLE IF NOT EXISTS static_enriched_events (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id        VARCHAR(64)  NOT NULL,
    event_type      VARCHAR(64),
    user_id         VARCHAR(64),
    event_timestamp TIMESTAMP WITH TIME ZONE,
    source_ip       VARCHAR(64),
    domain          VARCHAR(255),
    department      VARCHAR(128),
    role            VARCHAR(128),
    site            VARCHAR(128),
    manager_email   VARCHAR(255),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dynamic_enriched_events (
    id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id                 VARCHAR(64)  NOT NULL,
    event_type               VARCHAR(64),
    user_id                  VARCHAR(64),
    event_timestamp          TIMESTAMP WITH TIME ZONE,
    source_ip                VARCHAR(64),
    domain                   VARCHAR(255),
    risk_score               INTEGER,
    session_active           BOOLEAN,
    policy_violation         BOOLEAN,
    last_seen_minutes_ago    INTEGER,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
