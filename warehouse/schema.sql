PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stg_occupation_data (
  onetsoc_code      TEXT NOT NULL,
  title             TEXT NOT NULL,
  description       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_occupation (
  occupation_id     INTEGER PRIMARY KEY,
  onetsoc_code      TEXT NOT NULL UNIQUE,
  title             TEXT NOT NULL,
  description       TEXT,
  major_group_code  TEXT,
  FOREIGN KEY (major_group_code) REFERENCES dim_major_group(major_group_code)
);

CREATE TABLE IF NOT EXISTS dim_major_group (
  major_group_code       TEXT PRIMARY KEY,
  code_full              TEXT NOT NULL UNIQUE,
  name                   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_element (
  element_id  TEXT PRIMARY KEY,
  domain      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_occupation_element_rating (
  occupation_id     INTEGER NOT NULL,
  element_id        TEXT    NOT NULL,
  scale_id          TEXT    NOT NULL,
  data_value        REAL    NOT NULL,
  n                 REAL,
  standard_error    REAL,
  lower_ci_bound    REAL,
  upper_ci_bound    REAL,
  recommend_suppress TEXT,
  not_relevant      TEXT,
  date_updated      TEXT,
  domain_source     TEXT,
  FOREIGN KEY (occupation_id) REFERENCES dim_occupation(occupation_id),
  FOREIGN KEY (element_id) REFERENCES dim_element(element_id),
  FOREIGN KEY (scale_id) REFERENCES dim_scale(scale_id),
  UNIQUE (occupation_id, element_id, scale_id)
);

CREATE TABLE IF NOT EXISTS stg_level_scale_anchors (
  element_id         TEXT NOT NULL,
  scale_id           TEXT NOT NULL,
  anchor_value       INTEGER NOT NULL,
  anchor_description TEXT NOT NULL
);

-- Scales reference (staging)
CREATE TABLE IF NOT EXISTS stg_scales_reference (
  scale_id    TEXT NOT NULL,
  scale_name  TEXT NOT NULL,
  minimum     REAL NOT NULL,
  maximum     REAL NOT NULL
);

-- Anchor descriptions per element/scale
CREATE TABLE IF NOT EXISTS dim_element_scale (
  element_id         TEXT NOT NULL,
  scale_id           TEXT NOT NULL,
  anchor_value       INTEGER NOT NULL,
  anchor_description TEXT NOT NULL,
  PRIMARY KEY (element_id, scale_id, anchor_value),
  FOREIGN KEY (element_id) REFERENCES dim_element(element_id),
  FOREIGN KEY (scale_id)   REFERENCES dim_scale(scale_id)
);

-- Scale reference
CREATE TABLE IF NOT EXISTS dim_scale (
  scale_id   TEXT PRIMARY KEY,
  name       TEXT,
  min_value  REAL,
  max_value  REAL,
  step       REAL
);


CREATE TABLE IF NOT EXISTS stg_skills (
  onetsoc_code      TEXT NOT NULL,
  element_id        TEXT NOT NULL,
  scale_id          TEXT NOT NULL,
  data_value        REAL NOT NULL,
  n                 REAL,
  standard_error    REAL,
  lower_ci_bound    REAL,
  upper_ci_bound    REAL,
  recommend_suppress TEXT,
  not_relevant      TEXT,
  date_updated      TEXT NOT NULL,
  domain_source     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_knowledge (
  onetsoc_code      TEXT NOT NULL,
  element_id        TEXT NOT NULL,
  scale_id          TEXT NOT NULL,
  data_value        REAL NOT NULL,
  n                 REAL,
  standard_error    REAL,
  lower_ci_bound    REAL,
  upper_ci_bound    REAL,
  recommend_suppress TEXT,
  not_relevant      TEXT,
  date_updated      TEXT NOT NULL,
  domain_source     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_abilities (
  onetsoc_code      TEXT NOT NULL,
  element_id        TEXT NOT NULL,
  scale_id          TEXT NOT NULL,
  data_value        REAL NOT NULL,
  n                 REAL,
  standard_error    REAL,
  lower_ci_bound    REAL,
  upper_ci_bound    REAL,
  recommend_suppress TEXT,
  not_relevant      TEXT,
  date_updated      TEXT NOT NULL,
  domain_source     TEXT NOT NULL
);

-- Invalid SKA rows captured during transform → staging (diagnostics only)
CREATE TABLE IF NOT EXISTS stg_invalid_ska (
  domain              TEXT NOT NULL, -- 'SKILL' | 'KNOWLEDGE' | 'ABILITY'
  onetsoc_code        TEXT,
  element_id          TEXT,
  scale_id            TEXT,
  data_value          TEXT,
  n                   TEXT,
  standard_error      TEXT,
  lower_ci_bound      TEXT,
  upper_ci_bound      TEXT,
  recommend_suppress  TEXT,
  not_relevant        TEXT,
  date_updated        TEXT,
  domain_source       TEXT,
  error_reason        TEXT NOT NULL
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_dim_occupation_onetsoc_code ON dim_occupation(onetsoc_code);
CREATE INDEX IF NOT EXISTS idx_fact_elem_occ ON fact_occupation_element_rating(occupation_id);
CREATE INDEX IF NOT EXISTS idx_fact_elem_element ON fact_occupation_element_rating(element_id);
CREATE INDEX IF NOT EXISTS idx_fact_elem_scale ON fact_occupation_element_rating(scale_id);
CREATE INDEX IF NOT EXISTS idx_fact_occ_elem_scale
  ON fact_occupation_element_rating(occupation_id, element_id, scale_id);
