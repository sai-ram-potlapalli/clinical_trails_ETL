-- Clinical Trials Data Warehouse Schema (SQLite compatible)
-- Dimensional model with star schema design

-- Raw data table (landing zone)
CREATE TABLE IF NOT EXISTS raw_trials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nct_id TEXT UNIQUE NOT NULL,
    brief_title TEXT,
    official_title TEXT,
    lead_sponsor_name TEXT,
    lead_sponsor_class TEXT,
    condition TEXT,
    intervention_name TEXT,
    intervention_type TEXT,
    phase TEXT,
    enrollment_count INTEGER,
    study_start_date TEXT,
    primary_completion_date TEXT,
    study_completion_date TEXT,
    status TEXT,
    location_country TEXT,
    location_state TEXT,
    location_city TEXT,
    location_facility TEXT,
    study_type TEXT,
    allocation TEXT,
    intervention_model TEXT,
    primary_purpose TEXT,
    masking_info TEXT,
    outcome_measure_description TEXT,
    extraction_date TEXT DEFAULT CURRENT_TIMESTAMP,
    data_source TEXT DEFAULT 'ClinicalTrials.gov API',
    validation_status TEXT DEFAULT 'valid',
    raw_data TEXT
);

-- Date dimension table
CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend INTEGER NOT NULL,
    is_holiday INTEGER DEFAULT 0,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Sponsor dimension table
CREATE TABLE IF NOT EXISTS dim_sponsor (
    sponsor_key INTEGER PRIMARY KEY AUTOINCREMENT,
    sponsor_id TEXT UNIQUE NOT NULL,
    sponsor_name TEXT NOT NULL,
    sponsor_class TEXT,
    sponsor_type TEXT,
    sponsor_category TEXT,
    is_industry INTEGER DEFAULT 0,
    is_academic INTEGER DEFAULT 0,
    is_government INTEGER DEFAULT 0,
    country TEXT,
    state TEXT,
    city TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
);

-- Location dimension table
CREATE TABLE IF NOT EXISTS dim_location (
    location_key INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id TEXT UNIQUE NOT NULL,
    country TEXT NOT NULL,
    state TEXT,
    city TEXT,
    facility_name TEXT,
    region TEXT,
    continent TEXT,
    latitude REAL,
    longitude REAL,
    timezone TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
);

-- Condition dimension table
CREATE TABLE IF NOT EXISTS dim_condition (
    condition_key INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT UNIQUE NOT NULL,
    condition_name TEXT NOT NULL,
    condition_category TEXT,
    condition_type TEXT,
    icd_code TEXT,
    mesh_term TEXT,
    is_rare INTEGER DEFAULT 0,
    prevalence_category TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
);

-- Intervention dimension table
CREATE TABLE IF NOT EXISTS dim_intervention (
    intervention_key INTEGER PRIMARY KEY AUTOINCREMENT,
    intervention_id TEXT UNIQUE NOT NULL,
    intervention_name TEXT NOT NULL,
    intervention_type TEXT,
    intervention_category TEXT,
    drug_name TEXT,
    device_name TEXT,
    procedure_name TEXT,
    is_drug INTEGER DEFAULT 0,
    is_device INTEGER DEFAULT 0,
    is_procedure INTEGER DEFAULT 0,
    is_behavioral INTEGER DEFAULT 0,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
);

-- Fact table for clinical trials
CREATE TABLE IF NOT EXISTS fact_trials (
    trial_key INTEGER PRIMARY KEY AUTOINCREMENT,
    nct_id TEXT UNIQUE NOT NULL,
    
    -- Dimension keys
    sponsor_key INTEGER,
    location_key INTEGER,
    condition_key INTEGER,
    intervention_key INTEGER,
    start_date_key INTEGER,
    completion_date_key INTEGER,
    
    -- Trial attributes
    brief_title TEXT,
    official_title TEXT,
    phase TEXT,
    enrollment_count INTEGER,
    actual_enrollment INTEGER,
    status TEXT,
    study_type TEXT,
    allocation TEXT,
    intervention_model TEXT,
    primary_purpose TEXT,
    masking_info TEXT,
    
    -- Calculated measures
    duration_days INTEGER,
    enrollment_target_met INTEGER,
    is_completed INTEGER,
    is_terminated INTEGER,
    is_recruiting INTEGER,
    
    -- Quality metrics
    data_completeness_score REAL,
    data_quality_score REAL,
    
    -- Metadata
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_date TEXT DEFAULT CURRENT_TIMESTAMP,
    source_system TEXT DEFAULT 'ClinicalTrials.gov',
    etl_batch_id TEXT
);

-- Staging tables for transformations
CREATE TABLE IF NOT EXISTS stg_sponsors (
    sponsor_id TEXT PRIMARY KEY,
    sponsor_name TEXT NOT NULL,
    sponsor_class TEXT,
    sponsor_type TEXT,
    country TEXT,
    state TEXT,
    city TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_locations (
    location_id TEXT PRIMARY KEY,
    country TEXT NOT NULL,
    state TEXT,
    city TEXT,
    facility_name TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_conditions (
    condition_id TEXT PRIMARY KEY,
    condition_name TEXT NOT NULL,
    condition_category TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_interventions (
    intervention_id TEXT PRIMARY KEY,
    intervention_name TEXT NOT NULL,
    intervention_type TEXT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_trials (
    nct_id TEXT PRIMARY KEY,
    sponsor_id TEXT,
    location_id TEXT,
    condition_id TEXT,
    intervention_id TEXT,
    start_date_key INTEGER,
    completion_date_key INTEGER,
    brief_title TEXT,
    official_title TEXT,
    phase TEXT,
    enrollment_count INTEGER,
    status TEXT,
    study_type TEXT,
    allocation TEXT,
    intervention_model TEXT,
    primary_purpose TEXT,
    masking_info TEXT,
    duration_days INTEGER,
    data_completeness_score REAL,
    data_quality_score REAL,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_trials_nct_id ON raw_trials(nct_id);
CREATE INDEX IF NOT EXISTS idx_raw_trials_sponsor ON raw_trials(lead_sponsor_name);
CREATE INDEX IF NOT EXISTS idx_raw_trials_condition ON raw_trials(condition);
CREATE INDEX IF NOT EXISTS idx_raw_trials_status ON raw_trials(status);
CREATE INDEX IF NOT EXISTS idx_raw_trials_extraction_date ON raw_trials(extraction_date);

CREATE INDEX IF NOT EXISTS idx_fact_trials_nct_id ON fact_trials(nct_id);
CREATE INDEX IF NOT EXISTS idx_fact_trials_sponsor ON fact_trials(sponsor_key);
CREATE INDEX IF NOT EXISTS idx_fact_trials_location ON fact_trials(location_key);
CREATE INDEX IF NOT EXISTS idx_fact_trials_condition ON fact_trials(condition_key);
CREATE INDEX IF NOT EXISTS idx_fact_trials_status ON fact_trials(status);
CREATE INDEX IF NOT EXISTS idx_fact_trials_start_date ON fact_trials(start_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_trials_completion_date ON fact_trials(completion_date_key);

CREATE INDEX IF NOT EXISTS idx_dim_sponsor_name ON dim_sponsor(sponsor_name);
CREATE INDEX IF NOT EXISTS idx_dim_location_country ON dim_location(country);
CREATE INDEX IF NOT EXISTS idx_dim_location_state ON dim_location(state);
CREATE INDEX IF NOT EXISTS idx_dim_condition_name ON dim_condition(condition_name);
CREATE INDEX IF NOT EXISTS idx_dim_intervention_name ON dim_intervention(intervention_name);

-- Comments for documentation
COMMENT ON TABLE raw_trials IS 'Raw clinical trials data from ClinicalTrials.gov API';
COMMENT ON TABLE dim_dates IS 'Date dimension for temporal analysis';
COMMENT ON TABLE dim_sponsor IS 'Trial sponsor dimension';
COMMENT ON TABLE dim_location IS 'Geographic location dimension';
COMMENT ON TABLE dim_condition IS 'Medical condition dimension';
COMMENT ON TABLE dim_intervention IS 'Intervention/treatment dimension';
COMMENT ON TABLE fact_trials IS 'Fact table with trial metrics and measures';

-- Grant permissions (adjust as needed)
GRANT USAGE ON SCHEMA staging TO public;
GRANT USAGE ON SCHEMA warehouse TO public;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO public;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA warehouse TO public; 