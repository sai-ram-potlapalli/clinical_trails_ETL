-- Warehouse Transformations (SQLite compatible)
-- Load data from staging tables to final dimensional warehouse tables

-- Clear warehouse tables
DELETE FROM dim_sponsor;
DELETE FROM dim_location;
DELETE FROM dim_condition;
DELETE FROM dim_intervention;
DELETE FROM fact_trials;

-- Load sponsor dimension
INSERT INTO dim_sponsor (
    sponsor_id,
    sponsor_name,
    sponsor_class,
    sponsor_type,
    is_industry,
    is_academic,
    is_government,
    country,
    state,
    city,
    created_date,
    updated_date,
    is_active
)
SELECT 
    s.sponsor_id,
    s.sponsor_name,
    s.sponsor_class,
    s.sponsor_type,
    CASE WHEN s.sponsor_type = 'Industry' THEN 1 ELSE 0 END as is_industry,
    CASE WHEN s.sponsor_type = 'Academic' THEN 1 ELSE 0 END as is_academic,
    CASE WHEN s.sponsor_type = 'Government' THEN 1 ELSE 0 END as is_government,
    s.country,
    s.state,
    s.city,
    s.created_date,
    CURRENT_TIMESTAMP as updated_date,
    1 as is_active
FROM stg_sponsors s;

-- Load location dimension
INSERT INTO dim_location (
    location_id,
    country,
    state,
    city,
    facility_name,
    region,
    continent,
    latitude,
    longitude,
    timezone,
    created_date,
    updated_date,
    is_active
)
SELECT 
    l.location_id,
    l.country,
    l.state,
    l.city,
    l.facility_name,
    CASE 
        WHEN l.country IN ('United States', 'Canada', 'Mexico') THEN 'North America'
        WHEN l.country IN ('United Kingdom', 'Germany', 'France', 'Italy', 'Spain') THEN 'Europe'
        WHEN l.country IN ('China', 'Japan', 'India', 'South Korea') THEN 'Asia'
        WHEN l.country IN ('Brazil', 'Argentina', 'Chile') THEN 'South America'
        WHEN l.country IN ('Australia', 'New Zealand') THEN 'Oceania'
        WHEN l.country IN ('South Africa', 'Nigeria', 'Kenya') THEN 'Africa'
        ELSE 'Other'
    END as region,
    CASE 
        WHEN l.country IN ('United States', 'Canada', 'Mexico') THEN 'North America'
        WHEN l.country IN ('United Kingdom', 'Germany', 'France', 'Italy', 'Spain') THEN 'Europe'
        WHEN l.country IN ('China', 'Japan', 'India', 'South Korea') THEN 'Asia'
        WHEN l.country IN ('Brazil', 'Argentina', 'Chile') THEN 'South America'
        WHEN l.country IN ('Australia', 'New Zealand') THEN 'Oceania'
        WHEN l.country IN ('South Africa', 'Nigeria', 'Kenya') THEN 'Africa'
        ELSE 'Other'
    END as continent,
    NULL as latitude,
    NULL as longitude,
    CASE 
        WHEN l.country = 'United States' THEN 'America/New_York'
        WHEN l.country = 'United Kingdom' THEN 'Europe/London'
        WHEN l.country = 'Germany' THEN 'Europe/Berlin'
        WHEN l.country = 'France' THEN 'Europe/Paris'
        WHEN l.country = 'China' THEN 'Asia/Shanghai'
        WHEN l.country = 'Japan' THEN 'Asia/Tokyo'
        ELSE 'UTC'
    END as timezone,
    l.created_date,
    CURRENT_TIMESTAMP as updated_date,
    1 as is_active
FROM stg_locations l;

-- Load condition dimension
INSERT INTO dim_condition (
    condition_id,
    condition_name,
    condition_category,
    condition_type,
    icd_code,
    mesh_term,
    is_rare,
    prevalence_category,
    created_date,
    updated_date,
    is_active
)
SELECT 
    c.condition_id,
    c.condition_name,
    c.condition_category,
    CASE 
        WHEN c.condition_category = 'Cancer' THEN 'Oncological'
        WHEN c.condition_category = 'Cardiovascular' THEN 'Cardiovascular'
        WHEN c.condition_category = 'Diabetes' THEN 'Metabolic'
        WHEN c.condition_category = 'Respiratory' THEN 'Pulmonary'
        WHEN c.condition_category = 'Neurological' THEN 'Neurological'
        WHEN c.condition_category = 'Mental Health' THEN 'Psychiatric'
        WHEN c.condition_category = 'Autoimmune' THEN 'Immunological'
        WHEN c.condition_category = 'Infectious Disease' THEN 'Infectious'
        ELSE 'Other'
    END as condition_type,
    NULL as icd_code,
    c.condition_name as mesh_term,
    CASE 
        WHEN c.condition_category IN ('Rare Disease', 'Orphan Disease') THEN 1
        ELSE 0
    END as is_rare,
    CASE 
        WHEN c.condition_category = 'Cancer' THEN 'High'
        WHEN c.condition_category = 'Diabetes' THEN 'High'
        WHEN c.condition_category = 'Cardiovascular' THEN 'High'
        WHEN c.condition_category = 'Mental Health' THEN 'Medium'
        ELSE 'Low'
    END as prevalence_category,
    c.created_date,
    CURRENT_TIMESTAMP as updated_date,
    1 as is_active
FROM stg_conditions c;

-- Load intervention dimension
INSERT INTO dim_intervention (
    intervention_id,
    intervention_name,
    intervention_type,
    intervention_category,
    drug_name,
    device_name,
    procedure_name,
    is_drug,
    is_device,
    is_procedure,
    is_behavioral,
    created_date,
    updated_date,
    is_active
)
SELECT 
    i.intervention_id,
    i.intervention_name,
    i.intervention_type,
    i.intervention_type as intervention_category,
    CASE WHEN i.intervention_type = 'Drug' THEN i.intervention_name ELSE NULL END as drug_name,
    CASE WHEN i.intervention_type = 'Device' THEN i.intervention_name ELSE NULL END as device_name,
    CASE WHEN i.intervention_type = 'Procedure' THEN i.intervention_name ELSE NULL END as procedure_name,
    CASE WHEN i.intervention_type = 'Drug' THEN 1 ELSE 0 END as is_drug,
    CASE WHEN i.intervention_type = 'Device' THEN 1 ELSE 0 END as is_device,
    CASE WHEN i.intervention_type = 'Procedure' THEN 1 ELSE 0 END as is_procedure,
    CASE WHEN i.intervention_type = 'Behavioral' THEN 1 ELSE 0 END as is_behavioral,
    i.created_date,
    CURRENT_TIMESTAMP as updated_date,
    1 as is_active
FROM stg_interventions i;

-- Load fact table
INSERT INTO fact_trials (
    nct_id,
    sponsor_key,
    location_key,
    condition_key,
    intervention_key,
    start_date_key,
    completion_date_key,
    brief_title,
    official_title,
    phase,
    enrollment_count,
    actual_enrollment,
    status,
    study_type,
    allocation,
    intervention_model,
    primary_purpose,
    masking_info,
    duration_days,
    enrollment_target_met,
    is_completed,
    is_terminated,
    is_recruiting,
    data_completeness_score,
    data_quality_score,
    created_date,
    updated_date,
    source_system,
    etl_batch_id
)
SELECT 
    st.nct_id,
    ds.sponsor_key,
    dl.location_key,
    dc.condition_key,
    di.intervention_key,
    st.start_date_key,
    st.completion_date_key,
    st.brief_title,
    st.official_title,
    st.phase,
    st.enrollment_count,
    st.enrollment_count as actual_enrollment,
    st.status,
    st.study_type,
    st.allocation,
    st.intervention_model,
    st.primary_purpose,
    st.masking_info,
    st.duration_days,
    CASE 
        WHEN st.enrollment_count IS NOT NULL AND st.enrollment_count > 0 THEN 1
        ELSE 0
    END as enrollment_target_met,
    CASE 
        WHEN LOWER(st.status) LIKE '%completed%' THEN 1
        ELSE 0
    END as is_completed,
    CASE 
        WHEN LOWER(st.status) LIKE '%terminated%' THEN 1
        ELSE 0
    END as is_terminated,
    CASE 
        WHEN LOWER(st.status) LIKE '%recruiting%' THEN 1
        ELSE 0
    END as is_recruiting,
    st.data_completeness_score,
    st.data_quality_score,
    CURRENT_TIMESTAMP as created_date,
    CURRENT_TIMESTAMP as updated_date,
    'ClinicalTrials.gov' as source_system,
    CURRENT_TIMESTAMP as etl_batch_id
FROM stg_trials st
LEFT JOIN dim_sponsor ds ON st.sponsor_id = ds.sponsor_id
LEFT JOIN dim_location dl ON st.location_id = dl.location_id
LEFT JOIN dim_condition dc ON st.condition_id = dc.condition_id
LEFT JOIN dim_intervention di ON st.intervention_id = di.intervention_id;

-- (Date dimension and procedural/statistics blocks omitted for SQLite compatibility) 