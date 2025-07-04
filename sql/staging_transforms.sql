-- Staging Transformations (SQLite compatible)
-- Transform raw data into cleaned staging tables

-- Clear staging tables
DELETE FROM stg_sponsors;
DELETE FROM stg_locations;
DELETE FROM stg_conditions;
DELETE FROM stg_interventions;
DELETE FROM stg_trials;

-- Transform sponsors
INSERT INTO stg_sponsors (
    sponsor_id,
    sponsor_name,
    sponsor_class,
    sponsor_type,
    country,
    state,
    city,
    created_date
)
SELECT DISTINCT
    -- Generate unique sponsor ID (deterministic, not cryptographic)
    CASE 
        WHEN LeadSponsorName IS NOT NULL THEN 
            'SPONSOR_' || lower(replace(LeadSponsorName,' ','_'))
        ELSE 'SPONSOR_UNKNOWN'
    END as sponsor_id,
    
    -- Clean sponsor name
    COALESCE(TRIM(LeadSponsorName), 'Unknown Sponsor') as sponsor_name,
    
    -- Clean sponsor class
    CASE 
        WHEN LeadSponsorClass IS NULL OR LeadSponsorClass = '' THEN 'Unknown'
        ELSE TRIM(LeadSponsorClass)
    END as sponsor_class,
    
    -- Determine sponsor type
    CASE 
        WHEN lower(LeadSponsorClass) LIKE '%industry%' THEN 'Industry'
        WHEN lower(LeadSponsorClass) LIKE '%university%' OR lower(LeadSponsorClass) LIKE '%academic%' THEN 'Academic'
        WHEN lower(LeadSponsorClass) LIKE '%government%' OR lower(LeadSponsorClass) LIKE '%nih%' THEN 'Government'
        WHEN lower(LeadSponsorClass) LIKE '%hospital%' OR lower(LeadSponsorClass) LIKE '%medical%' THEN 'Medical Center'
        ELSE 'Other'
    END as sponsor_type,
    
    -- Extract country from sponsor name (basic logic)
    CASE 
        WHEN lower(LeadSponsorName) LIKE '%usa%' OR lower(LeadSponsorName) LIKE '%united states%' THEN 'United States'
        WHEN lower(LeadSponsorName) LIKE '%canada%' THEN 'Canada'
        WHEN lower(LeadSponsorName) LIKE '%uk%' OR lower(LeadSponsorName) LIKE '%united kingdom%' THEN 'United Kingdom'
        WHEN lower(LeadSponsorName) LIKE '%germany%' THEN 'Germany'
        WHEN lower(LeadSponsorName) LIKE '%france%' THEN 'France'
        WHEN lower(LeadSponsorName) LIKE '%japan%' THEN 'Japan'
        WHEN lower(LeadSponsorName) LIKE '%china%' THEN 'China'
        ELSE 'Unknown'
    END as country,
    NULL as state,
    NULL as city,
    CURRENT_TIMESTAMP as created_date
FROM raw_trials
WHERE LeadSponsorName IS NOT NULL
  AND LeadSponsorName != ''
  AND validation_status = 'valid';

-- Transform locations
WITH locs AS (
    SELECT DISTINCT
        CASE 
            WHEN LocationCountry IS NOT NULL OR LocationState IS NOT NULL OR LocationCity IS NOT NULL THEN
                'LOC_' || lower(replace(COALESCE(LocationCountry,'') || '_' || COALESCE(LocationState,'') || '_' || COALESCE(LocationCity,''),' ','_'))
            ELSE 'LOC_UNKNOWN'
        END as base_location_id,
        COALESCE(TRIM(LocationCountry), 'Unknown') as country,
        CASE WHEN LocationState IS NULL OR LocationState = '' THEN NULL ELSE TRIM(LocationState) END as state,
        CASE WHEN LocationCity IS NULL OR LocationCity = '' THEN NULL ELSE TRIM(LocationCity) END as city,
        CASE WHEN LocationFacility IS NULL OR LocationFacility = '' THEN NULL ELSE TRIM(LocationFacility) END as facility_name,
        CURRENT_TIMESTAMP as created_date
    FROM raw_trials
    WHERE ((LocationCountry IS NOT NULL AND LocationCountry != '')
       OR (LocationState IS NOT NULL AND LocationState != '')
       OR (LocationCity IS NOT NULL AND LocationCity != ''))
      AND validation_status = 'valid'
),
locs_numbered AS (
    SELECT *,
           ROW_NUMBER() OVER () as rn
    FROM locs
)
INSERT OR IGNORE INTO stg_locations (
    location_id,
    country,
    state,
    city,
    facility_name,
    created_date
)
SELECT 
    CASE WHEN base_location_id = 'LOC_UNKNOWN' THEN base_location_id || '_' || rn ELSE base_location_id END,
    country, state, city, facility_name, created_date
FROM locs_numbered;

-- Transform conditions
WITH conds AS (
    SELECT DISTINCT
        CASE 
            WHEN Condition IS NOT NULL AND TRIM(Condition) != '' THEN 
                'COND_' || lower(replace(Condition,' ',''))
            ELSE 'COND_UNKNOWN'
        END as base_condition_id,
        COALESCE(TRIM(Condition), 'Unknown Condition') as condition_name,
        CASE 
            WHEN lower(Condition) LIKE '%cancer%' OR lower(Condition) LIKE '%tumor%' OR lower(Condition) LIKE '%oncology%' THEN 'Cancer'
            WHEN lower(Condition) LIKE '%diabetes%' OR lower(Condition) LIKE '%diabetic%' THEN 'Diabetes'
            WHEN lower(Condition) LIKE '%heart%' OR lower(Condition) LIKE '%cardiac%' OR lower(Condition) LIKE '%cardiovascular%' THEN 'Cardiovascular'
            WHEN lower(Condition) LIKE '%asthma%' OR lower(Condition) LIKE '%copd%' OR lower(Condition) LIKE '%respiratory%' THEN 'Respiratory'
            WHEN lower(Condition) LIKE '%alzheimer%' OR lower(Condition) LIKE '%parkinson%' OR lower(Condition) LIKE '%neurological%' THEN 'Neurological'
            WHEN lower(Condition) LIKE '%depression%' OR lower(Condition) LIKE '%anxiety%' OR lower(Condition) LIKE '%mental%' THEN 'Mental Health'
            WHEN lower(Condition) LIKE '%arthritis%' OR lower(Condition) LIKE '%lupus%' OR lower(Condition) LIKE '%autoimmune%' THEN 'Autoimmune'
            WHEN lower(Condition) LIKE '%infection%' OR lower(Condition) LIKE '%viral%' OR lower(Condition) LIKE '%bacterial%' THEN 'Infectious Disease'
            WHEN lower(Condition) LIKE '%pediatric%' OR lower(Condition) LIKE '%child%' OR lower(Condition) LIKE '%infant%' THEN 'Pediatric'
            WHEN lower(Condition) LIKE '%elderly%' OR lower(Condition) LIKE '%geriatric%' OR lower(Condition) LIKE '%aging%' THEN 'Geriatric'
            ELSE 'Other'
        END as condition_category,
        CURRENT_TIMESTAMP as created_date
    FROM raw_trials
    WHERE validation_status = 'valid'
),
conds_numbered AS (
    SELECT *, ROW_NUMBER() OVER () as rn FROM conds
)
INSERT OR IGNORE INTO stg_conditions (
    condition_id,
    condition_name,
    condition_category,
    created_date
)
SELECT 
    CASE WHEN base_condition_id = 'COND_UNKNOWN' THEN base_condition_id || '_' || rn ELSE base_condition_id END,
    condition_name, condition_category, created_date
FROM conds_numbered;

-- Transform interventions
WITH ints AS (
    SELECT DISTINCT
        CASE 
            WHEN InterventionName IS NOT NULL AND TRIM(InterventionName) != '' THEN 
                'INT_' || lower(replace(InterventionName,' ',''))
            ELSE 'INT_UNKNOWN'
        END as base_intervention_id,
        COALESCE(TRIM(InterventionName), 'Unknown Intervention') as intervention_name,
        CASE 
            WHEN InterventionType IS NULL OR InterventionType = '' THEN 'Unknown'
            ELSE TRIM(InterventionType)
        END as intervention_type,
        CURRENT_TIMESTAMP as created_date
    FROM raw_trials
    WHERE validation_status = 'valid'
),
ints_numbered AS (
    SELECT *, ROW_NUMBER() OVER () as rn FROM ints
)
INSERT OR IGNORE INTO stg_interventions (
    intervention_id,
    intervention_name,
    intervention_type,
    created_date
)
SELECT 
    CASE WHEN base_intervention_id = 'INT_UNKNOWN' THEN base_intervention_id || '_' || rn ELSE base_intervention_id END,
    intervention_name, intervention_type, created_date
FROM ints_numbered;

-- Transform trials with dimension keys
INSERT INTO stg_trials (
    nct_id,
    sponsor_id,
    location_id,
    condition_id,
    intervention_id,
    start_date_key,
    completion_date_key,
    brief_title,
    official_title,
    phase,
    enrollment_count,
    status,
    study_type,
    allocation,
    intervention_model,
    primary_purpose,
    masking_info,
    duration_days,
    data_completeness_score,
    data_quality_score,
    created_date
)
SELECT 
    NCTId,
    CASE WHEN LeadSponsorName IS NOT NULL THEN 'SPONSOR_' || lower(replace(LeadSponsorName,' ','_')) ELSE 'SPONSOR_UNKNOWN' END,
    CASE WHEN LocationCountry IS NOT NULL OR LocationState IS NOT NULL OR LocationCity IS NOT NULL THEN 'LOC_' || lower(replace(COALESCE(LocationCountry,'') || '_' || COALESCE(LocationState,'') || '_' || COALESCE(LocationCity,''),' ','_')) ELSE 'LOC_UNKNOWN' END,
    CASE WHEN Condition IS NOT NULL THEN 'COND_' || lower(replace(Condition,' ','')) ELSE 'COND_UNKNOWN' END,
    CASE WHEN InterventionName IS NOT NULL THEN 'INT_' || lower(replace(InterventionName,' ','')) ELSE 'INT_UNKNOWN' END,
    NULL as start_date_key, -- Date dimension not implemented in SQLite
    NULL as completion_date_key, -- Date dimension not implemented in SQLite
    CASE WHEN BriefTitle IS NULL OR BriefTitle = '' THEN NULL ELSE TRIM(BriefTitle) END as brief_title,
    CASE WHEN OfficialTitle IS NULL OR OfficialTitle = '' THEN NULL ELSE TRIM(OfficialTitle) END as official_title,
    CASE WHEN Phase IS NULL OR Phase = '' THEN 'Unknown' ELSE TRIM(Phase) END as phase,
    CASE WHEN EnrollmentCount IS NULL OR EnrollmentCount <= 0 THEN NULL ELSE EnrollmentCount END as enrollment_count,
    CASE WHEN Status IS NULL OR Status = '' THEN 'Unknown' ELSE TRIM(Status) END as status,
    CASE WHEN StudyType IS NULL OR StudyType = '' THEN 'Unknown' ELSE TRIM(StudyType) END as study_type,
    CASE WHEN Allocation IS NULL OR Allocation = '' THEN 'Unknown' ELSE TRIM(Allocation) END as allocation,
    CASE WHEN InterventionModel IS NULL OR InterventionModel = '' THEN 'Unknown' ELSE TRIM(InterventionModel) END as intervention_model,
    CASE WHEN PrimaryPurpose IS NULL OR PrimaryPurpose = '' THEN 'Unknown' ELSE TRIM(PrimaryPurpose) END as primary_purpose,
    CASE WHEN MaskingInfo IS NULL OR MaskingInfo = '' THEN 'Unknown' ELSE TRIM(MaskingInfo) END as masking_info,
    NULL as duration_days, -- Date math not implemented in SQLite
    CASE WHEN BriefTitle IS NOT NULL AND LeadSponsorName IS NOT NULL AND Condition IS NOT NULL THEN 100.0 WHEN BriefTitle IS NOT NULL OR LeadSponsorName IS NOT NULL OR Condition IS NOT NULL THEN 50.0 ELSE 0.0 END as data_completeness_score,
    85.0 as data_quality_score,
    CURRENT_TIMESTAMP as created_date
FROM raw_trials
WHERE validation_status = 'valid'
  AND NCTId IS NOT NULL; 