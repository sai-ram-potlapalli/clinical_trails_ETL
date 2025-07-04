"""
Clinical Trials Data Transformation

Module for cleaning, validating, and transforming raw clinical trials data
into the dimensional data warehouse structure.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, date
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from utils.database import db_manager
from utils.logging import log_etl_job, ETLLogger
from utils.helpers import (
    clean_string, parse_date, extract_location_info, normalize_sponsor_name,
    calculate_duration_days, generate_hash_key, categorize_condition,
    calculate_data_quality_score, extract_phase_number
)

logger = ETLLogger()

@log_etl_job("Transform Raw Trials Data")
def transform_raw_trials() -> pd.DataFrame:
    """
    Transform raw trial data into cleaned format for dimensional loading.
    
    Returns:
        DataFrame with cleaned and transformed trial data
    """
    logger.logger.info("Starting raw trials transformation")
    
    try:
        # Load raw data from database
        raw_query = """
        SELECT * FROM raw_trials 
        WHERE validation_status = 'valid'
        ORDER BY extraction_date DESC
        """
        raw_df = db_manager.execute_query(raw_query)
        
        if raw_df.empty:
            logger.logger.warning("No raw trials data found for transformation")
            return pd.DataFrame()
        
        # --- ADDED: Ensure status column exists ---
        if 'status' not in raw_df.columns:
            if 'Status' in raw_df.columns:
                raw_df['status'] = raw_df['Status']
            elif 'overall_status' in raw_df.columns:
                raw_df['status'] = raw_df['overall_status']
            elif 'trial_status' in raw_df.columns:
                raw_df['status'] = raw_df['trial_status']
            else:
                raw_df['status'] = ""
        
        # --- ADDED: Ensure phase column exists ---
        if 'phase' not in raw_df.columns:
            if 'Phase' in raw_df.columns:
                raw_df['phase'] = raw_df['Phase']
            elif 'study_phase' in raw_df.columns:
                raw_df['phase'] = raw_df['study_phase']
            else:
                raw_df['phase'] = ""
        
        # --- ADDED: Ensure location_country, location_state, location_city exist ---
        if 'location_country' not in raw_df.columns:
            # Try to parse from 'location' column if available
            if 'location' in raw_df.columns:
                loc_info = raw_df['location'].apply(extract_location_info)
                raw_df['location_country'] = loc_info.apply(lambda x: x['country'])
                raw_df['location_state'] = loc_info.apply(lambda x: x['state'])
                raw_df['location_city'] = loc_info.apply(lambda x: x['city'])
            else:
                # If no location info, create empty columns
                raw_df['location_country'] = None
                raw_df['location_state'] = None
                raw_df['location_city'] = None
        
        # --- ADDED: Ensure lead_sponsor_name exists ---
        if 'lead_sponsor_name' not in raw_df.columns:
            if 'LeadSponsorName' in raw_df.columns:
                raw_df['lead_sponsor_name'] = raw_df['LeadSponsorName']
            elif 'sponsor' in raw_df.columns:
                raw_df['lead_sponsor_name'] = raw_df['sponsor']
            else:
                raw_df['lead_sponsor_name'] = None
        
        # --- ADDED: Ensure lead_sponsor_class column exists ---
        if 'lead_sponsor_class' not in raw_df.columns:
            if 'LeadSponsorClass' in raw_df.columns:
                raw_df['lead_sponsor_class'] = raw_df['LeadSponsorClass']
            elif 'sponsor_class' in raw_df.columns:
                raw_df['lead_sponsor_class'] = raw_df['sponsor_class']
            else:
                raw_df['lead_sponsor_class'] = ""
        
        # --- ADDED: Ensure condition column exists ---
        if 'condition' not in raw_df.columns:
            if 'Condition' in raw_df.columns:
                raw_df['condition'] = raw_df['Condition']
            elif 'conditions' in raw_df.columns:
                raw_df['condition'] = raw_df['conditions']
            else:
                raw_df['condition'] = ""
        
        # --- ADDED: Ensure intervention_name column exists ---
        if 'intervention_name' not in raw_df.columns:
            if 'InterventionName' in raw_df.columns:
                raw_df['intervention_name'] = raw_df['InterventionName']
            elif 'intervention' in raw_df.columns:
                raw_df['intervention_name'] = raw_df['intervention']
            else:
                raw_df['intervention_name'] = ""
        
        # --- ADDED: Ensure intervention_type column exists ---
        if 'intervention_type' not in raw_df.columns:
            if 'InterventionType' in raw_df.columns:
                raw_df['intervention_type'] = raw_df['InterventionType']
            elif 'interventiontype' in raw_df.columns:
                raw_df['intervention_type'] = raw_df['interventiontype']
            else:
                raw_df['intervention_type'] = ""
        
        # --- ADDED: Ensure enrollment_count column exists ---
        if 'enrollment_count' not in raw_df.columns:
            if 'EnrollmentCount' in raw_df.columns:
                raw_df['enrollment_count'] = raw_df['EnrollmentCount']
            elif 'enrollment' in raw_df.columns:
                raw_df['enrollment_count'] = raw_df['enrollment']
            elif 'enrollment_total' in raw_df.columns:
                raw_df['enrollment_count'] = raw_df['enrollment_total']
            else:
                raw_df['enrollment_count'] = ""
        
        logger.logger.info(f"Loaded {len(raw_df)} raw trials for transformation")
        
        # Apply transformations
        transformed_df = apply_transformations(raw_df)
        
        # Calculate quality metrics
        quality_metrics = calculate_data_quality_score(
            transformed_df,
            required_columns=['nct_id', 'brief_title', 'lead_sponsor_name']
        )
        
        logger.log_data_quality('transformed_trials', quality_metrics)
        
        logger.logger.info(f"Transformation completed: {len(transformed_df)} trials processed")
        return transformed_df
        
    except Exception as e:
        logger.log_error(e, context="Raw trials transformation")
        raise

def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all data transformations to raw trial data.
    
    Args:
        df: Raw trial DataFrame
    
    Returns:
        Transformed DataFrame
    """
    logger.logger.info("Applying data transformations")
    
    # Create a copy to avoid modifying original
    transformed = df.copy()
    
    # Clean and standardize text fields
    transformed = clean_text_fields(transformed)
    
    # Parse and validate dates
    transformed = parse_date_fields(transformed)
    
    # Extract and clean location information
    transformed = process_location_data(transformed)
    
    # Normalize sponsor information
    transformed = process_sponsor_data(transformed)
    
    # Process condition data
    transformed = process_condition_data(transformed)
    
    # Process intervention data
    transformed = process_intervention_data(transformed)
    
    # Calculate derived fields
    transformed = calculate_derived_fields(transformed)
    
    # Add transformation metadata
    transformed['transformation_date'] = datetime.now()
    transformed['data_version'] = '1.0'
    
    return transformed

def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize text fields."""
    logger.logger.info("Cleaning text fields")
    
    text_columns = [
        'brief_title', 'official_title', 'lead_sponsor_name', 'lead_sponsor_class',
        'condition', 'intervention_name', 'intervention_type', 'phase', 'status',
        'location_country', 'location_state', 'location_city', 'location_facility',
        'study_type', 'allocation', 'intervention_model', 'primary_purpose',
        'masking_info', 'outcome_measure_description'
    ]
    
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_string)
    
    return df

def parse_date_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and validate date fields."""
    logger.logger.info("Parsing date fields")
    
    date_columns = {
        'study_start_date': 'study_start_date',
        'primary_completion_date': 'primary_completion_date',
        'study_completion_date': 'study_completion_date'
    }
    
    for col, new_col in date_columns.items():
        if col in df.columns:
            df[new_col] = df[col].apply(lambda x: parse_date(x) if pd.notna(x) else None)
    
    return df

def process_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean location data."""
    logger.logger.info("Processing location data")
    
    # Create location ID
    df['location_id'] = df.apply(
        lambda row: generate_hash_key(
            row.get('location_country', ''),
            row.get('location_state', ''),
            row.get('location_city', '')
        ),
        axis=1
    )
    
    # Add location metadata
    df['location_region'] = df['location_country'].apply(categorize_region)
    df['location_continent'] = df['location_country'].apply(categorize_continent)
    
    return df

def process_sponsor_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and normalize sponsor data."""
    logger.logger.info("Processing sponsor data")
    
    # Normalize sponsor names
    df['lead_sponsor_name'] = df['lead_sponsor_name'].apply(normalize_sponsor_name)
    
    # Create sponsor ID
    df['sponsor_id'] = df['lead_sponsor_name'].apply(
        lambda x: generate_hash_key(x) if pd.notna(x) else 'SPONSOR_UNKNOWN'
    )
    
    # Categorize sponsor type
    df['sponsor_type'] = df['lead_sponsor_class'].apply(categorize_sponsor_type)
    df['sponsor_category'] = df['lead_sponsor_name'].apply(categorize_sponsor_category)
    
    return df

def process_condition_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and categorize condition data."""
    logger.logger.info("Processing condition data")
    
    # Categorize conditions
    df['condition_category'] = df['condition'].apply(categorize_condition)
    
    # Create condition ID
    df['condition_id'] = df['condition'].apply(
        lambda x: generate_hash_key(x) if pd.notna(x) else 'COND_UNKNOWN'
    )
    
    return df

def process_intervention_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and categorize intervention data."""
    logger.logger.info("Processing intervention data")
    
    # Create intervention ID
    df['intervention_id'] = df['intervention_name'].apply(
        lambda x: generate_hash_key(x) if pd.notna(x) else 'INT_UNKNOWN'
    )
    
    # Categorize intervention type
    df['intervention_category'] = df['intervention_type'].apply(categorize_intervention_type)
    
    return df

def calculate_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived fields and metrics."""
    logger.logger.info("Calculating derived fields")
    
    # Calculate trial duration
    df['duration_days'] = df.apply(
        lambda row: calculate_duration_days(
            row.get('study_start_date'),
            row.get('study_completion_date')
        ),
        axis=1
    )
    
    # Extract phase number
    df['phase_number'] = df['phase'].apply(extract_phase_number)
    
    # Calculate enrollment metrics
    df['enrollment_category'] = df['enrollment_count'].apply(categorize_enrollment)
    
    # Add status flags
    df['is_completed'] = df['status'].apply(lambda x: 'completed' in str(x).lower() if pd.notna(x) else False)
    df['is_recruiting'] = df['status'].apply(lambda x: 'recruiting' in str(x).lower() if pd.notna(x) else False)
    df['is_terminated'] = df['status'].apply(lambda x: 'terminated' in str(x).lower() if pd.notna(x) else False)
    
    # Calculate data quality scores
    df['data_completeness_score'] = df.apply(calculate_completeness_score, axis=1)
    df['data_quality_score'] = df.apply(calculate_quality_score, axis=1)
    
    return df

def categorize_region(country: str) -> str:
    """Categorize country into region."""
    if pd.isna(country):
        return 'Unknown'
    
    country_lower = str(country).lower()
    
    regions = {
        'North America': ['united states', 'canada', 'mexico'],
        'Europe': ['united kingdom', 'germany', 'france', 'italy', 'spain', 'netherlands'],
        'Asia': ['china', 'japan', 'india', 'south korea', 'singapore'],
        'Latin America': ['brazil', 'argentina', 'chile', 'colombia'],
        'Africa': ['south africa', 'nigeria', 'kenya', 'egypt'],
        'Oceania': ['australia', 'new zealand']
    }
    
    for region, countries in regions.items():
        if any(c in country_lower for c in countries):
            return region
    
    return 'Other'

def categorize_continent(country: str) -> str:
    """Categorize country into continent."""
    if pd.isna(country):
        return 'Unknown'
    
    country_lower = str(country).lower()
    
    continents = {
        'North America': ['united states', 'canada', 'mexico'],
        'Europe': ['united kingdom', 'germany', 'france', 'italy', 'spain'],
        'Asia': ['china', 'japan', 'india', 'south korea'],
        'South America': ['brazil', 'argentina', 'chile'],
        'Africa': ['south africa', 'nigeria', 'kenya'],
        'Oceania': ['australia', 'new zealand']
    }
    
    for continent, countries in continents.items():
        if any(c in country_lower for c in countries):
            return continent
    
    return 'Other'

def categorize_sponsor_type(sponsor_class: str) -> str:
    """Categorize sponsor by type."""
    if pd.isna(sponsor_class):
        return 'Unknown'
    
    sponsor_lower = str(sponsor_class).lower()
    
    if 'industry' in sponsor_lower:
        return 'Industry'
    elif 'university' in sponsor_lower or 'academic' in sponsor_lower:
        return 'Academic'
    elif 'government' in sponsor_lower or 'nih' in sponsor_lower:
        return 'Government'
    elif 'hospital' in sponsor_lower or 'medical' in sponsor_lower:
        return 'Medical Center'
    else:
        return 'Other'

def categorize_sponsor_category(sponsor_name: str) -> str:
    """Categorize sponsor by category."""
    if pd.isna(sponsor_name):
        return 'Unknown'
    
    sponsor_lower = str(sponsor_name).lower()
    
    if any(word in sponsor_lower for word in ['pharma', 'biotech', 'pharmaceutical']):
        return 'Pharmaceutical'
    elif any(word in sponsor_lower for word in ['university', 'college', 'institute']):
        return 'Academic'
    elif any(word in sponsor_lower for word in ['hospital', 'medical center', 'clinic']):
        return 'Medical'
    elif any(word in sponsor_lower for word in ['government', 'national', 'federal']):
        return 'Government'
    else:
        return 'Other'

def categorize_intervention_type(intervention_type: str) -> str:
    """Categorize intervention type."""
    if pd.isna(intervention_type):
        return 'Unknown'
    
    intervention_lower = str(intervention_type).lower()
    
    if 'drug' in intervention_lower:
        return 'Drug'
    elif 'device' in intervention_lower:
        return 'Device'
    elif 'procedure' in intervention_lower or 'surgery' in intervention_lower:
        return 'Procedure'
    elif 'behavioral' in intervention_lower or 'lifestyle' in intervention_lower:
        return 'Behavioral'
    else:
        return 'Other'

def categorize_enrollment(enrollment: int) -> str:
    """Categorize enrollment count."""
    if pd.isna(enrollment) or enrollment <= 0:
        return 'Unknown'
    
    if enrollment <= 50:
        return 'Small (≤50)'
    elif enrollment <= 200:
        return 'Medium (51-200)'
    elif enrollment <= 1000:
        return 'Large (201-1000)'
    else:
        return 'Very Large (>1000)'

def calculate_completeness_score(row: pd.Series) -> float:
    """Calculate data completeness score for a trial."""
    required_fields = ['nct_id', 'brief_title', 'lead_sponsor_name', 'condition']
    optional_fields = ['enrollment_count', 'phase', 'status', 'study_start_date']
    
    required_score = sum(1 for field in required_fields if pd.notna(row.get(field, None))) / len(required_fields)
    optional_score = sum(1 for field in optional_fields if pd.notna(row.get(field, None))) / len(optional_fields)
    
    # Weight required fields more heavily
    return (required_score * 0.7) + (optional_score * 0.3)

def calculate_quality_score(row: pd.Series) -> float:
    """Calculate overall data quality score for a trial."""
    completeness_score = row.get('data_completeness_score', 0)
    
    # Additional quality factors
    quality_factors = []
    
    # Check for valid dates
    if pd.notna(row.get('study_start_date')) and pd.notna(row.get('study_completion_date')):
        if row['study_start_date'] < row['study_completion_date']:
            quality_factors.append(1.0)
        else:
            quality_factors.append(0.5)
    else:
        quality_factors.append(0.0)
    
    # Check for reasonable enrollment count
    enrollment = row.get('enrollment_count')
    if pd.notna(enrollment) and enrollment > 0 and enrollment <= 100000:
        quality_factors.append(1.0)
    else:
        quality_factors.append(0.5)
    
    # Check for valid phase
    if pd.notna(row.get('phase')) and str(row['phase']).lower() != 'unknown':
        quality_factors.append(1.0)
    else:
        quality_factors.append(0.5)
    
    # Calculate average quality factor
    avg_quality_factor = sum(quality_factors) / len(quality_factors) if quality_factors else 0.5
    
    # Combine completeness and quality factors
    return (completeness_score * 0.6) + (avg_quality_factor * 0.4)

def main():
    """Main transformation function."""
    try:
        # Transform raw data
        transformed_df = transform_raw_trials()
        
        if not transformed_df.empty:
            logger.logger.info("Data transformation completed successfully")
        else:
            logger.logger.warning("No data to transform")
            
    except Exception as e:
        logger.log_error(e, context="Main transformation")
        raise
    finally:
        db_manager.close()

if __name__ == "__main__":
    main() 