"""
Helper Utilities Module

Provides common helper functions for data processing, validation,
and utility operations used throughout the application.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, date
import re
import hashlib
from pathlib import Path
import json

def clean_string(value: str) -> str:
    """Clean and normalize string values."""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string and strip whitespace
    cleaned = str(value).strip()
    
    # Remove extra whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned if cleaned else None

def parse_date(date_str: str, formats: List[str] = None) -> Optional[date]:
    """Parse date string with multiple format support."""
    if pd.isna(date_str) or date_str is None:
        return None
    
    if formats is None:
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S'
        ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    
    return None

def extract_location_info(location_str: str) -> Dict[str, str]:
    """Extract city, state, and country from location string."""
    if pd.isna(location_str) or location_str is None:
        return {'city': None, 'state': None, 'country': None}
    
    location = clean_string(location_str)
    if not location:
        return {'city': None, 'state': None, 'country': None}
    
    # Common patterns for location parsing
    patterns = [
        # City, State, Country
        r'^([^,]+),\s*([^,]+),\s*([^,]+)$',
        # City, State
        r'^([^,]+),\s*([^,]+)$',
        # Just City
        r'^([^,]+)$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, location)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                return {
                    'city': clean_string(groups[0]),
                    'state': clean_string(groups[1]),
                    'country': clean_string(groups[2])
                }
            elif len(groups) == 2:
                return {
                    'city': clean_string(groups[0]),
                    'state': clean_string(groups[1]),
                    'country': None
                }
            else:
                return {
                    'city': clean_string(groups[0]),
                    'state': None,
                    'country': None
                }
    
    return {'city': location, 'state': None, 'country': None}

def normalize_sponsor_name(sponsor_name: str) -> str:
    """Normalize sponsor names for consistency."""
    if pd.isna(sponsor_name) or sponsor_name is None:
        return None
    
    sponsor = clean_string(sponsor_name)
    if not sponsor:
        return None
    
    # Common abbreviations and normalizations
    normalizations = {
        'inc': 'Inc.',
        'corp': 'Corp.',
        'llc': 'LLC',
        'ltd': 'Ltd.',
        'co': 'Co.',
        'university': 'University',
        'univ': 'University',
        'medical center': 'Medical Center',
        'med center': 'Medical Center',
        'hospital': 'Hospital',
        'hosp': 'Hospital'
    }
    
    # Apply normalizations
    for old, new in normalizations.items():
        sponsor = re.sub(rf'\b{old}\b', new, sponsor, flags=re.IGNORECASE)
    
    return sponsor

def calculate_duration_days(start_date: date, end_date: date) -> Optional[int]:
    """Calculate duration in days between two dates."""
    if pd.isna(start_date) or pd.isna(end_date):
        return None
    
    try:
        return (end_date - start_date).days
    except (TypeError, AttributeError):
        return None

def generate_hash_key(*values) -> str:
    """Generate a hash key from multiple values."""
    combined = '|'.join(str(v) for v in values if v is not None)
    return hashlib.md5(combined.encode()).hexdigest()

def validate_email(email: str) -> bool:
    """Validate email format."""
    if pd.isna(email) or email is None:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email)))

def extract_phase_number(phase_str: str) -> Optional[int]:
    """Extract phase number from phase string."""
    if pd.isna(phase_str) or phase_str is None:
        return None
    
    # Look for phase numbers in the string
    match = re.search(r'phase\s*(\d+)', str(phase_str), re.IGNORECASE)
    if match:
        return int(match.group(1))
    
    return None

def categorize_condition(condition_name: str) -> str:
    """Categorize medical conditions into broad categories."""
    if pd.isna(condition_name) or condition_name is None:
        return 'Unknown'
    
    condition = str(condition_name).lower()
    
    # Define condition categories
    categories = {
        'cancer': ['cancer', 'tumor', 'neoplasm', 'oncology', 'leukemia', 'lymphoma'],
        'cardiovascular': ['heart', 'cardiac', 'cardiovascular', 'hypertension', 'stroke'],
        'diabetes': ['diabetes', 'diabetic', 'glucose', 'insulin'],
        'respiratory': ['asthma', 'copd', 'respiratory', 'lung', 'pulmonary'],
        'neurological': ['alzheimer', 'parkinson', 'neurological', 'brain', 'stroke'],
        'mental_health': ['depression', 'anxiety', 'mental', 'psychiatric', 'bipolar'],
        'infectious': ['infection', 'viral', 'bacterial', 'hiv', 'covid'],
        'autoimmune': ['arthritis', 'lupus', 'autoimmune', 'inflammatory'],
        'pediatric': ['pediatric', 'child', 'infant', 'neonatal'],
        'geriatric': ['elderly', 'geriatric', 'aging', 'senior']
    }
    
    for category, keywords in categories.items():
        if any(keyword in condition for keyword in keywords):
            return category
    
    return 'Other'

def calculate_data_quality_score(df: pd.DataFrame, 
                               required_columns: List[str] = None) -> Dict[str, Any]:
    """Calculate data quality metrics for a DataFrame."""
    total_rows = len(df)
    if total_rows == 0:
        return {
            'total_rows': 0,
            'quality_score': 0,
            'missing_values': {},
            'duplicate_rows': 0,
            'completeness': 0
        }
    
    # Calculate missing values
    missing_values = {}
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / total_rows) * 100
        missing_values[col] = {
            'count': missing_count,
            'percentage': round(missing_pct, 2)
        }
    
    # Calculate duplicate rows
    duplicate_rows = df.duplicated().sum()
    duplicate_pct = (duplicate_rows / total_rows) * 100
    
    # Calculate completeness for required columns
    completeness = 100
    if required_columns:
        required_completeness = []
        for col in required_columns:
            if col in df.columns:
                col_completeness = ((total_rows - df[col].isna().sum()) / total_rows) * 100
                required_completeness.append(col_completeness)
        
        if required_completeness:
            completeness = sum(required_completeness) / len(required_completeness)
    
    # Calculate overall quality score
    quality_score = max(0, 100 - (duplicate_pct * 2) - (100 - completeness))
    
    return {
        'total_rows': total_rows,
        'quality_score': round(quality_score, 2),
        'missing_values': missing_values,
        'duplicate_rows': duplicate_rows,
        'duplicate_percentage': round(duplicate_pct, 2),
        'completeness': round(completeness, 2)
    }

def save_json_data(data: Dict[str, Any], filepath: str) -> None:
    """Save data to JSON file with proper formatting."""
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def load_json_data(filepath: str) -> Dict[str, Any]:
    """Load data from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)

def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 1000) -> List[pd.DataFrame]:
    """Split DataFrame into chunks for processing."""
    return [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, returning default if denominator is zero."""
    try:
        if denominator == 0 or pd.isna(denominator):
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default

def format_number(value: Union[int, float], format_type: str = 'comma') -> str:
    """Format numbers for display."""
    if pd.isna(value):
        return 'N/A'
    
    if format_type == 'comma':
        return f"{value:,}"
    elif format_type == 'percent':
        return f"{value:.1f}%"
    elif format_type == 'currency':
        return f"${value:,.2f}"
    else:
        return str(value) 