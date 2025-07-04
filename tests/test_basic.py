"""
Basic Tests for Clinical Trials Metadata Management System

Simple unit tests to verify core functionality.
"""

import pytest
import pandas as pd
from datetime import datetime, date
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from utils.helpers import (
    clean_string, parse_date, extract_location_info, normalize_sponsor_name,
    calculate_duration_days, generate_hash_key, categorize_condition,
    calculate_data_quality_score, extract_phase_number
)

class TestHelperFunctions:
    """Test helper utility functions."""
    
    def test_clean_string(self):
        """Test string cleaning function."""
        assert clean_string("  test  string  ") == "test string"
        assert clean_string("") is None
        assert clean_string(None) is None
        assert clean_string("   ") is None
    
    def test_parse_date(self):
        """Test date parsing function."""
        assert parse_date("2023-01-15") == date(2023, 1, 15)
        assert parse_date("01/15/2023") == date(2023, 1, 15)
        assert parse_date("invalid") is None
        assert parse_date(None) is None
    
    def test_extract_location_info(self):
        """Test location information extraction."""
        result = extract_location_info("New York, NY, USA")
        assert result['city'] == "New York"
        assert result['state'] == "NY"
        assert result['country'] == "USA"
        
        result = extract_location_info("London, UK")
        assert result['city'] == "London"
        assert result['state'] == "UK"
        assert result['country'] is None
    
    def test_normalize_sponsor_name(self):
        """Test sponsor name normalization."""
        assert normalize_sponsor_name("ABC Pharma Inc") == "ABC Pharma Inc."
        assert normalize_sponsor_name("XYZ University") == "XYZ University"
        assert normalize_sponsor_name("") is None
        assert normalize_sponsor_name(None) is None
    
    def test_calculate_duration_days(self):
        """Test duration calculation."""
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 15)
        assert calculate_duration_days(start_date, end_date) == 14
        assert calculate_duration_days(None, end_date) is None
        assert calculate_duration_days(start_date, None) is None
    
    def test_generate_hash_key(self):
        """Test hash key generation."""
        key1 = generate_hash_key("test", "value")
        key2 = generate_hash_key("test", "value")
        assert key1 == key2
        
        key3 = generate_hash_key("test", "different")
        assert key1 != key3
    
    def test_categorize_condition(self):
        """Test condition categorization."""
        assert categorize_condition("Breast Cancer") == "cancer"
        assert categorize_condition("Type 2 Diabetes") == "diabetes"
        assert categorize_condition("Heart Disease") == "cardiovascular"
        assert categorize_condition("Unknown Condition") == "Other"
    
    def test_extract_phase_number(self):
        """Test phase number extraction."""
        assert extract_phase_number("Phase 1") == 1
        assert extract_phase_number("Phase 2") == 2
        assert extract_phase_number("Phase 3") == 3
        assert extract_phase_number("No Phase") is None
        assert extract_phase_number(None) is None
    
    def test_calculate_data_quality_score(self):
        """Test data quality score calculation."""
        # Test with complete data
        df = pd.DataFrame({
            'col1': ['a', 'b', 'c'],
            'col2': [1, 2, 3],
            'col3': ['x', 'y', 'z']
        })
        
        quality = calculate_data_quality_score(df, required_columns=['col1', 'col2'])
        assert quality['total_rows'] == 3
        assert quality['quality_score'] > 0
        assert quality['completeness'] == 100.0
        
        # Test with missing data
        df_missing = pd.DataFrame({
            'col1': ['a', None, 'c'],
            'col2': [1, 2, None],
            'col3': ['x', 'y', 'z']
        })
        
        quality_missing = calculate_data_quality_score(df_missing, required_columns=['col1', 'col2'])
        assert quality_missing['quality_score'] < quality['quality_score']

class TestDataValidation:
    """Test data validation functions."""
    
    def test_required_fields_validation(self):
        """Test required fields validation."""
        # This would test the validation logic in the API client
        pass
    
    def test_date_validation(self):
        """Test date validation."""
        # This would test date validation logic
        pass
    
    def test_enrollment_validation(self):
        """Test enrollment count validation."""
        # This would test enrollment validation logic
        pass

if __name__ == "__main__":
    pytest.main([__file__]) 