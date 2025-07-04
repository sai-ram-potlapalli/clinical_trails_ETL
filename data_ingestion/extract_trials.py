"""
Clinical Trials Data Extraction

Main script for extracting clinical trials data from ClinicalTrials.gov API
and loading it into raw database tables for further processing.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from data_ingestion.api_client import api_client
from utils.database import db_manager
from utils.logging import log_etl_job, ETLLogger
from utils.helpers import calculate_data_quality_score, save_json_data
from utils.config import config

logger = ETLLogger()

@log_etl_job("Extract Clinical Trials Data")
def extract_trials_data(
    search_terms: List[str] = None,
    conditions: List[str] = None,
    sponsors: List[str] = None,
    phases: List[str] = None,
    status: List[str] = None,
    start_date: str = None,
    end_date: str = None,
    limit: int = None,
    save_to_file: bool = True
) -> pd.DataFrame:
    """
    Extract clinical trials data from ClinicalTrials.gov API.
    
    Args:
        search_terms: List of search terms
        conditions: List of medical conditions
        sponsors: List of sponsor names
        phases: List of trial phases
        status: List of trial statuses
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        limit: Maximum number of trials to retrieve
        save_to_file: Whether to save raw data to file
    
    Returns:
        DataFrame with extracted trial data
    """
    logger.logger.info("Starting clinical trials data extraction")
    
    # Extract trials from API
    trials = []
    validation_results = []
    
    try:
        for trial in api_client.search_trials(
            search_terms=search_terms,
            conditions=conditions,
            sponsors=sponsors,
            phases=phases,
            status=status,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        ):
            # Validate trial data
            validation = api_client.validate_trial_data(trial)
            validation_results.append(validation)
            
            if validation:
                trials.append(validation)
            else:
                logger.logger.warning(f"Invalid trial {trial.get('NCTId', 'Unknown')}")
        
        logger.logger.info(f"Extracted {len(trials)} valid trials")
        
        # Convert to DataFrame
        df = pd.DataFrame(trials)
        
        # Add extraction metadata
        df['extraction_date'] = datetime.now()
        df['data_source'] = 'ClinicalTrials.gov API'
        df['validation_status'] = 'valid'
        
        # Save to file if requested
        if save_to_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"data/raw/trials_extraction_{timestamp}.json"
            
            extraction_data = {
                'metadata': {
                    'extraction_date': datetime.now().isoformat(),
                    'total_trials': len(trials),
                    'search_criteria': {
                        'search_terms': search_terms,
                        'conditions': conditions,
                        'sponsors': sponsors,
                        'phases': phases,
                        'status': status,
                        'start_date': start_date,
                        'end_date': end_date,
                        'limit': limit
                    }
                },
                'validation_summary': {
                    'total_processed': len(validation_results),
                    'valid_trials': len(trials),
                    'invalid_trials': len(validation_results) - len(trials),
                    'validation_rate': len(trials) / len(validation_results) * 100 if validation_results else 0
                },
                'trials': trials
            }
            
            save_json_data(extraction_data, filepath)
            logger.logger.info(f"Raw data saved to {filepath}")
        
        return df
        
    except Exception as e:
        logger.log_error(e, context="Data extraction")
        raise

@log_etl_job("Load Raw Trials Data")
def load_raw_data(df: pd.DataFrame) -> None:
    """
    Load raw trial data to database staging table.
    
    Args:
        df: DataFrame with trial data
    """
    logger.logger.info("Loading raw trial data to database")
    
    try:
        # Calculate data quality metrics
        quality_metrics = calculate_data_quality_score(
            df, 
            required_columns=['NCTId', 'BriefTitle']
        )
        
        logger.log_data_quality('raw_trials', quality_metrics)
        
        # Load to database
        db_manager.load_dataframe(
            df=df,
            table_name='raw_trials',
            if_exists='replace',
            index=False
        )
        
        # Create indexes for performance
        db_manager.create_index('raw_trials', 'NCTId')
        db_manager.create_index('raw_trials', 'LeadSponsorName')
        db_manager.create_index('raw_trials', 'Condition')
        
        logger.logger.info(f"Successfully loaded {len(df)} trials to raw_trials table")
        
    except Exception as e:
        logger.log_error(e, context="Raw data loading")
        raise

def extract_recent_trials(days: int = 30) -> pd.DataFrame:
    """
    Extract trials updated in the last N days.
    
    Args:
        days: Number of days to look back
    
    Returns:
        DataFrame with recent trial data
    """
    logger.logger.info(f"Extracting trials updated in the last {days} days")
    
    trials = []
    for trial in api_client.get_recent_trials(days=days):
        validation = api_client.validate_trial_data(trial)
        if validation['is_valid']:
            trials.append(trial)
    
    df = pd.DataFrame(trials)
    df['extraction_date'] = datetime.now()
    df['data_source'] = 'ClinicalTrials.gov API'
    df['validation_status'] = 'valid'
    
    logger.logger.info(f"Extracted {len(trials)} recent trials")
    return df

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Extract clinical trials data')
    parser.add_argument('--search-terms', nargs='+', help='Search terms')
    parser.add_argument('--conditions', nargs='+', help='Medical conditions')
    parser.add_argument('--sponsors', nargs='+', help='Sponsor names')
    parser.add_argument('--phases', nargs='+', help='Trial phases')
    parser.add_argument('--status', nargs='+', help='Trial statuses')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, help='Maximum number of trials')
    parser.add_argument('--recent', type=int, help='Extract trials from last N days')
    parser.add_argument('--save-file', action='store_true', help='Save raw data to file')
    parser.add_argument('--load-db', action='store_true', help='Load data to database')
    
    args = parser.parse_args()
    
    try:
        if args.recent:
            # Extract recent trials
            df = extract_recent_trials(days=args.recent)
        else:
            # Extract trials with specified criteria
            df = extract_trials_data(
                search_terms=args.search_terms,
                conditions=args.conditions,
                sponsors=args.sponsors,
                phases=args.phases,
                status=args.status,
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit,
                save_to_file=args.save_file
            )
        
        # Load to database if requested
        if args.load_db and not df.empty:
            load_raw_data(df)
        
        logger.logger.info("Data extraction completed successfully")
        
    except Exception as e:
        logger.log_error(e, context="Main extraction")
        sys.exit(1)
    finally:
        api_client.close()
        db_manager.close()

if __name__ == "__main__":
    main() 