"""
ClinicalTrials.gov API Client

Provides a robust client for extracting clinical trials data from the
ClinicalTrials.gov API v2 with proper error handling, rate limiting, and
data validation.
"""

import requests
import pandas as pd
import time
import json
from typing import Dict, Any, List, Optional, Generator
from datetime import datetime, timedelta
import logging
from urllib.parse import urlencode

from utils.config import config
from utils.logging import ETLLogger, log_etl_job
from utils.helpers import save_json_data, load_json_data

logger = ETLLogger()

class ClinicalTrialsAPIClient:
    """Client for ClinicalTrials.gov API v2."""
    
    def __init__(self):
        """Initialize API client."""
        self.api_config = config.get_api_config()
        self.base_url = self.api_config.get('base_url')
        self.timeout = self.api_config.get('timeout', 30)
        self.max_retries = self.api_config.get('max_retries', 3)
        self.batch_size = self.api_config.get('batch_size', 1000)
        
        # Rate limiting
        self.request_delay = 1.0  # seconds between requests
        self.last_request_time = 0
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ClinicalTrials-ETL/1.0 (Data Architecture Project)',
            'Accept': 'application/json'
        })
    
    def _rate_limit(self) -> None:
        """Implement rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make API request with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                # Debug: print the request URL and params
                logger.logger.debug(f"API Request: {self.base_url} | Params: {params}")
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.log_error(
                    e, 
                    context="API request", 
                    attempt=attempt + 1,
                    params=params
                )
                
                if attempt == self.max_retries - 1:
                    raise
                
                # Exponential backoff
                time.sleep(2 ** attempt)
        
        raise Exception("Max retries exceeded")
    
    def search_trials(self, 
                     search_terms: List[str] = None,
                     conditions: List[str] = None,
                     sponsors: List[str] = None,
                     phases: List[str] = None,
                     status: List[str] = None,
                     start_date: str = None,
                     end_date: str = None,
                     limit: int = None) -> Generator[Dict[str, Any], None, None]:
        """
        Search for clinical trials with various filters using API v2.
        """
        query_parts = []
        if search_terms:
            query_parts.extend(search_terms)
        if conditions:
            for condition in conditions:
                query_parts.append(condition)
        if sponsors:
            for sponsor in sponsors:
                query_parts.append(sponsor)
        if phases:
            for phase in phases:
                query_parts.append(phase)
        if status:
            for s in status:
                query_parts.append(s)
        if start_date:
            query_parts.append(f'startDate:[{start_date} TO *]')
        if end_date:
            query_parts.append(f'completionDate:[* TO {end_date}]')
        query_term = ' '.join(query_parts) if query_parts else ''

        page_token = None
        trials_fetched = 0
        while True:
            params = {
                'query.term': query_term,
                'pageSize': min(self.batch_size, (limit - trials_fetched) if limit else self.batch_size)
            }
            if page_token:
                params['pageToken'] = page_token
            response = self._make_request(params)
            studies = response.get('studies', [])
            if not studies:
                break
            for study in studies:
                if limit and trials_fetched >= limit:
                    return
                trial_data = self._transform_study_data(study)
                yield trial_data
                trials_fetched += 1
            page_token = response.get('nextPageToken')
            if not page_token or (limit and trials_fetched >= limit):
                break
    
    def _transform_study_data(self, study: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform API v2 study data to the expected format.
        """
        try:
            protocol = study.get('protocolSection', {})
            identification = protocol.get('identificationModule', {})
            description = protocol.get('descriptionModule', {})
            sponsor = protocol.get('sponsorCollaboratorsModule', {})
            conditions = protocol.get('conditionsModule', {})
            interventions = protocol.get('armsInterventionsModule', {})
            design = protocol.get('designModule', {})
            enrollment = protocol.get('enrollmentModule', {})
            dates = protocol.get('statusModule', {})
            locations = protocol.get('contactsLocationsModule', {})

            # Parse enrollment count as integer if possible
            raw_enrollment = enrollment.get('enrollmentCount')
            if isinstance(raw_enrollment, str):
                try:
                    enrollment_count = int(raw_enrollment.replace(',', '').strip())
                except Exception:
                    enrollment_count = None
            elif isinstance(raw_enrollment, (int, float)):
                enrollment_count = int(raw_enrollment)
            else:
                enrollment_count = None

            # Transform to expected format
            transformed = {
                'NCTId': identification.get('nctId'),
                'BriefTitle': description.get('briefSummary'),
                'OfficialTitle': description.get('officialTitle'),
                'LeadSponsorName': sponsor.get('leadSponsor', {}).get('name'),
                'LeadSponsorClass': sponsor.get('leadSponsor', {}).get('class'),
                'Condition': '; '.join(conditions.get('conditions', [])),
                'InterventionName': '; '.join([
                    intervention.get('name', '') 
                    for intervention in interventions.get('interventions', [])
                ]),
                'InterventionType': '; '.join([
                    intervention.get('type', '') 
                    for intervention in interventions.get('interventions', [])
                ]),
                'Phase': '; '.join(design.get('phases', [])),
                'EnrollmentCount': enrollment_count,
                'StudyStartDate': dates.get('startDateStruct', {}).get('date'),
                'PrimaryCompletionDate': dates.get('primaryCompletionDateStruct', {}).get('date'),
                'StudyCompletionDate': dates.get('completionDateStruct', {}).get('date'),
                'Status': dates.get('overallStatus'),
                'LocationCountry': '; '.join([
                    location.get('country', '') 
                    for location in locations.get('locations', [])
                ]),
                'LocationState': '; '.join([
                    location.get('state', '') 
                    for location in locations.get('locations', [])
                ]),
                'LocationCity': '; '.join([
                    location.get('city', '') 
                    for location in locations.get('locations', [])
                ]),
                'LocationFacility': '; '.join([
                    location.get('facility', '') 
                    for location in locations.get('locations', [])
                ]),
                'StudyType': design.get('studyType'),
                'Allocation': design.get('allocation'),
                'InterventionModel': design.get('interventionModel'),
                'PrimaryPurpose': design.get('primaryPurpose'),
                'MaskingInfo': design.get('maskingInfo', {}).get('masking'),
                'OutcomeMeasureDescription': '; '.join([
                    outcome.get('description', '') 
                    for outcome in protocol.get('outcomesModule', {}).get('outcomes', [])
                ])
            }
            return transformed
        except Exception as e:
            logger.log_error(e, context="Transform study data", study_id=study.get('nctId', 'unknown'))
            return {}
    
    def get_recent_trials(self, days: int = 30) -> Generator[Dict[str, Any], None, None]:
        """Get trials updated in the last N days."""
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        return self.search_trials(start_date=start_date)
    
    def get_trial_details(self, nct_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific trial."""
        params = {
            'query.term': f'nctId:"{nct_id}"',
            'fields': '*'
        }
        
        response = self._make_request(params)
        studies = response.get('studies', [])
        
        if studies:
            return self._transform_study_data(studies[0])
        
        return {}
    
    def save_search_results(self, 
                           trials: List[Dict[str, Any]], 
                           filepath: str) -> None:
        """Save search results to JSON file."""
        save_json_data(trials, filepath)
        logger.logger.info(f"Saved {len(trials)} trials to {filepath}")
    
    def load_search_results(self, filepath: str) -> List[Dict[str, Any]]:
        """Load search results from JSON file."""
        return load_json_data(filepath)
    
    def validate_trial_data(self, trial: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean trial data.
        """
        validated = {}
        required_fields = ['NCTId']
        for field in required_fields:
            if not trial.get(field):
                logger.logger.warning(f"Missing required field: {field}")
                return {}
        for key, value in trial.items():
            if key == 'EnrollmentCount':
                # Ensure EnrollmentCount is int or None
                if value is None or value == '' or (isinstance(value, str) and not value.strip()):
                    validated[key] = None
                else:
                    try:
                        validated[key] = int(str(value).replace(',', '').strip())
                    except Exception:
                        validated[key] = None
            elif value is None:
                validated[key] = None
            elif isinstance(value, str):
                cleaned = value.strip()
                validated[key] = cleaned if cleaned else None
            elif isinstance(value, (int, float)):
                if value < 0:
                    validated[key] = None
                else:
                    validated[key] = value
            else:
                validated[key] = value
        return validated
    
    def close(self) -> None:
        """Close the API client session."""
        if self.session:
            self.session.close()
            logger.logger.info("API client session closed")

# Global API client instance
api_client = ClinicalTrialsAPIClient() 