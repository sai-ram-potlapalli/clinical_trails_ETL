"""
Configuration Management Module

Handles application configuration including database connections,
API settings, and environment-specific configurations.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class Config:
    """Configuration management class."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration from YAML file."""
        self.config_path = config_path or "config/database.yml"
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning(f"Config file {self.config_path} not found. Using defaults.")
                return self._get_default_config()
                
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            'database': {
                'type': 'sqlite',
                'filepath': 'clinical_trials.db'
            },
            'api': {
                'base_url': 'https://clinicaltrials.gov/api/v2/studies',
                'timeout': 30,
                'max_retries': 3,
                'batch_size': 1000
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'file': 'logs/clinical_trials.log'
            },
            'etl': {
                'batch_size': 1000,
                'max_workers': 4,
                'timeout': 300
            }
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get('database', {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration."""
        return self.config.get('api', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration."""
        return self.config.get('logging', {})
    
    def get_etl_config(self) -> Dict[str, Any]:
        """Get ETL configuration."""
        return self.config.get('etl', {})
    
    def get_connection_string(self) -> str:
        """Get database connection string."""
        db_config = self.get_database_config()
        
        if db_config.get('type') == 'sqlite':
            return self._get_sqlite_connection_string(db_config)
        elif db_config.get('type') == 'snowflake':
            return self._get_snowflake_connection_string(db_config)
        else:
            return self._get_postgresql_connection_string(db_config)
    
    def _get_postgresql_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Get PostgreSQL connection string."""
        return (
            f"postgresql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
    
    def _get_snowflake_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Get Snowflake connection string."""
        return (
            f"snowflake://{db_config['username']}:{db_config['password']}"
            f"@{db_config['account']}/{db_config['database']}/{db_config['schema']}"
        )
    
    def _get_sqlite_connection_string(self, db_config: Dict[str, Any]) -> str:
        """Get SQLite connection string."""
        filepath = db_config.get('filepath', 'clinical_trials.db')
        return f"sqlite:///{filepath}"
    
    def update_config(self, updates: Dict[str, Any]) -> None:
        """Update configuration with new values."""
        self.config.update(updates)
        
        # Save updated config to file
        try:
            config_file = Path(self.config_path)
            config_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(config_file, 'w') as file:
                yaml.dump(self.config, file, default_flow_style=False)
                
            logger.info(f"Configuration updated and saved to {self.config_path}")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")

# Global configuration instance
config = Config() 