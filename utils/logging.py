"""
Logging Module

Provides structured logging with timestamps, performance tracking,
and ETL job monitoring capabilities.
"""

import logging
import logging.handlers
import time
import functools
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from datetime import datetime
import json

from .config import config

class ETLLogger:
    """Specialized logger for ETL operations with performance tracking."""
    
    def __init__(self, name: str = "etl_logger"):
        """Initialize ETL logger."""
        self.logger = logging.getLogger(name)
        self.setup_logging()
        
    def setup_logging(self) -> None:
        """Setup logging configuration."""
        log_config = config.get_logging_config()
        
        # Create logs directory
        log_file = Path(log_config.get('file', 'logs/clinical_trials.log'))
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        self.logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
        
        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler with rotation
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
    
    def log_etl_start(self, job_name: str, **kwargs) -> Dict[str, Any]:
        """Log the start of an ETL job."""
        start_time = time.time()
        job_info = {
            'job_name': job_name,
            'start_time': datetime.now().isoformat(),
            'start_timestamp': start_time,
            **kwargs
        }
        
        self.logger.info(f"ETL Job Started: {job_name}", extra={'job_info': job_info})
        return job_info
    
    def log_etl_end(self, job_info: Dict[str, Any], row_count: int = 0, **kwargs) -> None:
        """Log the end of an ETL job with performance metrics."""
        end_time = time.time()
        duration = end_time - job_info['start_timestamp']
        
        completion_info = {
            **job_info,
            'end_time': datetime.now().isoformat(),
            'duration_seconds': round(duration, 2),
            'row_count': row_count,
            'rows_per_second': round(row_count / duration, 2) if duration > 0 else 0,
            **kwargs
        }
        
        self.logger.info(
            f"ETL Job Completed: {job_info['job_name']} - "
            f"Duration: {duration:.2f}s, Rows: {row_count:,}",
            extra={'completion_info': completion_info}
        )
    
    def log_data_quality(self, table_name: str, quality_metrics: Dict[str, Any]) -> None:
        """Log data quality metrics."""
        self.logger.info(
            f"Data Quality Report - Table: {table_name}",
            extra={'quality_metrics': quality_metrics}
        )
    
    def log_error(self, error: Exception, context: str = "", **kwargs) -> None:
        """Log errors with context."""
        error_info = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'timestamp': datetime.now().isoformat(),
            **kwargs
        }
        
        self.logger.error(
            f"Error in {context}: {error}",
            extra={'error_info': error_info},
            exc_info=True
        )
    
    def log_performance(self, operation: str, duration: float, **kwargs) -> None:
        """Log performance metrics."""
        perf_info = {
            'operation': operation,
            'duration_seconds': round(duration, 3),
            'timestamp': datetime.now().isoformat(),
            **kwargs
        }
        
        self.logger.info(
            f"Performance: {operation} - {duration:.3f}s",
            extra={'performance_info': perf_info}
        )

def log_execution_time(func: Callable) -> Callable:
    """Decorator to log function execution time."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = ETLLogger()
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.log_performance(
                operation=f"{func.__module__}.{func.__name__}",
                duration=duration
            )
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.log_error(
                error=e,
                context=f"{func.__module__}.{func.__name__}",
                duration_seconds=duration
            )
            raise
    
    return wrapper

def log_etl_job(job_name: str):
    """Decorator to log ETL job execution with start/end tracking."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = ETLLogger()
            job_info = logger.log_etl_start(job_name)
            
            try:
                result = func(*args, **kwargs)
                
                # Try to extract row count from result if it's a DataFrame
                row_count = 0
                if hasattr(result, 'shape') and hasattr(result.shape, '__len__'):
                    row_count = result.shape[0]
                elif isinstance(result, dict) and 'row_count' in result:
                    row_count = result['row_count']
                
                logger.log_etl_end(job_info, row_count=row_count)
                return result
            except Exception as e:
                logger.log_error(e, context=job_name)
                logger.log_etl_end(job_info, error=str(e))
                raise
        
        return wrapper
    return decorator

# Global logger instance
etl_logger = ETLLogger() 