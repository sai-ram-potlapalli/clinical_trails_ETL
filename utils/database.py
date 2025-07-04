"""
Database Connection Module

Provides database connection utilities for PostgreSQL and Snowflake,
with connection pooling and query execution capabilities.
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from typing import Dict, Any, List, Optional, Union
import logging
from contextlib import contextmanager

from .config import config
from .logging import ETLLogger

logger = ETLLogger()

class DatabaseManager:
    """Database connection and query management."""
    
    def __init__(self):
        """Initialize database manager."""
        self.engine = None
        self.session_factory = None
        self._setup_connection()
    
    def _setup_connection(self) -> None:
        """Setup database connection based on configuration."""
        db_config = config.get_database_config()
        db_type = db_config.get('type', 'sqlite')
        
        try:
            if db_type == 'sqlite':
                self._setup_sqlite_connection(db_config)
            elif db_type == 'snowflake':
                self._setup_snowflake_connection(db_config)
            else:
                self._setup_postgresql_connection(db_config)
                
            logger.logger.info(f"Database connection established: {db_type}")
        except Exception as e:
            logger.log_error(e, context="Database connection setup")
            raise
    
    def _setup_postgresql_connection(self, db_config: Dict[str, Any]) -> None:
        """Setup PostgreSQL connection."""
        connection_string = (
            f"postgresql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        self.engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        self.session_factory = sessionmaker(bind=self.engine)
    
    def _setup_snowflake_connection(self, db_config: Dict[str, Any]) -> None:
        """Setup Snowflake connection."""
        connection_string = (
            f"snowflake://{db_config['username']}:{db_config['password']}"
            f"@{db_config['account']}/{db_config['database']}/{db_config['schema']}"
        )
        
        self.engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
        
        self.session_factory = sessionmaker(bind=self.engine)
    
    def _setup_sqlite_connection(self, db_config: Dict[str, Any]) -> None:
        """Setup SQLite connection."""
        connection_string = (
            f"sqlite:///{db_config.get('filepath', 'clinical_trials.db')}"
        )
        self.engine = create_engine(
            connection_string,
            pool_pre_ping=True
        )
        self.session_factory = sessionmaker(bind=self.engine)
    
    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        try:
            with self.engine.connect() as connection:
                if params:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                logger.log_performance(
                    operation="query_execution",
                    duration=0,  # Would need to measure actual time
                    query_length=len(query),
                    result_rows=len(df)
                )
                return df
        except Exception as e:
            logger.log_error(e, context="Query execution", query=query)
            raise
    
    def execute_script(self, script_path: str) -> None:
        """Execute a SQL script file."""
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            
            with self.engine.connect() as connection:
                for statement in script.split(';'):
                    stmt = statement.strip()
                    if stmt:
                        connection.execute(text(stmt))
                connection.commit()
            
            logger.logger.info(f"SQL script executed: {script_path}")
        except Exception as e:
            logger.log_error(e, context="Script execution", script_path=script_path)
            raise
    
    def load_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'replace', index: bool = False,
                      method: str = 'multi') -> None:
        """Load DataFrame to database table."""
        try:
            start_time = pd.Timestamp.now()
            
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                method=method,
                chunksize=1000
            )
            
            duration = (pd.Timestamp.now() - start_time).total_seconds()
            logger.log_performance(
                operation="dataframe_load",
                duration=duration,
                table_name=table_name,
                row_count=len(df)
            )
            
            logger.logger.info(f"DataFrame loaded to {table_name}: {len(df):,} rows")
        except Exception as e:
            logger.log_error(e, context="DataFrame load", table_name=table_name)
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_name = :table_name)"
                ), {'table_name': table_name})
                return result.scalar()
        except Exception as e:
            logger.log_error(e, context="Table existence check", table_name=table_name)
            return False
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table information including row count and schema."""
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            count_result = self.execute_query(count_query)
            row_count = count_result.iloc[0]['row_count']
            
            # Get column information
            schema_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            schema_result = self.execute_query(schema_query)
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': schema_result.to_dict('records')
            }
        except Exception as e:
            logger.log_error(e, context="Table info retrieval", table_name=table_name)
            return {}
    
    def create_index(self, table_name: str, column_name: str, 
                    index_name: Optional[str] = None) -> None:
        """Create index on table column."""
        if not index_name:
            index_name = f"idx_{table_name}_{column_name}"
        
        try:
            index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
            with self.engine.connect() as connection:
                connection.execute(text(index_query))
                connection.commit()
            
            logger.logger.info(f"Index created: {index_name} on {table_name}.{column_name}")
        except Exception as e:
            logger.log_error(e, context="Index creation", 
                           table_name=table_name, column_name=column_name)
            raise
    
    def vacuum_table(self, table_name: str) -> None:
        """Vacuum table to reclaim storage and update statistics."""
        try:
            vacuum_query = f"VACUUM ANALYZE {table_name}"
            with self.engine.connect() as connection:
                connection.execute(text(vacuum_query))
                connection.commit()
            
            logger.logger.info(f"Table vacuumed: {table_name}")
        except Exception as e:
            logger.log_error(e, context="Table vacuum", table_name=table_name)
            # Don't raise for vacuum errors as they're not critical
    
    def close(self) -> None:
        """Close database connections."""
        if self.engine:
            self.engine.dispose()
            logger.logger.info("Database connections closed")

# Global database manager instance
db_manager = DatabaseManager() 