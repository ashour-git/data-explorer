"""
Core Database Connection Module
Professional PostgreSQL connection management with enterprise security
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import json
import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import time

logger = logging.getLogger(__name__)

class DatabaseConnection:
    """Enterprise-grade database connection manager."""
    
    def __init__(self, config_path: str = 'config.json'):
        self.config_path = config_path
        self.environments = {}
        self.load_configuration()
    
    def load_configuration(self) -> None:
        """Load database environment configurations."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            self.environments = config.get('environments', {})
            logger.info(f"Loaded {len(self.environments)} database environments")
            
        except Exception as e:
            logger.error(f"Configuration loading failed: {e}")
            raise
    
    @contextmanager
    def get_connection(self, environment: str):
        """Context manager for database connections with automatic cleanup."""
        if environment not in self.environments:
            raise ValueError(f"Environment '{environment}' not found in configuration")
        
        env_config = self.environments[environment]
        conn = None
        
        try:
            # Build connection parameters
            conn_params = {
                'host': env_config['host'],
                'port': env_config['port'],
                'database': env_config['database'],
                'user': env_config['username'],
                'password': env_config['password']
            }
            
            # Add connection arguments if specified
            if 'connection_args' in env_config:
                conn_params.update(env_config['connection_args'])
            
            # Establish connection
            start_time = time.time()
            conn = psycopg2.connect(**conn_params)
            connect_time = time.time() - start_time
            
            logger.info(f"Connected to {environment} in {connect_time:.3f}s")
            yield conn
            
        except Exception as e:
            logger.error(f"Connection to {environment} failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug(f"Connection to {environment} closed")
    
    def test_connection(self, environment: str) -> Dict[str, Any]:
        """Test database connection and return basic information."""
        try:
            with self.get_connection(environment) as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                cursor.execute("""
                    SELECT 
                        current_database() as database_name,
                        current_user as connected_user,
                        version() as postgresql_version,
                        pg_size_pretty(pg_database_size(current_database())) as database_size
                """)
                
                result = cursor.fetchone()
                cursor.close()
                
                return {
                    'status': 'success',
                    'environment': environment,
                    'database_info': dict(result)
                }
                
        except Exception as e:
            return {
                'status': 'failed',
                'environment': environment,
                'error': str(e)
            }
    
    def get_available_environments(self) -> List[str]:
        """Get list of configured environments."""
        return list(self.environments.keys())
    
    def execute_query(self, environment: str, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Execute query and return results as list of dictionaries."""
        try:
            with self.get_connection(environment) as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                cursor.execute(query, params)
                results = [dict(row) for row in cursor.fetchall()]
                cursor.close()
                return results
                
        except Exception as e:
            logger.error(f"Query execution failed in {environment}: {e}")
            raise
