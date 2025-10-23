"""
Layer 1: Physical Map Discovery
Database and Schema Inventory Module

This module performs the initial survey of the database landscape,
discovering all databases, schemas, and their basic properties.
"""

import logging
import sys
import os
from typing import Dict, List, Any

# Handle relative imports for both package usage and direct execution
try:
    from ..core.database_connection import DatabaseConnection
    from ..core.utils import ArchaeologyReport
except ImportError:
    # Direct execution - add parent directories to path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from core.database_connection import DatabaseConnection
    from core.utils import ArchaeologyReport

logger = logging.getLogger(__name__)


class DatabaseInventory:
    """Database and schema discovery for the physical layer analysis."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def discover_databases(self, environment: str) -> Dict[str, Any]:
        """Discover all accessible databases in the environment."""
        logger.info(f"Starting database discovery for {environment}")
        
        query = """
        SELECT 
            datname as database_name,
            pg_size_pretty(pg_database_size(datname)) as database_size,
            pg_database_size(datname) as database_size_bytes,
            datcollate as collation,
            datctype as character_type,
            pg_encoding_to_char(encoding) as encoding
        FROM pg_database 
        WHERE datistemplate = false
        ORDER BY pg_database_size(datname) DESC
        """
        
        try:
            databases = self.db_connection.execute_query(environment, query)
            logger.info(f"Discovered {len(databases)} databases in {environment}")
            
            return {
                'environment': environment,
                'total_databases': len(databases),
                'databases': databases
            }
            
        except Exception as e:
            logger.error(f"Database discovery failed for {environment}: {e}")
            raise
    
    def discover_schemas(self, environment: str) -> Dict[str, Any]:
        """Discover all schemas in the current database."""
        logger.info(f"Starting schema discovery for {environment}")
        
        # System schemas to identify separately
        system_schemas = [
            'information_schema', 'pg_catalog', 'pg_toast',
            'pg_temp_1', 'pg_toast_temp_1'
        ]
        
        query = """
        SELECT 
            schema_name,
            schema_owner,
            CASE 
                WHEN schema_name = ANY(%s) THEN 'system'
                WHEN schema_name LIKE 'pg_%%' THEN 'system'
                ELSE 'user'
            END as schema_type
        FROM information_schema.schemata
        ORDER BY 
            CASE 
                WHEN schema_name = ANY(%s) THEN 2
                WHEN schema_name LIKE 'pg_%%' THEN 2
                ELSE 1
            END,
            schema_name
        """
        
        try:
            schemas = self.db_connection.execute_query(
                environment, 
                query, 
                (system_schemas, system_schemas)
            )
            
            # Categorize schemas
            user_schemas = [s for s in schemas if s['schema_type'] == 'user']
            system_schemas_found = [s for s in schemas 
                                  if s['schema_type'] == 'system']
            
            logger.info(f"Discovered {len(user_schemas)} user schemas and "
                       f"{len(system_schemas_found)} system schemas")
            
            return {
                'environment': environment,
                'total_schemas': len(schemas),
                'user_schemas': len(user_schemas),
                'system_schemas': len(system_schemas_found),
                'schema_details': {
                    'user_schemas': user_schemas,
                    'system_schemas': system_schemas_found
                }
            }
            
        except Exception as e:
            logger.error(f"Schema discovery failed for {environment}: {e}")
            raise
    
    def get_schema_table_counts(self, environment: str) -> Dict[str, Any]:
        """Get table counts per schema for initial sizing analysis."""
        logger.info(f"Analyzing schema table distribution for {environment}")
        
        query = """
        SELECT 
            table_schema,
            count(*) as table_count,
            count(CASE WHEN table_type = 'BASE TABLE' THEN 1 END) as base_tables,
            count(CASE WHEN table_type = 'VIEW' THEN 1 END) as views
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_schema NOT LIKE 'pg_%'
        GROUP BY table_schema
        ORDER BY table_count DESC
        """
        
        try:
            schema_stats = self.db_connection.execute_query(environment, query)
            
            total_tables = sum(s['table_count'] for s in schema_stats)
            total_base_tables = sum(s['base_tables'] for s in schema_stats)
            total_views = sum(s['views'] for s in schema_stats)
            
            logger.info(f"Found {total_tables} total objects: "
                       f"{total_base_tables} tables, {total_views} views")
            
            return {
                'environment': environment,
                'summary': {
                    'total_schemas_with_tables': len(schema_stats),
                    'total_table_objects': total_tables,
                    'total_base_tables': total_base_tables,
                    'total_views': total_views
                },
                'schema_breakdown': schema_stats
            }
            
        except Exception as e:
            logger.error(f"Schema table analysis failed for {environment}: {e}")
            raise
    
    def generate_inventory_report(self, environment: str) -> str:
        """Generate comprehensive database inventory report."""
        logger.info(f"Generating database inventory report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Gather all inventory data
            databases = self.discover_databases(environment)
            schemas = self.discover_schemas(environment)
            table_distribution = self.get_schema_table_counts(environment)
            
            # Add sections to report
            report.add_section('database_inventory', databases)
            report.add_section('schema_inventory', schemas)
            report.add_section('table_distribution', table_distribution)
            
            # Export report
            filename = report.export('layer1_database_inventory')
            logger.info(f"Database inventory report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Inventory report generation failed: {e}")
            raise


def main():
    """Command-line interface for database inventory discovery."""
    import sys
    
    # Handle relative imports for both package usage and direct execution
    try:
        from ..core.utils import setup_logging
    except ImportError:
        # Direct execution - add parent directories to path
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from core.utils import setup_logging
    
    setup_logging()
    
    if len(sys.argv) < 2:
        print("Usage: python -m data_archaeologist.layer1_physical.database_inventory <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        inventory = DatabaseInventory(db_conn)
        
        print(f"Starting database inventory for {environment}...")
        report_file = inventory.generate_inventory_report(environment)
        print(f"Inventory complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Database inventory failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
