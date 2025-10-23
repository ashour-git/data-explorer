"""
Layer 1: Physical Map Discovery
Column Profiling and Data Quality Analysis Module

This module performs detailed column-level analysis including data types,
memory consumption, NULL percentages, and initial data quality assessment.
"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional

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


class ColumnProfiler:
    """Column profiling and data quality analysis for physical layer discovery."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def profile_table_columns(self, environment: str, 
                             schema_name: str, table_name: str) -> Dict[str, Any]:
        """Profile all columns in a specific table."""
        logger.info(f"Profiling columns for {schema_name}.{table_name} in {environment}")
        
        # Get column metadata
        metadata_query = """
        SELECT 
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type,
            character_maximum_length,
            character_octet_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            udt_name as user_defined_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        
        try:
            columns_metadata = self.db_connection.execute_query(
                environment, metadata_query, (schema_name, table_name)
            )
            
            # Get table row count for percentage calculations
            count_query = f"""
            SELECT count(*) as total_rows 
            FROM "{schema_name}"."{table_name}"
            """
            
            total_rows_result = self.db_connection.execute_query(environment, count_query)
            total_rows = total_rows_result[0]['total_rows'] if total_rows_result else 0
            
            # Profile each column
            column_profiles = []
            
            for col in columns_metadata:
                column_name = col['column_name']
                
                # Get NULL count and basic statistics
                profile_query = f"""
                SELECT 
                    count(*) as total_count,
                    count("{column_name}") as non_null_count,
                    count(*) - count("{column_name}") as null_count,
                    count(DISTINCT "{column_name}") as distinct_count
                FROM "{schema_name}"."{table_name}"
                """
                
                profile_result = self.db_connection.execute_query(environment, profile_query)
                profile_stats = profile_result[0] if profile_result else {}
                
                # Calculate percentages
                null_percentage = calculate_null_percentage(
                    profile_stats.get('null_count', 0), 
                    total_rows
                )
                
                distinct_percentage = (
                    (profile_stats.get('distinct_count', 0) / total_rows * 100) 
                    if total_rows > 0 else 0
                )
                
                # Determine column characteristics
                column_characteristics = self._analyze_column_characteristics(
                    col, profile_stats, total_rows
                )
                
                column_profile = {
                    **col,
                    'statistics': profile_stats,
                    'null_percentage': round(null_percentage, 2),
                    'distinct_percentage': round(distinct_percentage, 2),
                    'characteristics': column_characteristics
                }
                
                column_profiles.append(column_profile)
            
            logger.info(f"Profiled {len(column_profiles)} columns for {schema_name}.{table_name}")
            
            return {
                'environment': environment,
                'schema_name': schema_name,
                'table_name': table_name,
                'total_rows': total_rows,
                'total_columns': len(column_profiles),
                'column_profiles': column_profiles
            }
            
        except Exception as e:
            logger.error(f"Column profiling failed for {schema_name}.{table_name}: {e}")
            raise
    
    def _analyze_column_characteristics(self, metadata: Dict, 
                                      stats: Dict, total_rows: int) -> Dict[str, Any]:
        """Analyze column characteristics and classify column purpose."""
        characteristics = {
            'likely_purpose': 'unknown',
            'data_quality_issues': [],
            'optimization_notes': [],
            'business_insights': []
        }
        
        null_count = stats.get('null_count', 0)
        distinct_count = stats.get('distinct_count', 0)
        null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
        distinct_percentage = (distinct_count / total_rows * 100) if total_rows > 0 else 0
        
        # Analyze likely purpose based on patterns
        column_name = metadata['column_name'].lower()
        data_type = metadata['data_type'].lower()
        
        # Primary key indicators
        if distinct_count == total_rows and null_count == 0:
            characteristics['likely_purpose'] = 'primary_key_candidate'
            characteristics['business_insights'].append('Unique, non-null - potential primary key')
        
        # Foreign key indicators
        elif ('_id' in column_name or column_name.endswith('id')) and data_type in ['integer', 'bigint']:
            characteristics['likely_purpose'] = 'foreign_key_candidate'
            characteristics['business_insights'].append('ID pattern suggests foreign key relationship')
        
        # Timestamp columns
        elif data_type in ['timestamp', 'timestamptz', 'date']:
            if 'created' in column_name or 'updated' in column_name:
                characteristics['likely_purpose'] = 'audit_timestamp'
            else:
                characteristics['likely_purpose'] = 'business_timestamp'
        
        # Status/category columns
        elif distinct_count < 20 and data_type in ['varchar', 'text', 'character']:
            characteristics['likely_purpose'] = 'categorical_data'
            characteristics['business_insights'].append(f'Low cardinality ({distinct_count} values) suggests categories/status')
        
        # Analyze data quality issues
        if null_percentage > 50:
            characteristics['data_quality_issues'].append(f'High NULL rate ({null_percentage:.1f}%)')
        
        if null_percentage > 90:
            characteristics['data_quality_issues'].append('Extremely high NULL rate - unused field or data quality problem')
        
        if distinct_count == 1 and total_rows > 1:
            characteristics['data_quality_issues'].append('Single value across all rows - potential constant or default')
        
        # Optimization notes
        if data_type == 'text' and metadata.get('character_maximum_length') is None:
            characteristics['optimization_notes'].append('Unbounded text field - consider VARCHAR with limit')
        
        if distinct_percentage > 95 and total_rows > 1000:
            characteristics['optimization_notes'].append('High cardinality - good candidate for indexing')
        
        return characteristics
    
    def analyze_schema_column_patterns(self, environment: str, 
                                     schema_name: str, limit: int = 20) -> Dict[str, Any]:
        """Analyze column patterns across all tables in a schema."""
        logger.info(f"Analyzing column patterns for schema {schema_name} in {environment}")
        
        # Get top tables by size for analysis
        tables_query = """
        SELECT 
            tablename,
            n_live_tup as estimated_rows
        FROM pg_stat_user_tables 
        WHERE schemaname = %s
        ORDER BY n_live_tup DESC NULLS LAST
        LIMIT %s
        """
        
        try:
            tables = self.db_connection.execute_query(environment, tables_query, (schema_name, limit))
            
            schema_analysis = {
                'environment': environment,
                'schema_name': schema_name,
                'tables_analyzed': len(tables),
                'table_profiles': [],
                'schema_summary': {
                    'total_columns': 0,
                    'high_null_columns': 0,
                    'primary_key_candidates': 0,
                    'foreign_key_candidates': 0,
                    'data_quality_issues': 0
                }
            }
            
            for table in tables:
                table_name = table['tablename']
                
                try:
                    table_profile = self.profile_table_columns(environment, schema_name, table_name)
                    schema_analysis['table_profiles'].append(table_profile)
                    
                    # Update schema summary
                    schema_analysis['schema_summary']['total_columns'] += table_profile['total_columns']
                    
                    for col in table_profile['column_profiles']:
                        if col['null_percentage'] > 50:
                            schema_analysis['schema_summary']['high_null_columns'] += 1
                        
                        if col['characteristics']['likely_purpose'] == 'primary_key_candidate':
                            schema_analysis['schema_summary']['primary_key_candidates'] += 1
                        elif col['characteristics']['likely_purpose'] == 'foreign_key_candidate':
                            schema_analysis['schema_summary']['foreign_key_candidates'] += 1
                        
                        if col['characteristics']['data_quality_issues']:
                            schema_analysis['schema_summary']['data_quality_issues'] += 1
                    
                except Exception as e:
                    logger.warning(f"Skipping table {table_name} due to error: {e}")
                    continue
            
            logger.info(f"Schema analysis complete for {schema_name}: "
                       f"{schema_analysis['schema_summary']['total_columns']} columns analyzed")
            
            return schema_analysis
            
        except Exception as e:
            logger.error(f"Schema column pattern analysis failed for {schema_name}: {e}")
            raise
    
    def generate_column_profile_report(self, environment: str, 
                                     target_schema: Optional[str] = None) -> str:
        """Generate comprehensive column profiling report."""
        logger.info(f"Generating column profile report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            if target_schema:
                # Analyze specific schema
                schema_analysis = self.analyze_schema_column_patterns(environment, target_schema)
                report.add_section('schema_column_analysis', schema_analysis)
            else:
                # Analyze all user schemas
                schemas_query = """
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
                AND schema_name NOT LIKE 'pg_%'
                ORDER BY schema_name
                """
                
                schemas = self.db_connection.execute_query(environment, schemas_query)
                
                all_schema_analyses = []
                for schema in schemas[:5]:  # Limit to top 5 schemas to avoid overwhelming
                    schema_name = schema['schema_name']
                    schema_analysis = self.analyze_schema_column_patterns(environment, schema_name, 10)
                    all_schema_analyses.append(schema_analysis)
                
                report.add_section('all_schemas_column_analysis', all_schema_analyses)
            
            # Export report
            filename = report.export('layer1_column_profiling')
            logger.info(f"Column profiling report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Column profile report generation failed: {e}")
            raise


def main():
    """Command-line interface for column profiling analysis."""
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
        print("Usage: python -m data_archaeologist.layer1_physical.column_profiling <environment> [schema_name]")
        sys.exit(1)
    
    environment = sys.argv[1]
    schema_name = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        db_conn = DatabaseConnection()
        profiler = ColumnProfiler(db_conn)
        
        print(f"Starting column profiling for {environment}" + 
              (f" (schema: {schema_name})" if schema_name else " (all schemas)"))
        
        report_file = profiler.generate_column_profile_report(environment, schema_name)
        print(f"Column profiling complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Column profiling failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
