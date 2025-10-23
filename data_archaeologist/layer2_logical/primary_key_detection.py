"""
Layer 2: Logical Blueprint Discovery
Primary Key Detection and Analysis Module

This module systematically identifies the "anchor" of each table - the unique identifier
that serves as the foundation for all relationships and data integrity.
"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional, Tuple

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


class PrimaryKeyDetective:
    """Primary key detection and analysis for logical blueprint discovery."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def discover_declared_primary_keys(self, environment: str) -> Dict[str, Any]:
        """Discover all formally declared primary keys in the database."""
        logger.info(f"Discovering declared primary keys for {environment}")
        
        query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as primary_key_columns,
            count(kcu.column_name) as column_count
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND tc.table_schema NOT LIKE 'pg_%'
        GROUP BY tc.table_schema, tc.table_name, tc.constraint_name
        ORDER BY tc.table_schema, tc.table_name
        """
        
        try:
            declared_pks = self.db_connection.execute_query(environment, query)
            
            # Categorize by complexity
            single_column_pks = [pk for pk in declared_pks if pk['column_count'] == 1]
            composite_pks = [pk for pk in declared_pks if pk['column_count'] > 1]
            
            logger.info(f"Found {len(declared_pks)} declared primary keys: "
                       f"{len(single_column_pks)} single-column, "
                       f"{len(composite_pks)} composite")
            
            return {
                'environment': environment,
                'summary': {
                    'total_declared_primary_keys': len(declared_pks),
                    'single_column_primary_keys': len(single_column_pks),
                    'composite_primary_keys': len(composite_pks)
                },
                'declared_primary_keys': declared_pks,
                'categorization': {
                    'single_column': single_column_pks,
                    'composite': composite_pks
                }
            }
            
        except Exception as e:
            logger.error(f"Primary key discovery failed for {environment}: {e}")
            raise
    
    def discover_natural_primary_keys(self, environment: str, 
                                    schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Discover natural primary keys - unique, non-null columns that could serve as anchors."""
        logger.info(f"Discovering natural primary keys for {environment}" + 
                   (f" in schema {schema_name}" if schema_name else ""))
        
        # Get tables to analyze
        if schema_name:
            tables_query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            params = (schema_name,)
        else:
            tables_query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND table_schema NOT LIKE 'pg_%'
            AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
            params = None
        
        try:
            tables = self.db_connection.execute_query(environment, tables_query, params)
            
            natural_keys = []
            tables_without_natural_keys = []
            
            for table in tables:
                schema = table['table_schema']
                table_name = table['table_name']
                
                # Find potential natural keys for this table
                table_natural_keys = self._analyze_table_for_natural_keys(
                    environment, schema, table_name
                )
                
                if table_natural_keys:
                    natural_keys.extend(table_natural_keys)
                else:
                    tables_without_natural_keys.append(f"{schema}.{table_name}")
            
            logger.info(f"Found {len(natural_keys)} natural key candidates")
            
            return {
                'environment': environment,
                'analysis_scope': schema_name or 'all_schemas',
                'summary': {
                    'tables_analyzed': len(tables),
                    'natural_key_candidates': len(natural_keys),
                    'tables_without_natural_keys': len(tables_without_natural_keys)
                },
                'natural_key_candidates': natural_keys,
                'tables_without_natural_keys': tables_without_natural_keys
            }
            
        except Exception as e:
            logger.error(f"Natural primary key discovery failed: {e}")
            raise
    
    def _analyze_table_for_natural_keys(self, environment: str, 
                                      schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Analyze a single table for natural primary key candidates."""
        try:
            # Get table row count first
            count_query = f'SELECT count(*) as row_count FROM "{schema_name}"."{table_name}"'
            count_result = self.db_connection.execute_query(environment, count_query)
            row_count = count_result[0]['row_count'] if count_result else 0
            
            if row_count == 0:
                return []
            
            # Get columns to test
            columns_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_connection.execute_query(
                environment, columns_query, (schema_name, table_name)
            )
            
            natural_keys = []
            
            for column in columns:
                column_name = column['column_name']
                
                # Test for uniqueness and non-nullability
                uniqueness_query = f"""
                SELECT 
                    count(*) as total_rows,
                    count(DISTINCT "{column_name}") as distinct_values,
                    count("{column_name}") as non_null_values
                FROM "{schema_name}"."{table_name}"
                """
                
                result = self.db_connection.execute_query(environment, uniqueness_query)
                stats = result[0] if result else {}
                
                total_rows = stats.get('total_rows', 0)
                distinct_values = stats.get('distinct_values', 0)
                non_null_values = stats.get('non_null_values', 0)
                
                # Check if this column qualifies as a natural primary key
                is_unique = distinct_values == total_rows
                is_non_null = non_null_values == total_rows
                
                if is_unique and is_non_null and total_rows > 0:
                    # Additional analysis for confidence scoring
                    confidence_score = self._calculate_primary_key_confidence(
                        column_name, column['data_type'], total_rows
                    )
                    
                    natural_key = {
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'column_name': column_name,
                        'data_type': column['data_type'],
                        'row_count': total_rows,
                        'distinct_values': distinct_values,
                        'confidence_score': confidence_score,
                        'key_characteristics': self._analyze_key_characteristics(column_name, column['data_type'])
                    }
                    
                    natural_keys.append(natural_key)
            
            return natural_keys
            
        except Exception as e:
            logger.warning(f"Failed to analyze {schema_name}.{table_name} for natural keys: {e}")
            return []
    
    def _calculate_primary_key_confidence(self, column_name: str, 
                                        data_type: str, row_count: int) -> int:
        """Calculate confidence score for primary key candidate (0-100)."""
        score = 50  # Base score
        
        # Column name patterns that suggest primary key
        if column_name.lower() in ['id', 'key', 'pk']:
            score += 30
        elif column_name.lower().endswith('_id') or column_name.lower().endswith('id'):
            score += 20
        elif 'key' in column_name.lower():
            score += 10
        
        # Data type preferences
        if data_type.lower() in ['integer', 'bigint', 'serial', 'bigserial']:
            score += 15
        elif data_type.lower() in ['uuid', 'char', 'varchar']:
            score += 10
        
        # Size considerations
        if row_count > 1000:
            score += 5
        
        return min(100, max(0, score))
    
    def _analyze_key_characteristics(self, column_name: str, data_type: str) -> Dict[str, Any]:
        """Analyze characteristics of a potential primary key."""
        characteristics = {
            'naming_pattern': 'unknown',
            'data_type_category': 'unknown',
            'likely_generation_method': 'unknown'
        }
        
        # Analyze naming patterns
        col_lower = column_name.lower()
        if col_lower == 'id':
            characteristics['naming_pattern'] = 'simple_id'
        elif col_lower.endswith('_id'):
            characteristics['naming_pattern'] = 'prefixed_id'
        elif 'key' in col_lower:
            characteristics['naming_pattern'] = 'key_pattern'
        elif 'uuid' in col_lower or 'guid' in col_lower:
            characteristics['naming_pattern'] = 'uuid_pattern'
        
        # Analyze data type
        data_type_lower = data_type.lower()
        if data_type_lower in ['serial', 'bigserial']:
            characteristics['data_type_category'] = 'auto_increment'
            characteristics['likely_generation_method'] = 'database_sequence'
        elif data_type_lower in ['integer', 'bigint']:
            characteristics['data_type_category'] = 'numeric'
            characteristics['likely_generation_method'] = 'application_generated'
        elif data_type_lower == 'uuid':
            characteristics['data_type_category'] = 'uuid'
            characteristics['likely_generation_method'] = 'uuid_generator'
        elif data_type_lower in ['varchar', 'char', 'text']:
            characteristics['data_type_category'] = 'string'
            characteristics['likely_generation_method'] = 'business_key'
        
        return characteristics
    
    def compare_declared_vs_natural_keys(self, environment: str) -> Dict[str, Any]:
        """Compare declared primary keys with discovered natural keys."""
        logger.info(f"Comparing declared vs natural primary keys for {environment}")
        
        try:
            declared_analysis = self.discover_declared_primary_keys(environment)
            natural_analysis = self.discover_natural_primary_keys(environment)
            
            declared_pks = declared_analysis['declared_primary_keys']
            natural_keys = natural_analysis['natural_key_candidates']
            
            # Create lookup sets for comparison
            declared_set = set()
            for pk in declared_pks:
                # For single-column primary keys
                if pk['column_count'] == 1:
                    key = (pk['table_schema'], pk['table_name'], pk['primary_key_columns'])
                    declared_set.add(key)
            
            natural_set = set()
            for nk in natural_keys:
                key = (nk['schema_name'], nk['table_name'], nk['column_name'])
                natural_set.add(key)
            
            # Find matches and mismatches
            matches = declared_set.intersection(natural_set)
            declared_only = declared_set - natural_set
            natural_only = natural_set - declared_set
            
            # Tables with no primary key at all
            all_tables_query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND table_schema NOT LIKE 'pg_%'
            AND table_type = 'BASE TABLE'
            """
            
            all_tables = self.db_connection.execute_query(environment, all_tables_query)
            tables_with_declared_pk = set((pk['table_schema'], pk['table_name']) for pk in declared_pks)
            tables_with_natural_key = set((nk['schema_name'], nk['table_name']) for nk in natural_keys)
            
            all_tables_set = set((t['table_schema'], t['table_name']) for t in all_tables)
            tables_without_any_key = all_tables_set - tables_with_declared_pk - tables_with_natural_key
            
            logger.info(f"Key comparison complete: {len(matches)} matches, "
                       f"{len(declared_only)} declared-only, {len(natural_only)} natural-only")
            
            return {
                'environment': environment,
                'comparison_summary': {
                    'total_tables': len(all_tables),
                    'tables_with_declared_pk': len(tables_with_declared_pk),
                    'tables_with_natural_key': len(tables_with_natural_key),
                    'matching_keys': len(matches),
                    'declared_only': len(declared_only),
                    'natural_only': len(natural_only),
                    'tables_without_any_key': len(tables_without_any_key)
                },
                'detailed_comparison': {
                    'matching_keys': list(matches),
                    'declared_only': list(declared_only),
                    'natural_only': list(natural_only),
                    'tables_without_any_key': list(tables_without_any_key)
                },
                'declared_analysis': declared_analysis,
                'natural_analysis': natural_analysis
            }
            
        except Exception as e:
            logger.error(f"Key comparison failed for {environment}: {e}")
            raise
    
    def generate_primary_key_report(self, environment: str) -> str:
        """Generate comprehensive primary key analysis report."""
        logger.info(f"Generating primary key analysis report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Perform comprehensive analysis
            comparison = self.compare_declared_vs_natural_keys(environment)
            
            # Add all sections to report
            report.add_section('primary_key_comparison', comparison)
            
            # Export report
            filename = report.export('layer2_primary_key_analysis')
            logger.info(f"Primary key analysis report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Primary key report generation failed: {e}")
            raise


def main():
    """Command-line interface for primary key analysis."""
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
        print("Usage: python -m data_archaeologist.layer2_logical.primary_key_detection <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        detective = PrimaryKeyDetective(db_conn)
        
        print(f"Starting primary key analysis for {environment}...")
        report_file = detective.generate_primary_key_report(environment)
        print(f"Primary key analysis complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Primary key analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
