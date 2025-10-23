"""
Layer 2: Logical Blueprint Discovery
Foreign Key Detection and Relationship Analysis Module

This module performs detective work to discover "bridges" between tables
by finding columns that match primary keys in other tables.
"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional, Set, Tuple

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


class ForeignKeyDetective:
    """Foreign key detection and relationship analysis for logical blueprint discovery."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def discover_declared_foreign_keys(self, environment: str) -> Dict[str, Any]:
        """Discover all formally declared foreign key constraints."""
        logger.info(f"Discovering declared foreign keys for {environment}")
        
        query = """
        SELECT 
            tc.table_schema as source_schema,
            tc.table_name as source_table,
            kcu.column_name as source_column,
            ccu.table_schema as target_schema,
            ccu.table_name as target_table,
            ccu.column_name as target_column,
            tc.constraint_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu 
            ON ccu.constraint_name = tc.constraint_name
        JOIN information_schema.referential_constraints rc 
            ON tc.constraint_name = rc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND tc.table_schema NOT LIKE 'pg_%'
        ORDER BY tc.table_schema, tc.table_name, kcu.column_name
        """
        
        try:
            declared_fks = self.db_connection.execute_query(environment, query)
            
            # Group by relationship patterns
            one_to_many = []
            self_referential = []
            
            for fk in declared_fks:
                if (fk['source_schema'] == fk['target_schema'] and 
                    fk['source_table'] == fk['target_table']):
                    self_referential.append(fk)
                else:
                    one_to_many.append(fk)
            
            logger.info(f"Found {len(declared_fks)} declared foreign keys: "
                       f"{len(one_to_many)} regular, {len(self_referential)} self-referential")
            
            return {
                'environment': environment,
                'summary': {
                    'total_declared_foreign_keys': len(declared_fks),
                    'regular_foreign_keys': len(one_to_many),
                    'self_referential_foreign_keys': len(self_referential)
                },
                'declared_foreign_keys': declared_fks,
                'categorization': {
                    'regular_relationships': one_to_many,
                    'self_referential': self_referential
                }
            }
            
        except Exception as e:
            logger.error(f"Declared foreign key discovery failed for {environment}: {e}")
            raise
    
    def discover_potential_foreign_keys(self, environment: str) -> Dict[str, Any]:
        """Discover potential foreign key relationships through pattern matching."""
        logger.info(f"Discovering potential foreign keys for {environment}")
        
        try:
            # First, get all primary keys to use as targets
            primary_keys = self._get_all_primary_keys(environment)
            
            # Get all columns that could be foreign keys
            potential_fk_columns = self._get_potential_foreign_key_columns(environment)
            
            # Match potential foreign keys with primary keys
            potential_relationships = []
            
            for pk in primary_keys:
                matching_columns = self._find_matching_columns(
                    environment, pk, potential_fk_columns
                )
                potential_relationships.extend(matching_columns)
            
            # Analyze relationship cardinality
            analyzed_relationships = []
            for rel in potential_relationships:
                cardinality_analysis = self._analyze_relationship_cardinality(environment, rel)
                rel.update(cardinality_analysis)
                analyzed_relationships.append(rel)
            
            # Categorize by confidence and relationship type
            high_confidence = [r for r in analyzed_relationships if r.get('confidence_score', 0) >= 80]
            medium_confidence = [r for r in analyzed_relationships if 50 <= r.get('confidence_score', 0) < 80]
            low_confidence = [r for r in analyzed_relationships if r.get('confidence_score', 0) < 50]
            
            logger.info(f"Found {len(potential_relationships)} potential relationships: "
                       f"{len(high_confidence)} high confidence, "
                       f"{len(medium_confidence)} medium confidence")
            
            return {
                'environment': environment,
                'summary': {
                    'total_potential_relationships': len(analyzed_relationships),
                    'high_confidence': len(high_confidence),
                    'medium_confidence': len(medium_confidence),
                    'low_confidence': len(low_confidence)
                },
                'potential_relationships': analyzed_relationships,
                'confidence_categorization': {
                    'high_confidence': high_confidence,
                    'medium_confidence': medium_confidence,
                    'low_confidence': low_confidence
                }
            }
            
        except Exception as e:
            logger.error(f"Potential foreign key discovery failed for {environment}: {e}")
            raise
    
    def _get_all_primary_keys(self, environment: str) -> List[Dict[str, Any]]:
        """Get all primary keys that could be targets for foreign keys."""
        query = """
        SELECT 
            tc.table_schema,
            tc.table_name,
            kcu.column_name,
            col.data_type,
            col.character_maximum_length,
            col.numeric_precision
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.columns col
            ON kcu.table_schema = col.table_schema
            AND kcu.table_name = col.table_name
            AND kcu.column_name = col.column_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        AND tc.table_schema NOT LIKE 'pg_%'
        ORDER BY tc.table_schema, tc.table_name
        """
        
        return self.db_connection.execute_query(environment, query)
    
    def _get_potential_foreign_key_columns(self, environment: str) -> List[Dict[str, Any]]:
        """Get columns that could potentially be foreign keys."""
        query = """
        SELECT 
            table_schema,
            table_name,
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_schema NOT LIKE 'pg_%'
        AND (
            column_name LIKE '%_id' 
            OR column_name LIKE '%id' 
            OR column_name LIKE '%_key'
            OR column_name LIKE '%key'
            OR data_type IN ('integer', 'bigint', 'uuid')
        )
        ORDER BY table_schema, table_name, column_name
        """
        
        return self.db_connection.execute_query(environment, query)
    
    def _find_matching_columns(self, environment: str, primary_key: Dict, 
                              potential_columns: List[Dict]) -> List[Dict[str, Any]]:
        """Find columns that could reference the given primary key."""
        matches = []
        
        pk_table = primary_key['table_name']
        pk_column = primary_key['column_name']
        pk_data_type = primary_key['data_type']
        
        for col in potential_columns:
            # Skip if it's the same table and column (can't be self-referencing this way)
            if (col['table_schema'] == primary_key['table_schema'] and
                col['table_name'] == primary_key['table_name'] and
                col['column_name'] == primary_key['column_name']):
                continue
            
            # Check for naming pattern matches
            naming_match = self._check_naming_pattern_match(pk_table, pk_column, col['column_name'])
            
            # Check for data type compatibility
            type_match = self._check_data_type_compatibility(pk_data_type, col['data_type'])
            
            if naming_match or type_match:
                confidence_score = self._calculate_foreign_key_confidence(
                    primary_key, col, naming_match, type_match
                )
                
                relationship = {
                    'source_schema': col['table_schema'],
                    'source_table': col['table_name'],
                    'source_column': col['column_name'],
                    'target_schema': primary_key['table_schema'],
                    'target_table': primary_key['table_name'],
                    'target_column': primary_key['column_name'],
                    'confidence_score': confidence_score,
                    'match_reasons': {
                        'naming_pattern': naming_match,
                        'data_type_compatible': type_match
                    }
                }
                
                matches.append(relationship)
        
        return matches
    
    def _check_naming_pattern_match(self, pk_table: str, pk_column: str, fk_column: str) -> bool:
        """Check if foreign key column name suggests it references the primary key."""
        fk_lower = fk_column.lower()
        pk_table_lower = pk_table.lower()
        pk_column_lower = pk_column.lower()
        
        # Direct column name match
        if fk_lower == pk_column_lower:
            return True
        
        # Table name + id pattern
        if fk_lower == f"{pk_table_lower}_id" or fk_lower == f"{pk_table_lower}id":
            return True
        
        # Abbreviated table name + id
        if len(pk_table_lower) > 3:
            table_abbrev = pk_table_lower[:3]
            if fk_lower == f"{table_abbrev}_id" or fk_lower == f"{table_abbrev}id":
                return True
        
        # Singular/plural variations
        if pk_table_lower.endswith('s') and len(pk_table_lower) > 3:
            singular = pk_table_lower[:-1]
            if fk_lower == f"{singular}_id" or fk_lower == f"{singular}id":
                return True
        
        return False
    
    def _check_data_type_compatibility(self, pk_type: str, fk_type: str) -> bool:
        """Check if data types are compatible for foreign key relationship."""
        pk_type_lower = pk_type.lower()
        fk_type_lower = fk_type.lower()
        
        # Exact match
        if pk_type_lower == fk_type_lower:
            return True
        
        # Compatible integer types
        integer_types = {'integer', 'bigint', 'serial', 'bigserial', 'smallint'}
        if pk_type_lower in integer_types and fk_type_lower in integer_types:
            return True
        
        # Compatible string types
        string_types = {'varchar', 'char', 'text', 'character varying', 'character'}
        if pk_type_lower in string_types and fk_type_lower in string_types:
            return True
        
        # UUID compatibility
        if 'uuid' in pk_type_lower and 'uuid' in fk_type_lower:
            return True
        
        return False
    
    def _calculate_foreign_key_confidence(self, pk: Dict, fk_col: Dict, 
                                        naming_match: bool, type_match: bool) -> int:
        """Calculate confidence score for potential foreign key relationship."""
        score = 0
        
        # Base score for type compatibility
        if type_match:
            score += 40
        
        # Naming pattern bonus
        if naming_match:
            score += 35
        
        # Data type preference bonuses
        if pk['data_type'].lower() in ['integer', 'bigint', 'serial', 'bigserial']:
            score += 10
        
        # Column naming conventions
        if fk_col['column_name'].lower().endswith('_id'):
            score += 10
        elif fk_col['column_name'].lower().endswith('id'):
            score += 5
        
        # Nullability considerations
        if fk_col['is_nullable'] == 'YES':
            score += 5  # Foreign keys can often be null
        
        return min(100, max(0, score))
    
    def _analyze_relationship_cardinality(self, environment: str, 
                                        relationship: Dict) -> Dict[str, Any]:
        """Analyze the cardinality of a potential relationship."""
        try:
            source_table = f'"{relationship["source_schema"]}"."{relationship["source_table"]}"'
            source_column = f'"{relationship["source_column"]}"'
            target_table = f'"{relationship["target_schema"]}"."{relationship["target_table"]}"'
            target_column = f'"{relationship["target_column"]}"'
            
            # Check cardinality
            cardinality_query = f"""
            WITH source_stats AS (
                SELECT 
                    count(*) as total_rows,
                    count(DISTINCT {source_column}) as distinct_values,
                    count({source_column}) as non_null_values
                FROM {source_table}
            ),
            target_stats AS (
                SELECT 
                    count(*) as total_rows,
                    count(DISTINCT {target_column}) as distinct_values
                FROM {target_table}
            ),
            relationship_stats AS (
                SELECT count(*) as matching_values
                FROM {source_table} s
                INNER JOIN {target_table} t ON s.{source_column} = t.{target_column}
            )
            SELECT 
                s.total_rows as source_total_rows,
                s.distinct_values as source_distinct_values,
                s.non_null_values as source_non_null_values,
                t.total_rows as target_total_rows,
                t.distinct_values as target_distinct_values,
                r.matching_values
            FROM source_stats s, target_stats t, relationship_stats r
            """
            
            result = self.db_connection.execute_query(environment, cardinality_query)
            
            if result:
                stats = result[0]
                
                # Determine relationship type
                source_duplicates = stats['source_total_rows'] > stats['source_distinct_values']
                target_duplicates = stats['target_total_rows'] > stats['target_distinct_values']
                
                if not source_duplicates and not target_duplicates:
                    relationship_type = 'one_to_one'
                elif source_duplicates and not target_duplicates:
                    relationship_type = 'many_to_one'
                elif not source_duplicates and target_duplicates:
                    relationship_type = 'one_to_many'
                else:
                    relationship_type = 'many_to_many'
                
                # Calculate data integrity score
                if stats['source_non_null_values'] > 0:
                    integrity_score = (stats['matching_values'] / stats['source_non_null_values']) * 100
                else:
                    integrity_score = 0
                
                return {
                    'cardinality_analysis': stats,
                    'relationship_type': relationship_type,
                    'data_integrity_score': round(integrity_score, 2),
                    'has_orphaned_records': stats['matching_values'] < stats['source_non_null_values']
                }
            
        except Exception as e:
            logger.warning(f"Cardinality analysis failed for relationship: {e}")
        
        return {
            'cardinality_analysis': None,
            'relationship_type': 'unknown',
            'data_integrity_score': 0,
            'has_orphaned_records': None
        }
    
    def generate_foreign_key_report(self, environment: str) -> str:
        """Generate comprehensive foreign key analysis report."""
        logger.info(f"Generating foreign key analysis report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Gather all foreign key data
            declared_fks = self.discover_declared_foreign_keys(environment)
            potential_fks = self.discover_potential_foreign_keys(environment)
            
            # Add sections to report
            report.add_section('declared_foreign_keys', declared_fks)
            report.add_section('potential_foreign_keys', potential_fks)
            
            # Export report
            filename = report.export('layer2_foreign_key_analysis')
            logger.info(f"Foreign key analysis report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Foreign key report generation failed: {e}")
            raise


def main():
    """Command-line interface for foreign key analysis."""
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
        print("Usage: python -m data_archaeologist.layer2_logical.foreign_key_detection <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        detective = ForeignKeyDetective(db_conn)
        
        print(f"Starting foreign key analysis for {environment}...")
        report_file = detective.generate_foreign_key_report(environment)
        print(f"Foreign key analysis complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Foreign key analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
