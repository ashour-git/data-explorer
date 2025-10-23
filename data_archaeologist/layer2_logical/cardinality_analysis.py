"""
Layer 2: Logical Blueprint Discovery
Relationship Cardinality Analysis Module

This module analyzes the "traffic flow" between tables to understand
one-to-many, many-to-many, and other relationship patterns.
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


class CardinalityAnalyzer:
    """Relationship cardinality analysis for understanding data flow patterns."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def analyze_all_relationships(self, environment: str) -> Dict[str, Any]:
        """Analyze cardinality for all discovered relationships."""
        logger.info(f"Analyzing relationship cardinalities for {environment}")
        
        try:
            # Get all potential relationships from foreign key analysis
            from .foreign_key_detection import ForeignKeyDetective
            fk_detective = ForeignKeyDetective(self.db_connection)
            
            declared_fks = fk_detective.discover_declared_foreign_keys(environment)
            potential_fks = fk_detective.discover_potential_foreign_keys(environment)
            
            # Analyze declared relationships
            declared_cardinalities = []
            for fk in declared_fks['declared_foreign_keys']:
                cardinality = self._analyze_single_relationship(environment, {
                    'source_schema': fk['source_schema'],
                    'source_table': fk['source_table'],
                    'source_column': fk['source_column'],
                    'target_schema': fk['target_schema'],
                    'target_table': fk['target_table'],
                    'target_column': fk['target_column']
                })
                cardinality['relationship_type'] = 'declared_foreign_key'
                cardinality['constraint_name'] = fk['constraint_name']
                declared_cardinalities.append(cardinality)
            
            # Analyze high-confidence potential relationships
            potential_cardinalities = []
            high_confidence_relationships = potential_fks['confidence_categorization']['high_confidence']
            
            for rel in high_confidence_relationships:
                cardinality = self._analyze_single_relationship(environment, rel)
                cardinality['relationship_type'] = 'potential_foreign_key'
                cardinality['confidence_score'] = rel['confidence_score']
                potential_cardinalities.append(cardinality)
            
            # Categorize by cardinality patterns
            cardinality_summary = self._categorize_cardinalities(
                declared_cardinalities + potential_cardinalities
            )
            
            logger.info(f"Analyzed {len(declared_cardinalities)} declared and "
                       f"{len(potential_cardinalities)} potential relationships")
            
            return {
                'environment': environment,
                'summary': {
                    'declared_relationships': len(declared_cardinalities),
                    'potential_relationships': len(potential_cardinalities),
                    'total_analyzed': len(declared_cardinalities) + len(potential_cardinalities)
                },
                'cardinality_breakdown': cardinality_summary,
                'detailed_analysis': {
                    'declared_relationships': declared_cardinalities,
                    'potential_relationships': potential_cardinalities
                }
            }
            
        except Exception as e:
            logger.error(f"Relationship cardinality analysis failed for {environment}: {e}")
            raise
    
    def _analyze_single_relationship(self, environment: str, 
                                   relationship: Dict[str, str]) -> Dict[str, Any]:
        """Analyze cardinality for a single relationship."""
        try:
            source_table = f'"{relationship["source_schema"]}"."{relationship["source_table"]}"'
            source_column = f'"{relationship["source_column"]}"'
            target_table = f'"{relationship["target_schema"]}"."{relationship["target_table"]}"'
            target_column = f'"{relationship["target_column"]}"'
            
            # Comprehensive cardinality analysis
            analysis_query = f"""
            WITH source_analysis AS (
                SELECT 
                    count(*) as total_rows,
                    count(DISTINCT {source_column}) as unique_values,
                    count({source_column}) as non_null_values,
                    count(*) - count({source_column}) as null_values,
                    max(cnt) as max_duplicates
                FROM (
                    SELECT {source_column}, count(*) as cnt
                    FROM {source_table}
                    WHERE {source_column} IS NOT NULL
                    GROUP BY {source_column}
                ) dup_analysis, {source_table}
            ),
            target_analysis AS (
                SELECT 
                    count(*) as total_rows,
                    count(DISTINCT {target_column}) as unique_values,
                    count({target_column}) as non_null_values
                FROM {target_table}
            ),
            relationship_analysis AS (
                SELECT 
                    count(*) as matching_records,
                    count(DISTINCT s.{source_column}) as matching_unique_source,
                    count(DISTINCT t.{target_column}) as matching_unique_target
                FROM {source_table} s
                INNER JOIN {target_table} t ON s.{source_column} = t.{target_column}
                WHERE s.{source_column} IS NOT NULL
            ),
            orphan_analysis AS (
                SELECT count(*) as orphaned_records
                FROM {source_table} s
                LEFT JOIN {target_table} t ON s.{source_column} = t.{target_column}
                WHERE s.{source_column} IS NOT NULL AND t.{target_column} IS NULL
            )
            SELECT 
                s.*,
                t.total_rows as target_total_rows,
                t.unique_values as target_unique_values,
                t.non_null_values as target_non_null_values,
                r.matching_records,
                r.matching_unique_source,
                r.matching_unique_target,
                o.orphaned_records
            FROM source_analysis s, target_analysis t, relationship_analysis r, orphan_analysis o
            """
            
            result = self.db_connection.execute_query(environment, analysis_query)
            
            if not result:
                return self._create_failed_analysis(relationship)
            
            stats = result[0]
            
            # Determine cardinality pattern
            cardinality_pattern = self._determine_cardinality_pattern(stats)
            
            # Calculate relationship quality metrics
            quality_metrics = self._calculate_relationship_quality(stats)
            
            # Business insights
            business_insights = self._generate_business_insights(
                relationship, cardinality_pattern, quality_metrics
            )
            
            return {
                'source_table': f"{relationship['source_schema']}.{relationship['source_table']}",
                'source_column': relationship['source_column'],
                'target_table': f"{relationship['target_schema']}.{relationship['target_table']}",
                'target_column': relationship['target_column'],
                'cardinality_pattern': cardinality_pattern,
                'quality_metrics': quality_metrics,
                'detailed_statistics': dict(stats),
                'business_insights': business_insights
            }
            
        except Exception as e:
            logger.warning(f"Failed to analyze relationship {relationship}: {e}")
            return self._create_failed_analysis(relationship)
    
    def _determine_cardinality_pattern(self, stats: Dict) -> Dict[str, Any]:
        """Determine the cardinality pattern from statistics."""
        source_total = stats['total_rows']
        source_unique = stats['unique_values']
        target_total = stats['target_total_rows']
        target_unique = stats['target_unique_values']
        matching_records = stats['matching_records']
        
        # Check for uniqueness on both sides
        source_has_duplicates = source_unique < source_total if source_total > 0 else False
        target_has_duplicates = target_unique < target_total if target_total > 0 else False
        
        # Determine pattern
        if not source_has_duplicates and not target_has_duplicates:
            pattern = "one_to_one"
            description = "Each record in source matches exactly one record in target"
        elif source_has_duplicates and not target_has_duplicates:
            pattern = "many_to_one"
            description = "Multiple source records can reference the same target record"
        elif not source_has_duplicates and target_has_duplicates:
            pattern = "one_to_many"
            description = "Each source record can reference multiple target records"
        else:
            pattern = "many_to_many"
            description = "Complex relationship with duplicates on both sides"
        
        # Calculate average fan-out
        if matching_records > 0 and stats['matching_unique_source'] > 0:
            avg_fanout = matching_records / stats['matching_unique_source']
        else:
            avg_fanout = 0
        
        return {
            'pattern': pattern,
            'description': description,
            'source_has_duplicates': source_has_duplicates,
            'target_has_duplicates': target_has_duplicates,
            'average_fanout': round(avg_fanout, 2),
            'max_duplicates_per_value': stats.get('max_duplicates', 0)
        }
    
    def _calculate_relationship_quality(self, stats: Dict) -> Dict[str, Any]:
        """Calculate quality metrics for the relationship."""
        total_source = stats['non_null_values']
        matching = stats['matching_records']
        orphaned = stats['orphaned_records']
        
        # Referential integrity score
        if total_source > 0:
            integrity_score = ((total_source - orphaned) / total_source) * 100
        else:
            integrity_score = 0
        
        # Data completeness score
        if stats['total_rows'] > 0:
            completeness_score = (stats['non_null_values'] / stats['total_rows']) * 100
        else:
            completeness_score = 0
        
        # Relationship density (how much of target is actually referenced)
        if stats['target_unique_values'] > 0:
            density_score = (stats['matching_unique_target'] / stats['target_unique_values']) * 100
        else:
            density_score = 0
        
        return {
            'referential_integrity_score': round(integrity_score, 2),
            'data_completeness_score': round(completeness_score, 2),
            'relationship_density_score': round(density_score, 2),
            'has_orphaned_records': orphaned > 0,
            'orphaned_record_count': orphaned,
            'matching_record_count': matching
        }
    
    def _generate_business_insights(self, relationship: Dict, 
                                  cardinality: Dict, quality: Dict) -> List[str]:
        """Generate business insights from the relationship analysis."""
        insights = []
        
        # Cardinality insights
        pattern = cardinality['pattern']
        if pattern == 'one_to_one':
            insights.append("One-to-one relationship suggests either data normalization or potential data model issue")
        elif pattern == 'many_to_one':
            insights.append("Many-to-one relationship indicates a lookup or reference pattern")
        elif pattern == 'one_to_many':
            insights.append("One-to-many relationship suggests a parent-child or hierarchical structure")
        elif pattern == 'many_to_many':
            insights.append("Many-to-many relationship may indicate missing junction table or complex business rules")
        
        # Quality insights
        if quality['referential_integrity_score'] < 90:
            insights.append(f"Referential integrity issues detected ({quality['referential_integrity_score']:.1f}% valid references)")
        
        if quality['has_orphaned_records']:
            insights.append(f"Found {quality['orphaned_record_count']} orphaned records that don't match target table")
        
        if quality['data_completeness_score'] < 80:
            insights.append(f"High NULL rate in foreign key column ({100 - quality['data_completeness_score']:.1f}% NULL)")
        
        if quality['relationship_density_score'] < 50:
            insights.append(f"Low relationship density - only {quality['relationship_density_score']:.1f}% of target records are referenced")
        
        # Performance insights
        if cardinality['average_fanout'] > 100:
            insights.append(f"High fan-out ratio ({cardinality['average_fanout']:.1f}) may impact query performance")
        
        return insights
    
    def _categorize_cardinalities(self, all_relationships: List[Dict]) -> Dict[str, Any]:
        """Categorize relationships by their cardinality patterns."""
        categories = {
            'one_to_one': [],
            'one_to_many': [],
            'many_to_one': [],
            'many_to_many': [],
            'failed_analysis': []
        }
        
        quality_summary = {
            'high_integrity': 0,  # >95%
            'medium_integrity': 0,  # 80-95%
            'low_integrity': 0,  # <80%
            'orphaned_relationships': 0
        }
        
        for rel in all_relationships:
            pattern = rel.get('cardinality_pattern', {}).get('pattern', 'failed_analysis')
            if pattern in categories:
                categories[pattern].append(rel)
            else:
                categories['failed_analysis'].append(rel)
            
            # Quality categorization
            integrity_score = rel.get('quality_metrics', {}).get('referential_integrity_score', 0)
            if integrity_score > 95:
                quality_summary['high_integrity'] += 1
            elif integrity_score >= 80:
                quality_summary['medium_integrity'] += 1
            else:
                quality_summary['low_integrity'] += 1
            
            if rel.get('quality_metrics', {}).get('has_orphaned_records', False):
                quality_summary['orphaned_relationships'] += 1
        
        return {
            'pattern_distribution': {k: len(v) for k, v in categories.items()},
            'quality_distribution': quality_summary,
            'detailed_categories': categories
        }
    
    def _create_failed_analysis(self, relationship: Dict) -> Dict[str, Any]:
        """Create a placeholder for failed relationship analysis."""
        return {
            'source_table': f"{relationship['source_schema']}.{relationship['source_table']}",
            'source_column': relationship['source_column'],
            'target_table': f"{relationship['target_schema']}.{relationship['target_table']}",
            'target_column': relationship['target_column'],
            'cardinality_pattern': {'pattern': 'analysis_failed'},
            'quality_metrics': {},
            'detailed_statistics': {},
            'business_insights': ['Analysis failed - unable to determine relationship characteristics']
        }
    
    def generate_cardinality_report(self, environment: str) -> str:
        """Generate comprehensive cardinality analysis report."""
        logger.info(f"Generating cardinality analysis report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Perform comprehensive cardinality analysis
            cardinality_analysis = self.analyze_all_relationships(environment)
            
            # Add sections to report
            report.add_section('cardinality_analysis', cardinality_analysis)
            
            # Export report
            filename = report.export('layer2_cardinality_analysis')
            logger.info(f"Cardinality analysis report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Cardinality report generation failed: {e}")
            raise


def main():
    """Command-line interface for cardinality analysis."""
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
        print("Usage: python -m data_archaeologist.layer2_logical.cardinality_analysis <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        analyzer = CardinalityAnalyzer(db_conn)
        
        print(f"Starting cardinality analysis for {environment}...")
        report_file = analyzer.generate_cardinality_report(environment)
        print(f"Cardinality analysis complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Cardinality analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
