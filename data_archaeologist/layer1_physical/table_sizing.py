"""
Layer 1: Physical Map Discovery
Table Sizing and Granularity Analysis Module

This module identifies the largest tables by row count and storage size,
providing insights into fact tables vs dimension tables and database gravity center.
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


class TableSizingAnalyzer:
    """Table sizing and granularity analysis for physical layer discovery."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def analyze_table_sizes(self, environment: str, limit: int = 50) -> Dict[str, Any]:
        """Analyze tables by storage size to identify database gravity center."""
        logger.info(f"Analyzing table sizes for {environment} (top {limit})")
        
        query = """
        SELECT 
            schemaname,
            tablename,
            schemaname || '.' || tablename as full_table_name,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
            pg_total_relation_size(schemaname||'.'||tablename) as total_size_bytes,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
            pg_relation_size(schemaname||'.'||tablename) as table_size_bytes,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                          pg_relation_size(schemaname||'.'||tablename)) as index_size,
            (pg_total_relation_size(schemaname||'.'||tablename) - 
             pg_relation_size(schemaname||'.'||tablename)) as index_size_bytes
        FROM pg_tables 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        AND schemaname NOT LIKE 'pg_%'
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT %s
        """
        
        try:
            size_analysis = self.db_connection.execute_query(environment, query, (limit,))
            
            # Calculate summary statistics
            total_size_bytes = sum(t['total_size_bytes'] for t in size_analysis)
            total_table_bytes = sum(t['table_size_bytes'] for t in size_analysis)
            total_index_bytes = sum(t['index_size_bytes'] for t in size_analysis)
            
            # Identify size categories
            large_tables = [t for t in size_analysis if t['total_size_bytes'] > 1024*1024*100]  # >100MB
            medium_tables = [t for t in size_analysis if 1024*1024*10 <= t['total_size_bytes'] <= 1024*1024*100]  # 10-100MB
            small_tables = [t for t in size_analysis if t['total_size_bytes'] < 1024*1024*10]  # <10MB
            
            logger.info(f"Size analysis complete: {len(large_tables)} large, "
                       f"{len(medium_tables)} medium, {len(small_tables)} small tables")
            
            return {
                'environment': environment,
                'analysis_metadata': {
                    'top_tables_analyzed': len(size_analysis),
                    'total_size_analyzed': format_bytes(total_size_bytes),
                    'table_data_size': format_bytes(total_table_bytes),
                    'index_size': format_bytes(total_index_bytes)
                },
                'size_categories': {
                    'large_tables_100mb_plus': len(large_tables),
                    'medium_tables_10_100mb': len(medium_tables),
                    'small_tables_under_10mb': len(small_tables)
                },
                'detailed_analysis': size_analysis,
                'size_breakdown': {
                    'large_tables': large_tables[:10],  # Top 10 large tables
                    'medium_tables': medium_tables[:10],
                    'small_tables': small_tables[:10]
                }
            }
            
        except Exception as e:
            logger.error(f"Table size analysis failed for {environment}: {e}")
            raise
    
    def analyze_row_counts(self, environment: str, limit: int = 50) -> Dict[str, Any]:
        """Analyze tables by row count to identify fact vs dimension tables."""
        logger.info(f"Analyzing table row counts for {environment} (top {limit})")
        
        query = """
        SELECT 
            schemaname,
            tablename,
            schemaname || '.' || tablename as full_table_name,
            n_live_tup as estimated_row_count,
            n_dead_tup as dead_row_count,
            n_tup_ins as total_inserts,
            n_tup_upd as total_updates,
            n_tup_del as total_deletes,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC NULLS LAST
        LIMIT %s
        """
        
        try:
            row_analysis = self.db_connection.execute_query(environment, query, (limit,))
            
            # Calculate summary statistics
            total_estimated_rows = sum(t.get('estimated_row_count', 0) or 0 for t in row_analysis)
            
            # Categorize by row count patterns (typical business patterns)
            fact_tables = [t for t in row_analysis if (t.get('estimated_row_count', 0) or 0) > 100000]  # >100K rows
            dimension_tables = [t for t in row_analysis if (t.get('estimated_row_count', 0) or 0) <= 100000]  # <=100K rows
            
            # Identify highly active tables (lots of inserts/updates)
            active_tables = [t for t in row_analysis 
                           if (t.get('total_inserts', 0) or 0) + (t.get('total_updates', 0) or 0) > 10000]
            
            logger.info(f"Row analysis complete: {len(fact_tables)} fact-like tables, "
                       f"{len(dimension_tables)} dimension-like tables")
            
            return {
                'environment': environment,
                'analysis_metadata': {
                    'tables_analyzed': len(row_analysis),
                    'total_estimated_rows': total_estimated_rows
                },
                'table_categories': {
                    'fact_tables_100k_plus': len(fact_tables),
                    'dimension_tables_under_100k': len(dimension_tables),
                    'highly_active_tables': len(active_tables)
                },
                'detailed_analysis': row_analysis,
                'category_breakdown': {
                    'likely_fact_tables': fact_tables[:15],
                    'likely_dimension_tables': dimension_tables[:15],
                    'highly_active_tables': active_tables[:10]
                }
            }
            
        except Exception as e:
            logger.error(f"Row count analysis failed for {environment}: {e}")
            raise
    
    def analyze_table_activity_patterns(self, environment: str) -> Dict[str, Any]:
        """Analyze table activity patterns to understand business processes."""
        logger.info(f"Analyzing table activity patterns for {environment}")
        
        query = """
        SELECT 
            schemaname,
            tablename,
            schemaname || '.' || tablename as full_table_name,
            seq_scan as sequential_scans,
            seq_tup_read as sequential_tuples_read,
            idx_scan as index_scans,
            idx_tup_fetch as index_tuples_fetched,
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            CASE 
                WHEN n_live_tup > 0 THEN round((n_dead_tup::numeric / n_live_tup::numeric) * 100, 2)
                ELSE 0 
            END as dead_tuple_percentage,
            CASE
                WHEN seq_scan + idx_scan > 0 THEN 
                    round((idx_scan::numeric / (seq_scan + idx_scan)::numeric) * 100, 2)
                ELSE 0
            END as index_usage_percentage
        FROM pg_stat_user_tables
        WHERE n_live_tup > 0
        ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
        """
        
        try:
            activity_analysis = self.db_connection.execute_query(environment, query)
            
            # Categorize tables by activity patterns
            read_heavy = [t for t in activity_analysis 
                         if (t.get('sequential_scans', 0) or 0) + (t.get('index_scans', 0) or 0) > 
                             (t.get('inserts', 0) or 0) + (t.get('updates', 0) or 0) + (t.get('deletes', 0) or 0)]
            
            write_heavy = [t for t in activity_analysis 
                          if (t.get('inserts', 0) or 0) + (t.get('updates', 0) or 0) + (t.get('deletes', 0) or 0) > 
                              (t.get('sequential_scans', 0) or 0) + (t.get('index_scans', 0) or 0)]
            
            # Tables with high dead tuple percentage (needs attention)
            maintenance_needed = [t for t in activity_analysis 
                                if (t.get('dead_tuple_percentage', 0) or 0) > 20]
            
            # Tables with low index usage (potential optimization)
            low_index_usage = [t for t in activity_analysis 
                             if (t.get('index_usage_percentage', 0) or 0) < 50 and 
                                (t.get('sequential_scans', 0) or 0) > 100]
            
            logger.info(f"Activity analysis complete: {len(read_heavy)} read-heavy, "
                       f"{len(write_heavy)} write-heavy tables")
            
            return {
                'environment': environment,
                'analysis_metadata': {
                    'total_active_tables': len(activity_analysis),
                    'read_heavy_tables': len(read_heavy),
                    'write_heavy_tables': len(write_heavy),
                    'maintenance_needed': len(maintenance_needed),
                    'optimization_candidates': len(low_index_usage)
                },
                'activity_patterns': {
                    'read_heavy_tables': read_heavy[:10],
                    'write_heavy_tables': write_heavy[:10],
                    'maintenance_needed': maintenance_needed,
                    'low_index_usage': low_index_usage
                },
                'detailed_analysis': activity_analysis[:30]  # Top 30 most active
            }
            
        except Exception as e:
            logger.error(f"Activity pattern analysis failed for {environment}: {e}")
            raise
    
    def generate_sizing_report(self, environment: str) -> str:
        """Generate comprehensive table sizing and granularity report."""
        logger.info(f"Generating table sizing report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Gather all sizing data
            size_analysis = self.analyze_table_sizes(environment)
            row_analysis = self.analyze_row_counts(environment)
            activity_analysis = self.analyze_table_activity_patterns(environment)
            
            # Add sections to report
            report.add_section('table_size_analysis', size_analysis)
            report.add_section('row_count_analysis', row_analysis)
            report.add_section('activity_pattern_analysis', activity_analysis)
            
            # Export report
            filename = report.export('layer1_table_sizing')
            logger.info(f"Table sizing report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Sizing report generation failed: {e}")
            raise


def main():
    """Command-line interface for table sizing analysis."""
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
        print("Usage: python -m data_archaeologist.layer1_physical.table_sizing <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        analyzer = TableSizingAnalyzer(db_conn)
        
        print(f"Starting table sizing analysis for {environment}...")
        report_file = analyzer.generate_sizing_report(environment)
        print(f"Sizing analysis complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Table sizing analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
