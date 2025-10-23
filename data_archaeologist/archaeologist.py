"""
Data Archaeologist - Complete Database Discovery Orchestrator

This is the main orchestrator that runs the complete three-layer discovery process
for database archaeology and business intelligence inference.
"""

import logging
import asyncio
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Any, Optional
import json
import sys

from .core import DatabaseConnection, setup_logging, ArchaeologyReport
from .layer1_physical import DatabaseInventory, TableSizingAnalyzer, ColumnProfiler
from .layer2_logical import PrimaryKeyDetective, ForeignKeyDetective, CardinalityAnalyzer
from .layer3_business import BusinessProcessInference

logger = logging.getLogger(__name__)


class DataArchaeologist:
    """Complete database archaeology and discovery orchestrator."""
    
    def __init__(self, config_path: str = 'config.json'):
        self.config_path = config_path
        self.db_connection = DatabaseConnection(config_path)
        self.results = {}
    
    def run_complete_discovery(self, environment: str, 
                             parallel_execution: bool = True) -> Dict[str, Any]:
        """Run the complete three-layer discovery process."""
        logger.info(f"Starting complete database archaeology for {environment}")
        
        discovery_start = datetime.now()
        
        try:
            if parallel_execution:
                results = self._run_parallel_discovery(environment)
            else:
                results = self._run_sequential_discovery(environment)
            
            discovery_duration = datetime.now() - discovery_start
            
            # Compile comprehensive report
            comprehensive_report = self._compile_comprehensive_report(
                environment, results, discovery_duration
            )
            
            self.results[environment] = comprehensive_report
            
            logger.info(f"Complete discovery finished in {discovery_duration.total_seconds():.2f} seconds")
            
            return comprehensive_report
            
        except Exception as e:
            logger.error(f"Complete discovery failed for {environment}: {e}")
            raise
    
    def _run_parallel_discovery(self, environment: str) -> Dict[str, Any]:
        """Run discovery with parallel execution of independent analyses."""
        logger.info("Executing parallel discovery process")
        
        results = {}
        
        # Layer 1: Physical analysis (can run in parallel)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            layer1_futures = {
                'database_inventory': executor.submit(self._run_database_inventory, environment),
                'table_sizing': executor.submit(self._run_table_sizing, environment),
                'column_profiling': executor.submit(self._run_column_profiling, environment)
            }
            
            # Collect Layer 1 results
            for analysis_type, future in layer1_futures.items():
                try:
                    results[analysis_type] = future.result()
                    logger.info(f"Completed {analysis_type}")
                except Exception as e:
                    logger.error(f"Failed {analysis_type}: {e}")
                    results[analysis_type] = {'error': str(e)}
        
        # Layer 2: Logical analysis (depends on Layer 1, can run in parallel)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            layer2_futures = {
                'primary_key_detection': executor.submit(self._run_primary_key_detection, environment),
                'foreign_key_detection': executor.submit(self._run_foreign_key_detection, environment),
                'cardinality_analysis': executor.submit(self._run_cardinality_analysis, environment)
            }
            
            # Collect Layer 2 results
            for analysis_type, future in layer2_futures.items():
                try:
                    results[analysis_type] = future.result()
                    logger.info(f"Completed {analysis_type}")
                except Exception as e:
                    logger.error(f"Failed {analysis_type}: {e}")
                    results[analysis_type] = {'error': str(e)}
        
        # Layer 3: Business analysis (depends on Layer 1 & 2)
        try:
            results['business_inference'] = self._run_business_inference(environment)
            logger.info("Completed business_inference")
        except Exception as e:
            logger.error(f"Failed business_inference: {e}")
            results['business_inference'] = {'error': str(e)}
        
        return results
    
    def _run_sequential_discovery(self, environment: str) -> Dict[str, Any]:
        """Run discovery with sequential execution."""
        logger.info("Executing sequential discovery process")
        
        results = {}
        
        # Layer 1: Physical Map Discovery
        logger.info("Starting Layer 1: Physical Map Discovery")
        
        try:
            results['database_inventory'] = self._run_database_inventory(environment)
            results['table_sizing'] = self._run_table_sizing(environment)
            results['column_profiling'] = self._run_column_profiling(environment)
            logger.info("Layer 1 complete")
        except Exception as e:
            logger.error(f"Layer 1 failed: {e}")
            raise
        
        # Layer 2: Logical Blueprint Discovery
        logger.info("Starting Layer 2: Logical Blueprint Discovery")
        
        try:
            results['primary_key_detection'] = self._run_primary_key_detection(environment)
            results['foreign_key_detection'] = self._run_foreign_key_detection(environment)
            results['cardinality_analysis'] = self._run_cardinality_analysis(environment)
            logger.info("Layer 2 complete")
        except Exception as e:
            logger.error(f"Layer 2 failed: {e}")
            raise
        
        # Layer 3: Business Story Discovery
        logger.info("Starting Layer 3: Business Story Discovery")
        
        try:
            results['business_inference'] = self._run_business_inference(environment)
            logger.info("Layer 3 complete")
        except Exception as e:
            logger.error(f"Layer 3 failed: {e}")
            raise
        
        return results
    
    def _run_database_inventory(self, environment: str) -> Dict[str, Any]:
        """Execute database inventory analysis."""
        analyzer = DatabaseInventory(self.db_connection)
        
        inventory_data = {
            'databases': analyzer.discover_databases(environment),
            'schemas': analyzer.discover_schemas(environment),
            'table_distribution': analyzer.get_schema_table_counts(environment)
        }
        
        return inventory_data
    
    def _run_table_sizing(self, environment: str) -> Dict[str, Any]:
        """Execute table sizing analysis."""
        analyzer = TableSizingAnalyzer(self.db_connection)
        
        sizing_data = {
            'size_analysis': analyzer.analyze_table_sizes(environment),
            'row_analysis': analyzer.analyze_row_counts(environment),
            'activity_patterns': analyzer.analyze_table_activity_patterns(environment)
        }
        
        return sizing_data
    
    def _run_column_profiling(self, environment: str) -> Dict[str, Any]:
        """Execute column profiling analysis."""
        profiler = ColumnProfiler(self.db_connection)
        
        # Profile top schemas only to avoid overwhelming analysis
        schemas_query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        AND schema_name NOT LIKE 'pg_%'
        ORDER BY schema_name
        LIMIT 3
        """
        
        schemas = self.db_connection.execute_query(environment, schemas_query)
        
        column_data = {}
        for schema in schemas:
            schema_name = schema['schema_name']
            try:
                column_data[schema_name] = profiler.analyze_schema_column_patterns(
                    environment, schema_name, 10
                )
            except Exception as e:
                logger.warning(f"Column profiling failed for schema {schema_name}: {e}")
                column_data[schema_name] = {'error': str(e)}
        
        return column_data
    
    def _run_primary_key_detection(self, environment: str) -> Dict[str, Any]:
        """Execute primary key detection analysis."""
        detective = PrimaryKeyDetective(self.db_connection)
        return detective.compare_declared_vs_natural_keys(environment)
    
    def _run_foreign_key_detection(self, environment: str) -> Dict[str, Any]:
        """Execute foreign key detection analysis."""
        detective = ForeignKeyDetective(self.db_connection)
        
        fk_data = {
            'declared_foreign_keys': detective.discover_declared_foreign_keys(environment),
            'potential_foreign_keys': detective.discover_potential_foreign_keys(environment)
        }
        
        return fk_data
    
    def _run_cardinality_analysis(self, environment: str) -> Dict[str, Any]:
        """Execute cardinality analysis."""
        analyzer = CardinalityAnalyzer(self.db_connection)
        return analyzer.analyze_all_relationships(environment)
    
    def _run_business_inference(self, environment: str) -> Dict[str, Any]:
        """Execute business process inference."""
        inferencer = BusinessProcessInference(self.db_connection)
        return inferencer.infer_business_processes(environment)
    
    def _compile_comprehensive_report(self, environment: str, 
                                    results: Dict[str, Any], 
                                    duration: Any) -> Dict[str, Any]:
        """Compile all analysis results into a comprehensive report."""
        
        # Extract key insights from each layer
        layer1_insights = self._extract_layer1_insights(results)
        layer2_insights = self._extract_layer2_insights(results)
        layer3_insights = self._extract_layer3_insights(results)
        
        # Generate executive summary
        executive_summary = self._generate_executive_summary(
            environment, layer1_insights, layer2_insights, layer3_insights
        )
        
        return {
            'metadata': {
                'environment': environment,
                'analysis_timestamp': datetime.now().isoformat(),
                'analysis_duration_seconds': duration.total_seconds(),
                'analysis_version': '1.0.0'
            },
            'executive_summary': executive_summary,
            'layer1_physical_analysis': {
                'insights': layer1_insights,
                'detailed_results': {
                    'database_inventory': results.get('database_inventory'),
                    'table_sizing': results.get('table_sizing'),
                    'column_profiling': results.get('column_profiling')
                }
            },
            'layer2_logical_analysis': {
                'insights': layer2_insights,
                'detailed_results': {
                    'primary_key_detection': results.get('primary_key_detection'),
                    'foreign_key_detection': results.get('foreign_key_detection'),
                    'cardinality_analysis': results.get('cardinality_analysis')
                }
            },
            'layer3_business_analysis': {
                'insights': layer3_insights,
                'detailed_results': {
                    'business_inference': results.get('business_inference')
                }
            }
        }
    
    def _extract_layer1_insights(self, results: Dict[str, Any]) -> List[str]:
        """Extract key insights from Layer 1 analysis."""
        insights = []
        
        # Database inventory insights
        if 'database_inventory' in results:
            inv = results['database_inventory']
            if 'schemas' in inv:
                user_schemas = inv['schemas'].get('user_schemas', 0)
                insights.append(f"Database contains {user_schemas} user-defined schemas")
        
        # Table sizing insights
        if 'table_sizing' in results:
            sizing = results['table_sizing']
            if 'size_analysis' in sizing:
                large_tables = sizing['size_analysis'].get('size_categories', {}).get('large_tables_100mb_plus', 0)
                if large_tables > 0:
                    insights.append(f"Found {large_tables} large tables (>100MB) indicating significant data volume")
        
        return insights
    
    def _extract_layer2_insights(self, results: Dict[str, Any]) -> List[str]:
        """Extract key insights from Layer 2 analysis."""
        insights = []
        
        # Primary key insights
        if 'primary_key_detection' in results:
            pk = results['primary_key_detection']
            if 'comparison_summary' in pk:
                tables_without_pk = pk['comparison_summary'].get('tables_without_any_key', 0)
                if tables_without_pk > 0:
                    insights.append(f"Found {tables_without_pk} tables without primary keys - data integrity risk")
        
        # Foreign key insights
        if 'foreign_key_detection' in results:
            fk = results['foreign_key_detection']
            if 'potential_foreign_keys' in fk:
                high_conf = len(fk['potential_foreign_keys'].get('confidence_categorization', {}).get('high_confidence', []))
                if high_conf > 0:
                    insights.append(f"Identified {high_conf} high-confidence undeclared foreign key relationships")
        
        return insights
    
    def _extract_layer3_insights(self, results: Dict[str, Any]) -> List[str]:
        """Extract key insights from Layer 3 analysis."""
        insights = []
        
        if 'business_inference' in results:
            business = results['business_inference']
            if 'business_insights' in business:
                insights.extend(business['business_insights'])
        
        return insights
    
    def _generate_executive_summary(self, environment: str, 
                                  layer1: List[str], layer2: List[str], 
                                  layer3: List[str]) -> Dict[str, Any]:
        """Generate executive summary of the database archaeology."""
        
        all_insights = layer1 + layer2 + layer3
        
        return {
            'environment': environment,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'key_findings': all_insights[:10],  # Top 10 insights
            'recommendations': [
                'Review tables without primary keys for data integrity improvements',
                'Consider formalizing discovered foreign key relationships',
                'Implement data quality monitoring for high-NULL columns',
                'Optimize queries for large tables identified in analysis'
            ],
            'architecture_assessment': 'Professional database structure with identified optimization opportunities'
        }
    
    def export_comprehensive_report(self, environment: str) -> str:
        """Export comprehensive archaeology report."""
        if environment not in self.results:
            raise ValueError(f"No results available for environment {environment}")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"data_archaeology_complete_{environment}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.results[environment], f, indent=2, default=str)
            
            logger.info(f"Comprehensive report exported: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to export comprehensive report: {e}")
            raise


def main():
    """Professional command-line interface for complete database archaeology."""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Data Archaeologist - Professional Database Discovery Framework",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --environment staging
  %(prog)s --environment production --sequential
  %(prog)s --environment staging --config custom_config.json
        """
    )
    
    parser.add_argument(
        '--environment',
        required=True,
        help='Target database environment (staging, production, etc.)'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Configuration file path (default: config.json)'
    )
    
    parser.add_argument(
        '--sequential',
        action='store_true',
        help='Run analysis sequentially instead of parallel execution'
    )
    
    parser.add_argument(
        '--output-dir',
        default='reports',
        help='Output directory for reports (default: reports)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress console output except errors'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = getattr(logging, args.log_level)
    setup_logging('data_archaeology_complete.log', log_level)
    
    try:
        # Validate environment exists
        db_connection = DatabaseConnection(args.config)
        available_envs = db_connection.get_available_environments()
        
        if args.environment not in available_envs:
            print(f"Error: Environment '{args.environment}' not found in configuration.")
            print(f"Available environments: {', '.join(available_envs)}")
            sys.exit(1)
        
        # Create output directory
        import os
        os.makedirs(args.output_dir, exist_ok=True)
        
        archaeologist = DataArchaeologist(args.config)
        
        if not args.quiet:
            print(f"Data Archaeologist - Professional Database Discovery")
            print(f"Environment: {args.environment}")
            print(f"Execution Mode: {'Sequential' if args.sequential else 'Parallel'}")
            print(f"Output Directory: {args.output_dir}")
            print("-" * 60)
        
        # Run complete discovery
        results = archaeologist.run_complete_discovery(
            args.environment, 
            parallel_execution=not args.sequential
        )
        
        # Export comprehensive report
        report_file = archaeologist.export_comprehensive_report(args.environment)
        
        # Move report to output directory
        import shutil
        final_report_path = os.path.join(args.output_dir, os.path.basename(report_file))
        shutil.move(report_file, final_report_path)
        
        if not args.quiet:
            print(f"\nDatabase archaeology completed successfully!")
            print(f"Report: {final_report_path}")
            
            # Display executive summary
            exec_summary = results.get('executive_summary', {})
            print(f"\nExecutive Summary:")
            print(f"Environment: {exec_summary.get('environment')}")
            print(f"Analysis Date: {exec_summary.get('analysis_date')}")
            print(f"\nKey Findings:")
            for finding in exec_summary.get('key_findings', [])[:5]:
                print(f"  • {finding}")
        
        return 0
        
    except KeyboardInterrupt:
        if not args.quiet:
            print("\nAnalysis interrupted by user.")
        return 1
        
    except Exception as e:
        logger.error(f"Database archaeology failed: {e}")
        if not args.quiet:
            print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
