"""
Layer 3: Business Story Discovery
Business Process Inference Module

This module synthesizes physical and logical analysis to infer
business processes and understand the business story behind the data.
"""

import logging
import sys
import os
from typing import Dict, List, Any, Optional, Set

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


class BusinessProcessInference:
    """Business process inference from database structure and relationships."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
    
    def identify_business_domains(self, environment: str) -> Dict[str, Any]:
        """Identify business domains based on table clusters and naming patterns."""
        logger.info(f"Identifying business domains for {environment}")
        
        try:
            # Get all tables with their characteristics
            tables_query = """
            SELECT 
                t.table_schema,
                t.table_name,
                COALESCE(s.n_live_tup, 0) as estimated_rows,
                pg_size_pretty(pg_total_relation_size(t.table_schema||'.'||t.table_name)) as table_size
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s 
                ON t.table_schema = s.schemaname AND t.table_name = s.tablename
            WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
            AND t.table_schema NOT LIKE 'pg_%'
            AND t.table_type = 'BASE TABLE'
            ORDER BY COALESCE(s.n_live_tup, 0) DESC
            """
            
            tables = self.db_connection.execute_query(environment, tables_query)
            
            # Analyze naming patterns to identify domains
            domain_clusters = self._cluster_tables_by_domain(tables)
            
            # Analyze each domain
            domain_analysis = {}
            for domain_name, domain_tables in domain_clusters.items():
                domain_analysis[domain_name] = self._analyze_business_domain(
                    environment, domain_name, domain_tables
                )
            
            logger.info(f"Identified {len(domain_clusters)} business domains")
            
            return {
                'environment': environment,
                'summary': {
                    'total_tables_analyzed': len(tables),
                    'business_domains_identified': len(domain_clusters),
                    'domain_names': list(domain_clusters.keys())
                },
                'domain_clusters': domain_clusters,
                'domain_analysis': domain_analysis
            }
            
        except Exception as e:
            logger.error(f"Business domain identification failed for {environment}: {e}")
            raise
    
    def _cluster_tables_by_domain(self, tables: List[Dict]) -> Dict[str, List[Dict]]:
        """Cluster tables into business domains based on naming patterns."""
        domains = {
            'user_management': [],
            'order_management': [],
            'product_catalog': [],
            'content_management': [],
            'audit_logging': [],
            'configuration': [],
            'reporting': [],
            'integration': [],
            'financial': [],
            'security': [],
            'uncategorized': []
        }
        
        # Define patterns for each domain
        domain_patterns = {
            'user_management': ['user', 'account', 'profile', 'auth', 'member', 'customer', 'person'],
            'order_management': ['order', 'cart', 'purchase', 'transaction', 'payment', 'invoice', 'billing'],
            'product_catalog': ['product', 'item', 'catalog', 'inventory', 'stock', 'category', 'brand'],
            'content_management': ['content', 'article', 'post', 'media', 'file', 'document', 'page'],
            'audit_logging': ['log', 'audit', 'event', 'activity', 'history', 'trace'],
            'configuration': ['config', 'setting', 'parameter', 'option', 'preference'],
            'reporting': ['report', 'dashboard', 'metric', 'analytics', 'stat'],
            'integration': ['api', 'webhook', 'sync', 'import', 'export', 'feed'],
            'financial': ['price', 'cost', 'revenue', 'financial', 'accounting', 'tax'],
            'security': ['permission', 'role', 'access', 'token', 'session', 'credential']
        }
        
        for table in tables:
            table_name = table['table_name'].lower()
            categorized = False
            
            # Check each domain pattern
            for domain, patterns in domain_patterns.items():
                if any(pattern in table_name for pattern in patterns):
                    domains[domain].append(table)
                    categorized = True
                    break
            
            # If no pattern matches, add to uncategorized
            if not categorized:
                domains['uncategorized'].append(table)
        
        # Remove empty domains
        return {k: v for k, v in domains.items() if v}
    
    def _analyze_business_domain(self, environment: str, 
                               domain_name: str, tables: List[Dict]) -> Dict[str, Any]:
        """Analyze a specific business domain."""
        try:
            # Get relationships within this domain
            domain_relationships = self._get_domain_relationships(environment, tables)
            
            # Identify core entities (largest tables)
            core_entities = sorted(tables, key=lambda x: x['estimated_rows'], reverse=True)[:5]
            
            # Identify fact vs dimension patterns
            fact_tables = [t for t in tables if t['estimated_rows'] > 10000]
            dimension_tables = [t for t in tables if t['estimated_rows'] <= 10000]
            
            # Analyze temporal patterns
            temporal_analysis = self._analyze_temporal_patterns(environment, tables)
            
            return {
                'domain_name': domain_name,
                'table_count': len(tables),
                'core_entities': core_entities,
                'fact_tables': len(fact_tables),
                'dimension_tables': len(dimension_tables),
                'relationships': domain_relationships,
                'temporal_patterns': temporal_analysis,
                'business_insights': self._generate_domain_insights(
                    domain_name, tables, domain_relationships
                )
            }
            
        except Exception as e:
            logger.warning(f"Failed to analyze domain {domain_name}: {e}")
            return {'domain_name': domain_name, 'analysis_failed': True, 'error': str(e)}
    
    def _get_domain_relationships(self, environment: str, 
                                tables: List[Dict]) -> Dict[str, Any]:
        """Get relationships between tables within a domain."""
        try:
            # Create table set for this domain
            domain_table_set = set(
                (t['table_schema'], t['table_name']) for t in tables
            )
            
            # Get foreign key relationships within domain
            fk_query = """
            SELECT 
                tc.table_schema as source_schema,
                tc.table_name as source_table,
                kcu.column_name as source_column,
                ccu.table_schema as target_schema,
                ccu.table_name as target_table,
                ccu.column_name as target_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
            AND tc.table_schema NOT LIKE 'pg_%'
            """
            
            all_relationships = self.db_connection.execute_query(environment, fk_query)
            
            # Filter to relationships within this domain
            domain_relationships = []
            for rel in all_relationships:
                source_table = (rel['source_schema'], rel['source_table'])
                target_table = (rel['target_schema'], rel['target_table'])
                
                if source_table in domain_table_set and target_table in domain_table_set:
                    domain_relationships.append(rel)
            
            return {
                'internal_relationships': len(domain_relationships),
                'relationship_details': domain_relationships
            }
            
        except Exception as e:
            logger.warning(f"Failed to get domain relationships: {e}")
            return {'internal_relationships': 0, 'relationship_details': []}
    
    def _analyze_temporal_patterns(self, environment: str, 
                                 tables: List[Dict]) -> Dict[str, Any]:
        """Analyze temporal patterns in domain tables."""
        temporal_columns = []
        
        for table in tables:
            try:
                # Look for timestamp columns
                timestamp_query = """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                AND (data_type LIKE '%timestamp%' OR data_type = 'date'
                     OR column_name ILIKE '%date%' OR column_name ILIKE '%time%'
                     OR column_name ILIKE '%created%' OR column_name ILIKE '%updated%')
                """
                
                columns = self.db_connection.execute_query(
                    environment, 
                    timestamp_query, 
                    (table['table_schema'], table['table_name'])
                )
                
                for col in columns:
                    temporal_columns.append({
                        'table': f"{table['table_schema']}.{table['table_name']}",
                        'column': col['column_name'],
                        'data_type': col['data_type'],
                        'pattern_type': self._classify_temporal_column(col['column_name'])
                    })
                    
            except Exception as e:
                logger.warning(f"Failed to analyze temporal patterns for table {table['table_name']}: {e}")
                continue
        
        # Categorize temporal patterns
        audit_columns = [c for c in temporal_columns if c['pattern_type'] == 'audit']
        business_columns = [c for c in temporal_columns if c['pattern_type'] == 'business']
        
        return {
            'total_temporal_columns': len(temporal_columns),
            'audit_timestamp_columns': len(audit_columns),
            'business_timestamp_columns': len(business_columns),
            'temporal_column_details': temporal_columns
        }
    
    def _classify_temporal_column(self, column_name: str) -> str:
        """Classify temporal column by purpose."""
        col_lower = column_name.lower()
        
        audit_patterns = ['created', 'updated', 'modified', 'deleted', 'inserted']
        if any(pattern in col_lower for pattern in audit_patterns):
            return 'audit'
        
        business_patterns = ['start', 'end', 'due', 'expire', 'schedule', 'delivery']
        if any(pattern in col_lower for pattern in business_patterns):
            return 'business'
        
        return 'unknown'
    
    def _generate_domain_insights(self, domain_name: str, tables: List[Dict], 
                                relationships: Dict) -> List[str]:
        """Generate business insights for a domain."""
        insights = []
        
        table_count = len(tables)
        relationship_count = relationships.get('internal_relationships', 0)
        
        # Domain complexity insights
        if table_count > 10:
            insights.append(f"Complex domain with {table_count} tables suggests sophisticated business processes")
        elif table_count < 3:
            insights.append(f"Simple domain with {table_count} tables indicates focused business area")
        
        # Relationship density insights
        if relationship_count > 0:
            density = relationship_count / table_count if table_count > 0 else 0
            if density > 1:
                insights.append("High relationship density indicates tightly coupled business processes")
            elif density < 0.3:
                insights.append("Low relationship density suggests independent business entities")
        else:
            insights.append("No internal relationships found - may indicate data integration opportunities")
        
        # Domain-specific insights
        if domain_name == 'user_management':
            insights.append("User management domain - core to application identity and access")
        elif domain_name == 'order_management':
            insights.append("Order management domain - critical for revenue and customer experience")
        elif domain_name == 'audit_logging':
            insights.append("Audit domain - important for compliance and debugging")
        elif domain_name == 'uncategorized':
            insights.append("Uncategorized tables may represent specialized business logic or legacy systems")
        
        return insights
    
    def infer_business_processes(self, environment: str) -> Dict[str, Any]:
        """Infer complete business processes from domain analysis."""
        logger.info(f"Inferring business processes for {environment}")
        
        try:
            # Get domain analysis
            domain_analysis = self.identify_business_domains(environment)
            
            # Identify cross-domain relationships
            cross_domain_relationships = self._analyze_cross_domain_relationships(
                environment, domain_analysis
            )
            
            # Infer process flows
            process_flows = self._infer_process_flows(
                domain_analysis, cross_domain_relationships
            )
            
            # Generate business insights
            business_insights = self._generate_business_insights(
                domain_analysis, cross_domain_relationships, process_flows
            )
            
            return {
                'environment': environment,
                'domain_analysis': domain_analysis,
                'cross_domain_relationships': cross_domain_relationships,
                'inferred_process_flows': process_flows,
                'business_insights': business_insights
            }
            
        except Exception as e:
            logger.error(f"Business process inference failed for {environment}: {e}")
            raise
    
    def _analyze_cross_domain_relationships(self, environment: str, 
                                          domain_analysis: Dict) -> Dict[str, Any]:
        """Analyze relationships that cross business domain boundaries."""
        try:
            # Get all foreign key relationships
            all_fk_query = """
            SELECT 
                tc.table_schema as source_schema,
                tc.table_name as source_table,
                kcu.column_name as source_column,
                ccu.table_schema as target_schema,
                ccu.table_name as target_table,
                ccu.column_name as target_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
            AND tc.table_schema NOT LIKE 'pg_%'
            """
            
            all_relationships = self.db_connection.execute_query(environment, all_fk_query)
            
            # Create domain lookup
            table_to_domain = {}
            for domain_name, domain_data in domain_analysis['domain_analysis'].items():
                if 'core_entities' in domain_data:
                    for table in domain_data.get('tables', []):
                        table_key = (table['table_schema'], table['table_name'])
                        table_to_domain[table_key] = domain_name
            
            # Find cross-domain relationships
            cross_domain_rels = []
            for rel in all_relationships:
                source_key = (rel['source_schema'], rel['source_table'])
                target_key = (rel['target_schema'], rel['target_table'])
                
                source_domain = table_to_domain.get(source_key, 'unknown')
                target_domain = table_to_domain.get(target_key, 'unknown')
                
                if source_domain != target_domain and source_domain != 'unknown' and target_domain != 'unknown':
                    cross_domain_rels.append({
                        **rel,
                        'source_domain': source_domain,
                        'target_domain': target_domain
                    })
            
            return {
                'total_cross_domain_relationships': len(cross_domain_rels),
                'relationship_details': cross_domain_rels
            }
            
        except Exception as e:
            logger.warning(f"Cross-domain relationship analysis failed: {e}")
            return {'total_cross_domain_relationships': 0, 'relationship_details': []}
    
    def _infer_process_flows(self, domain_analysis: Dict, 
                           cross_domain_rels: Dict) -> List[Dict[str, Any]]:
        """Infer business process flows from domain and relationship analysis."""
        process_flows = []
        
        # Common business process patterns
        patterns = [
            {
                'name': 'User Registration and Management',
                'domains': ['user_management', 'security'],
                'description': 'User account creation, authentication, and profile management'
            },
            {
                'name': 'Order Processing',
                'domains': ['user_management', 'product_catalog', 'order_management', 'financial'],
                'description': 'Complete order lifecycle from product selection to payment'
            },
            {
                'name': 'Content Publishing',
                'domains': ['user_management', 'content_management'],
                'description': 'Content creation, review, and publication workflow'
            },
            {
                'name': 'Inventory Management',
                'domains': ['product_catalog', 'order_management'],
                'description': 'Stock tracking and inventory updates based on orders'
            }
        ]
        
        available_domains = set(domain_analysis.get('domain_analysis', {}).keys())
        
        for pattern in patterns:
            pattern_domains = set(pattern['domains'])
            if pattern_domains.issubset(available_domains):
                # Check if there are cross-domain relationships supporting this flow
                supporting_relationships = [
                    rel for rel in cross_domain_rels.get('relationship_details', [])
                    if rel['source_domain'] in pattern_domains and rel['target_domain'] in pattern_domains
                ]
                
                process_flows.append({
                    'process_name': pattern['name'],
                    'involved_domains': pattern['domains'],
                    'description': pattern['description'],
                    'supporting_relationships': len(supporting_relationships),
                    'confidence_score': min(100, len(supporting_relationships) * 25)
                })
        
        return process_flows
    
    def _generate_business_insights(self, domain_analysis: Dict, 
                                  cross_domain_rels: Dict, 
                                  process_flows: List[Dict]) -> List[str]:
        """Generate high-level business insights."""
        insights = []
        
        domain_count = len(domain_analysis.get('domain_analysis', {}))
        cross_domain_count = cross_domain_rels.get('total_cross_domain_relationships', 0)
        process_count = len(process_flows)
        
        # Overall architecture insights
        if domain_count > 8:
            insights.append(f"Complex business architecture with {domain_count} distinct domains")
        elif domain_count < 4:
            insights.append(f"Focused business model with {domain_count} core domains")
        
        # Integration insights
        if cross_domain_count > domain_count * 2:
            insights.append("Highly integrated system with strong cross-domain dependencies")
        elif cross_domain_count < domain_count:
            insights.append("Loosely coupled domains - may indicate microservices architecture")
        
        # Process maturity insights
        high_confidence_processes = [p for p in process_flows if p['confidence_score'] > 75]
        if len(high_confidence_processes) > 2:
            insights.append(f"Mature business processes detected: {', '.join(p['process_name'] for p in high_confidence_processes)}")
        
        # Data model insights
        if 'user_management' in domain_analysis.get('domain_analysis', {}):
            insights.append("User-centric business model with identity management foundation")
        
        if 'order_management' in domain_analysis.get('domain_analysis', {}):
            insights.append("Transactional business model with order processing capabilities")
        
        return insights
    
    def generate_business_story_report(self, environment: str) -> str:
        """Generate comprehensive business story analysis report."""
        logger.info(f"Generating business story report for {environment}")
        
        report = ArchaeologyReport(environment)
        
        try:
            # Perform comprehensive business analysis
            business_analysis = self.infer_business_processes(environment)
            
            # Add sections to report
            report.add_section('business_process_analysis', business_analysis)
            
            # Export report
            filename = report.export('layer3_business_story')
            logger.info(f"Business story report exported: {filename}")
            
            return filename
            
        except Exception as e:
            logger.error(f"Business story report generation failed: {e}")
            raise


def main():
    """Command-line interface for business story analysis."""
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
        print("Usage: python -m data_archaeologist.layer3_business.business_inference <environment>")
        sys.exit(1)
    
    environment = sys.argv[1]
    
    try:
        db_conn = DatabaseConnection()
        analyzer = BusinessProcessInference(db_conn)
        
        print(f"Starting business story analysis for {environment}...")
        report_file = analyzer.generate_business_story_report(environment)
        print(f"Business story analysis complete. Report saved: {report_file}")
        
    except Exception as e:
        logger.error(f"Business story analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
