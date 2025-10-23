"""
Professional Database Summary Tool
Real-time database analysis for PostgreSQL environments
"""

import argparse
import json
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List
from enum import Enum

# Add the parent directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_archaeologist.core.database_connection import DatabaseConnection
from data_archaeologist.core.utils import setup_logging


class Environment(Enum):
    """Supported database environments."""
    STAGING = "staging"
    PRODUCTION = "production"
    
    @classmethod
    def choices(cls) -> List[str]:
        return [env.value for env in cls]


class OutputFormat(Enum):
    """Supported output formats."""
    CONSOLE = "console"
    JSON = "json"
    CSV = "csv"
    
    @classmethod
    def choices(cls) -> List[str]:
        return [fmt.value for fmt in cls]


def get_table_summary(db_connection: DatabaseConnection,
                     environment: str) -> List[Dict]:
    """Get table size and row count summary from real database."""
    query = """
    SELECT 
        t.table_schema as schema_name,
        t.table_name as table_name,
        COALESCE(s.n_live_tup, 0) as estimated_rows,
        pg_total_relation_size(
            quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)
        ) as size_bytes,
        pg_size_pretty(
            pg_total_relation_size(
                quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)
            )
        ) as size_human
    FROM information_schema.tables t
    LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name 
        AND s.schemaname = t.table_schema
    WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    AND t.table_type = 'BASE TABLE'
    ORDER BY pg_total_relation_size(
        quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)
    ) DESC
    """
    
    try:
        results = db_connection.execute_query(environment, query)
        
        # Format the results
        formatted_results = []
        for row in results:
            formatted_results.append({
                'schema': row['schema_name'],
                'table': row['table_name'],
                'full_table_name': f"{row['schema_name']}.{row['table_name']}",
                'rows': int(row['estimated_rows']) if row['estimated_rows'] else 0,
                'size_bytes': int(row['size_bytes']) if row['size_bytes'] else 0,
                'size_human': row['size_human']
            })
        
        return formatted_results
        
    except Exception as e:
        logging.error(f"Failed to get table summary from {environment}: {e}")
        raise


def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable format."""
    if bytes_value == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(bytes_value)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"


def print_console_report(results: List[Dict], environment: str):
    """Print professional console report."""
    total_tables = len(results)
    total_rows = sum(item['rows'] for item in results)
    total_size = sum(item['size_bytes'] for item in results)
    
    print(f"\n{'='*80}")
    print(f"DATABASE ANALYSIS REPORT - {environment.upper()}")
    print("Server: Real Database Connection")
    print(f"{'='*80}")
    print(f"Total Tables: {total_tables:,}")
    print(f"Total Rows: {total_rows:,}")
    print(f"Total Size: {format_bytes(total_size)}")
    print(f"{'='*80}")
    
    if results:
        print(f"\n{'Table':<40} {'Rows':<15} {'Size':<15}")
        print("-" * 70)
        
        for item in results[:20]:  # Show top 20 tables
            table_name = item['full_table_name']
            if len(table_name) > 40:
                table_name = table_name[:38] + ".."
            print(f"{table_name:<40} {item['rows']:<15,} "
                  f"{item['size_human']:<15}")
        
        if len(results) > 20:
            print(f"\n... and {len(results) - 20} more tables")
    else:
        print("\nNo tables found in the database.")
    
    print(f"\n{'='*80}")
    print("Analysis of REAL DATABASE completed successfully")
    print(f"{'='*80}\n")


def generate_json_report(results: List[Dict], environment: str) -> str:
    """Generate structured JSON report."""
    total_tables = len(results)
    total_rows = sum(item['rows'] for item in results)
    total_size = sum(item['size_bytes'] for item in results)
    
    report = {
        'metadata': {
            'environment': environment,
            'server_type': 'real_database',
            'total_tables': total_tables,
            'total_rows': total_rows,
            'total_size_bytes': total_size,
            'total_size_human': format_bytes(total_size),
            'analysis_timestamp': datetime.now().isoformat()
        },
        'tables': results
    }
    
    return json.dumps(report, indent=2)


def generate_csv_report(results: List[Dict], environment: str) -> str:
    """Generate CSV format report."""
    csv_lines = [
        "Schema,Table,Full_Table_Name,Rows,Size_Bytes,Size_Human_Readable"
    ]
    
    for item in results:
        csv_lines.append(
            f"{item['schema']},{item['table']},{item['full_table_name']},"
            f"{item['rows']},{item['size_bytes']},{item['size_human']}"
        )
    
    return "\n".join(csv_lines)


def test_database_connection(db_connection: DatabaseConnection, 
                           environment: str) -> bool:
    """Test database connection and return status."""
    try:
        test_result = db_connection.test_connection(environment)
        if test_result['status'] == 'success':
            db_info = test_result['database_info']
            print(f"Connected to {environment} successfully")
            print(f"   Database: {db_info['database_name']}")
            print(f"   User: {db_info['connected_user']}")
            pg_version = db_info['postgresql_version'].split(',')[0]
            print(f"   PostgreSQL Version: {pg_version}")
            print(f"   Database Size: {db_info['database_size']}")
            return True
        else:
            error_msg = test_result.get('error', 'Unknown error')
            print(f"Connection to {environment} failed: {error_msg}")
            return False
    except Exception as e:
        print(f"Connection to {environment} failed: {e}")
        return False


def main():
    """Main function for database analysis."""
    parser = argparse.ArgumentParser(
        description="Professional Database Summary Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --environment staging
  %(prog)s --environment production --format json
  %(prog)s --environment staging --format csv --output report.csv
        """
    )
    
    parser.add_argument(
        '--environment',
        choices=Environment.choices(),
        required=True,
        help='Database environment (staging or production)'
    )
    
    parser.add_argument(
        '--format',
        choices=OutputFormat.choices(),
        default='console',
        help='Output format (default: console)'
    )
    
    parser.add_argument(
        '--output',
        help='Output file path (optional)'
    )
    
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Test database connection only'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Configuration file path (default: config.json)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize database connection
        db_connection = DatabaseConnection(args.config)
        
        print("Data Archaeologist - Real Database Analysis")
        print(f"Environment: {args.environment}")
        print(f"Format: {args.format}")
        print("-" * 60)
        
        # Test connection first
        if not test_database_connection(db_connection, args.environment):
            print("\nCannot proceed without database connection.")
            print("Please check your configuration and credentials.")
            sys.exit(1)
        
        if args.test_connection:
            print("\nConnection test completed successfully.")
            return
        
        # Get table summary from real database
        print(f"\nAnalyzing tables in {args.environment} environment...")
        results = get_table_summary(db_connection, args.environment)
        
        # Generate and output report
        if args.format == 'console':
            print_console_report(results, args.environment)
        
        elif args.format == 'json':
            report = generate_json_report(results, args.environment)
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(report)
                print(f"JSON report saved to: {args.output}")
            else:
                print(report)
        
        elif args.format == 'csv':
            report = generate_csv_report(results, args.environment)
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(report)
                print(f"CSV report saved to: {args.output}")
            else:
                print(report)
    
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"Database analysis failed: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
