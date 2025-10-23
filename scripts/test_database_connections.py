"""
Database Connection Test Tool
Professional tool to test database connectivity and validate credentials
"""

import argparse
import sys
import os
from typing import Dict

# Add the parent directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_archaeologist.core.database_connection import DatabaseConnection
from data_archaeologist.core.utils import setup_logging


def test_environment_connection(db_connection: DatabaseConnection, 
                              environment: str) -> Dict:
    """Test connection to specific environment."""
    try:
        test_result = db_connection.test_connection(environment)
        return test_result
    except Exception as e:
        return {
            'status': 'failed',
            'environment': environment,
            'error': str(e)
        }


def main():
    """Main function for connection testing."""
    parser = argparse.ArgumentParser(
        description="Database Connection Test Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --environment staging
  %(prog)s --environment production
  %(prog)s --all
        """
    )
    
    parser.add_argument(
        '--environment',
        choices=['staging', 'production'],
        help='Test specific environment'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Test all configured environments'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Configuration file path (default: config.json)'
    )
    
    args = parser.parse_args()
    
    if not args.environment and not args.all:
        parser.error("Must specify either --environment or --all")
    
    # Setup logging
    setup_logging()
    
    try:
        # Initialize database connection
        db_connection = DatabaseConnection(args.config)
        
        print("Database Connection Test Tool")
        print("=" * 50)
        
        # Test environments
        environments_to_test = []
        if args.all:
            environments_to_test = db_connection.get_available_environments()
        else:
            environments_to_test = [args.environment]
        
        all_passed = True
        
        for env in environments_to_test:
            print(f"\nTesting {env} environment...")
            print("-" * 30)
            
            result = test_environment_connection(db_connection, env)
            
            if result['status'] == 'success':
                db_info = result['database_info']
                print(f"Status: CONNECTED")
                print(f"Database: {db_info['database_name']}")
                print(f"User: {db_info['connected_user']}")
                pg_version = db_info['postgresql_version'].split(',')[0]
                print(f"PostgreSQL Version: {pg_version}")
                print(f"Database Size: {db_info['database_size']}")
            else:
                print(f"Status: FAILED")
                print(f"Error: {result['error']}")
                all_passed = False
                
                # Provide helpful error messages
                if "authentication failed" in result['error'].lower():
                    print(f"Check environment variable: {env.upper()}_DB_PASSWORD")
                elif "could not connect" in result['error'].lower():
                    print("Check network connectivity and server availability")
                elif "does not exist" in result['error'].lower():
                    print("Check database name in configuration")
        
        print("\n" + "=" * 50)
        if all_passed:
            print("All connection tests PASSED")
        else:
            print("Some connection tests FAILED")
            print("\nTroubleshooting:")
            print("1. Set environment variables:")
            print("   export STAGING_DB_PASSWORD='your_password'")
            print("   export PROD_DB_PASSWORD='your_password'")
            print("2. Check network connectivity to Azure PostgreSQL")
            print("3. Verify credentials and database configuration")
        
        return 0 if all_passed else 1
    
    except Exception as e:
        print(f"Connection test failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
