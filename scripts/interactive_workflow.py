#!/usr/bin/env python3
"""
Data Archaeologist Interactive Workflow
Enterprise-grade database exploration and analysis interface

Author: Senior Data Engineer
Version: 2.0
Python: 3.8+
"""

import os
import sys
import json
import time
import logging
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from psycopg2 import sql
from psycopg2.sql import SQL, Identifier

# Enterprise-grade path resolution
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Professional imports with error handling
try:
    from data_archaeologist.core.database_connection import DatabaseConnection
    from data_archaeologist.core.utils import setup_logging
    from data_archaeologist.archaeologist import DataArchaeologist
    from scripts.database_summary_real import (
        get_table_summary,
        test_database_connection,
        print_console_report
    )
    # Safe SQL composition imports
    from psycopg2.sql import SQL, Identifier, Literal
    import psycopg2.sql as sql
except ImportError as e:
    print(f"CRITICAL: Import failed - {e}")
    print("Please ensure all dependencies are installed and PYTHONPATH is configured")
    sys.exit(1)


class DatabaseExplorer:
    """
    Enterprise-grade interactive database exploration system.
    
    Features:
    - Hierarchical database navigation
    - Professional error handling
    - Comprehensive table analysis
    - Rich statistics and reporting
    """
    
    def __init__(self, config_file: str = 'config.json'):
        """Initialize the database explorer."""
        self.config_file = config_file
        self.db_connection: Optional[DatabaseConnection] = None
        self.current_environment: Optional[str] = None
        self.current_database: Optional[str] = None
        self.last_analysis: Optional[Dict] = None
        self.logger = self._setup_logging()
        
        # Non-interactive mode support
        self.non_interactive = os.getenv('EXPLORER_NON_INTERACTIVE', '').lower() in ('1', 'true', 'yes')
        
        # Analysis settings from config
        self.analysis_settings = {}
        
        # Validate configuration on startup
        self._validate_configuration()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup enterprise logging."""
        logger = logging.getLogger('DatabaseExplorer')
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _non_interactive_input(self, prompt: str = "") -> str:
        """Handle input in non-interactive mode."""
        if self.non_interactive:
            print(f"[NON-INTERACTIVE] Skipping: {prompt}")
            return ""
        else:
            return input(prompt)
        return logger
    
    def _validate_configuration(self) -> None:
        """Validate configuration file exists and is valid."""
        config_path = Path(self.config_file)
        if not config_path.exists():
            self.logger.error(f"Configuration file not found: {self.config_file}")
            print(f"ERROR: Configuration file '{self.config_file}' not found")
            print("Please ensure config.json exists in the project root")
            sys.exit(1)
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            if 'environments' not in config:
                raise ValueError("'environments' section missing from config")
            
            self.logger.info(f"Configuration validated: {len(config['environments'])} environments")
            
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Invalid configuration: {e}")
            print(f"ERROR: Configuration error: {e}")
            raise Exception(f"Configuration validation failed: {e}")
    
    def _safe_input(self, prompt: str = "") -> str:
        """Safe input handling with non-interactive mode support."""
        if self.non_interactive:
            print(f"NON-INTERACTIVE: {prompt}")
            return ""
        return input(prompt)
    
    def _pause_for_user(self, message: str = "Press Enter to continue...") -> None:
        """Pause for user input with non-interactive mode support."""
        if not self.non_interactive:
            input(f"\n{message}")
        else:
            print(f"NON-INTERACTIVE: {message}")
    
    def run(self) -> None:
        """Main application entry point."""
        try:
            self.logger.info("Starting Database Explorer")
            self._show_welcome()
            self._main_menu_loop()
        except KeyboardInterrupt:
            print("\n\nGoodbye! Database exploration session ended.")
            self.logger.info("Session ended by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            print(f"\nERROR: An unexpected error occurred: {e}")
            print("Please check the logs for details.")
            raise
        finally:
            self._cleanup()
    
    def _show_welcome(self) -> None:
        """Display professional welcome screen."""
        print("\n" + "="*70)
        print("DATA ARCHAEOLOGIST - Professional Database Explorer")
        print("="*70)
        print("Enterprise-grade database analysis and exploration platform")
        print(f"Environment: {self.current_environment or 'Not selected'}")
        print("="*70)
    
    def _main_menu_loop(self) -> None:
        """Main menu interaction loop."""
        while True:
            self._display_main_menu()
            choice = self._get_user_choice(1, 9)
            
            if choice == 1:
                self._test_all_connections()
            elif choice == 2:
                self._select_environment()
            elif choice == 3:
                self._run_database_summary()
            elif choice == 4:
                self._browse_databases()
            elif choice == 5:
                self._run_complete_analysis()
            elif choice == 6:
                self._view_last_results()
            elif choice == 7:
                self._export_results()
            elif choice == 8:
                self._configuration_management()
            elif choice == 9:
                print("Exiting Database Explorer...")
                break
    
    def _display_main_menu(self) -> None:
        """Display the main menu."""
        print(f"\n{'='*50}")
        print("MAIN MENU")
        print("="*50)
        print("1. Test Database Connections")
        print("2. Select Environment")
        print("3. Run Database Summary Analysis")
        print("4. Browse Databases -> Schemas -> Tables")
        print("5. Run Complete Database Analysis")
        print("6. View Last Analysis Results")
        print("7. Export Analysis Results")
        print("8. Configuration Management")
        print("9. Exit")
        print("-"*50)
        print(f"Current Environment: {self.current_environment or 'None selected'}")
        print("-"*50)
    
    def _get_user_choice(self, min_choice: int, max_choice: int, prompt: str = None) -> int:
        """Get and validate user input choice."""
        if prompt is None:
            prompt = f"Select option ({min_choice}-{max_choice}): "
        
        while True:
            try:
                choice = input(prompt).strip()
                choice_int = int(choice)
                
                if min_choice <= choice_int <= max_choice:
                    return choice_int
                else:
                    print(f"ERROR: Please enter a number between {min_choice} and {max_choice}")
                    
            except ValueError:
                print("ERROR: Please enter a valid number")
            except KeyboardInterrupt:
                raise
    
    def _test_all_connections(self) -> None:
        """Test connections to all configured environments."""
        print("\nTesting Database Connections")
        print("="*50)
        
        try:
            if not self.db_connection:
                self.db_connection = DatabaseConnection(self.config_file)
            
            environments = self.db_connection.get_available_environments()
            results = {}
            
            for env in environments:
                print(f"Testing {env}...")
                try:
                    success = test_database_connection(self.db_connection, env)
                    results[env] = "SUCCESS" if success else "FAILED"
                    print(f"  {results[env]}")
                except Exception as e:
                    results[env] = f"ERROR: {str(e)[:50]}..."
                    print(f"  {results[env]}")
                print()
            
            print("Connection Summary:")
            print("-"*30)
            for env, status in results.items():
                print(f"  {env}: {status}")
                
        except Exception as e:
            self.logger.error(f"Connection testing failed: {e}")
            print(f"ERROR: Connection testing failed: {e}")
        
        input("\nPress Enter to continue...")
    
    def _select_environment(self) -> None:
        """Interactive environment selection."""
        print("\nEnvironment Selection")
        print("="*50)
        
        try:
            if not self.db_connection:
                self.db_connection = DatabaseConnection(self.config_file)
            
            environments = self.db_connection.get_available_environments()
            
            print("Available Environments:")
            for i, env in enumerate(environments, 1):
                # Get environment description if available
                try:
                    with open(self.config_file, 'r') as f:
                        config = json.load(f)
                    desc = config['environments'][env].get('description', 'No description')
                    print(f"{i}. {env.title()} - {desc}")
                except:
                    print(f"{i}. {env.title()}")
            
            print(f"{len(environments)+1}. Back to main menu")
            
            choice = self._get_user_choice(1, len(environments)+1)
            
            if choice <= len(environments):
                selected_env = environments[choice-1]
                self.current_environment = selected_env
                print(f"Selected environment: {selected_env.title()}")
                self.logger.info(f"Environment selected: {selected_env}")
            
        except Exception as e:
            self.logger.error(f"Environment selection failed: {e}")
            print(f"ERROR: Environment selection failed: {e}")
        
        input("\nPress Enter to continue...")
    
    def _run_database_summary(self) -> None:
        """Run database summary analysis."""
        if not self._ensure_environment_selected():
            return
        
        print(f"\nRunning Database Summary - {self.current_environment.title()}")
        print("="*60)
        
        try:
            if not self.db_connection:
                self.db_connection = DatabaseConnection(self.config_file)
            
            # Test connection first
            print("Testing connection...")
            if not test_database_connection(self.db_connection, self.current_environment):
                print("ERROR: Cannot proceed without database connection.")
                input("\nPress Enter to continue...")
                return
            
            print("Connection successful")
            print("Analyzing database structure...")
            
            # Get table summary
            results = get_table_summary(self.db_connection, self.current_environment)
            
            if results:
                self.last_analysis = {
                    'type': 'summary',
                    'environment': self.current_environment,
                    'results': results,
                    'timestamp': time.time()
                }
                
                # Display results
                print_console_report(results, self.current_environment)
                print(f"\nAnalysis completed - {len(results)} tables analyzed")
            else:
                print("⚠️  No tables found or accessible")
            
        except Exception as e:
            self.logger.error(f"Database summary failed: {e}")
            print(f"ERROR: Analysis failed: {e}")
        
        input("\nPress Enter to continue...")
    
    def _browse_databases(self) -> None:
        """Start hierarchical database browsing."""
        if not self._ensure_environment_selected():
            return
        
        try:
            if not self.db_connection:
                self.db_connection = DatabaseConnection(self.config_file)
            
            # Test connection
            if not test_database_connection(self.db_connection, self.current_environment):
                print("ERROR: Cannot proceed without database connection.")
                input("\nPress Enter to continue...")
                return
            
            self._database_browser()
            
        except Exception as e:
            self.logger.error(f"Database browsing failed: {e}")
            print(f"ERROR: Database browsing failed: {e}")
            input("\nPress Enter to continue...")
    
    def _database_browser(self) -> None:
        """Browse available databases with proper PostgreSQL connection semantics."""
        print(f"\nDatabase Browser - {self.current_environment.title()}")
        print("="*60)
        
        try:
            # Get databases using safe connection
            query = """
            SELECT datname as database_name, 
                   pg_size_pretty(pg_database_size(datname)) as size,
                   pg_encoding_to_char(encoding) as encoding
            FROM pg_database 
            WHERE datistemplate = false 
            ORDER BY datname
            """
            
            databases = self.db_connection.execute_query(self.current_environment, query)
            
            if not databases:
                print("ERROR: No databases found")
                self._pause_for_user()
                return
            
            print("Available Databases:")
            print(f"{'#':<3} {'Database':<25} {'Size':<15} {'Encoding':<10}")
            print("-"*55)
            
            for i, db in enumerate(databases, 1):
                print(f"{i:<3} {db['database_name']:<25} {db['size']:<15} {db['encoding']:<10}")
            
            print(f"{len(databases)+1:<3} Back to main menu")
            
            if self.non_interactive:
                print("NON-INTERACTIVE: Would browse first database")
                choice = 1
            else:
                choice = self._get_user_choice(1, len(databases)+1)
            
            if choice <= len(databases):
                selected_db = databases[choice-1]['database_name']
                self.current_database = selected_db
                print(f"Selected database: {selected_db}")
                self.logger.info(f"Database selected: {selected_db}")
                
                # Browse schemas in the selected database
                self._safe_schema_browser(selected_db)
                
        except Exception as e:
            self.logger.error(f"Database browsing error: {e}")
            print(f"ERROR: Error browsing databases: {e}")
        
        self._pause_for_user()
    
    def _safe_schema_browser(self, database_name: str) -> None:
        """Browse schemas with database-specific connection."""
        print(f"\nSchema Browser - {database_name}")
        print("="*60)
        
        try:
            # Use safe SQL composition for schema browsing - PostgreSQL doesn't need database filter
            query = SQL("""
            SELECT table_schema as schema_name,
                   COUNT(table_name) as table_count
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            GROUP BY table_schema
            ORDER BY table_schema
            """)
            
            schemas = self.db_connection.execute_query(
                self.current_environment, query)
            
            if not schemas:
                print("ERROR: No user schemas found")
                self._pause_for_user()
                return
            
            print("Available Schemas:")
            print(f"{'#':<3} {'Schema':<30} {'Tables':<10}")
            print("-"*45)
            
            for i, schema in enumerate(schemas, 1):
                print(f"{i:<3} {schema['schema_name']:<30} {schema['table_count']:<10}")
            
            print(f"{len(schemas)+1:<3} Back to database browser")
            
            if self.non_interactive:
                print("NON-INTERACTIVE: Would browse first schema")
                choice = 1
            else:
                choice = self._get_user_choice(1, len(schemas)+1)
            
            if choice <= len(schemas):
                selected_schema = schemas[choice-1]['schema_name']
                print(f"Selected schema: {selected_schema}")
                self.logger.info(f"Schema selected: {selected_schema}")
                
                # Browse tables in the selected schema
                self._safe_table_browser(database_name, selected_schema)
                
        except Exception as e:
            self.logger.error(f"Schema browsing error: {e}")
            print(f"ERROR: Error browsing schemas: {e}")
            self._pause_for_user()
                
    def _safe_table_browser(self, database_name: str, schema_name: str) -> None:
        """Browse tables with safe SQL composition."""
        print(f"\nTable Browser - {database_name}.{schema_name}")
        print("="*60)
        
        try:
            # Use safe SQL composition for table browsing
            query = SQL("""
            SELECT table_name,
                   table_type,
                   COALESCE(pg_size_pretty(
                       pg_total_relation_size({schema}.{table_name})
                   ), 'N/A') as size
            FROM information_schema.tables t
            WHERE table_schema = %s
            AND table_catalog = %s
            ORDER BY 
                CASE WHEN table_type = 'BASE TABLE' THEN 1 ELSE 2 END,
                table_name
            """).format(
                schema=Identifier(schema_name),
                table_name=Identifier('table_name')
            )
            
            tables = self.db_connection.execute_query(
                self.current_environment, query, (schema_name, database_name))
            
            if not tables:
                print("ERROR: No tables found in this schema")
                self._pause_for_user()
                return
            
            print("Available Tables:")
            print(f"{'#':<3} {'Table':<30} {'Type':<15} {'Size':<12}")
            print("-"*62)
            
            for i, table in enumerate(tables, 1):
                print(f"{i:<3} {table['table_name']:<30} {table['table_type']:<15} {table.get('size', 'N/A'):<12}")
            
            print(f"{len(tables)+1:<3} Back to schema browser")
            
            if self.non_interactive:
                print("NON-INTERACTIVE: Would analyze first table")
                choice = 1
            else:
                choice = self._get_user_choice(1, len(tables)+1)
            
            if choice <= len(tables):
                selected_table = tables[choice-1]['table_name']
                print(f"Selected table: {selected_table}")
                self.logger.info(f"Table selected: {selected_table}")
                
                # Analyze the selected table
                self._table_analyzer_enhanced(database_name, schema_name, selected_table)
                
        except Exception as e:
            self.logger.error(f"Table browsing error: {e}")
            print(f"ERROR: Error browsing tables: {e}")
        
        self._pause_for_user()
    
    def _pause_for_user(self) -> None:
        """Pause execution for user acknowledgment, with non-interactive support."""
        if not self.non_interactive:
            input("\nPress Enter to continue...")
        else:
            print("[NON-INTERACTIVE] Continuing...")
    
    def _table_analyzer_enhanced(self, database_name: str, schema_name: str, table_name: str) -> None:
        """Enhanced table analysis with safe SQL composition."""
        full_table_name = f"{schema_name}.{table_name}"
        
        while True:
            print(f"\nTable Analyzer - {full_table_name}")
            print("="*60)
            print("1. Preview data (first 10 rows)")
            print("2. Show table structure")
            print("3. Column statistics")
            print("4. NULL value analysis")
            print("5. Find duplicate rows")
            print("6. Show indexes")
            print("7. Generate CREATE statement")
            print("8. Export table structure")
            print("9. Back to table browser")
            
            if self.non_interactive:
                print("NON-INTERACTIVE: Would show table structure")
                choice = 2
            else:
                choice = self._get_user_choice(1, 9)
            
            try:
                if choice == 1:
                    self._preview_table_data_safe(schema_name, table_name)
                elif choice == 2:
                    self._show_table_structure_safe(schema_name, table_name)
                elif choice == 3:
                    self._show_column_statistics_safe(schema_name, table_name)
                elif choice == 4:
                    self._analyze_null_values_safe(schema_name, table_name)
                elif choice == 5:
                    self._find_duplicate_rows_safe(schema_name, table_name)
                elif choice == 6:
                    self._show_table_indexes_safe(schema_name, table_name)
                elif choice == 7:
                    self._generate_create_statement_safe(schema_name, table_name)
                elif choice == 8:
                    self._export_table_structure_safe(schema_name, table_name)
                elif choice == 9:
                    break
                    
            except Exception as e:
                self.logger.error(f"Table analysis operation failed: {e}")
                print(f"ERROR: Operation failed: {e}")
                
            if self.non_interactive:
                break
            self._pause_for_user()
    
    def _preview_table_data_safe(self, schema_name: str, table_name: str) -> None:
        """Preview table data using safe SQL composition."""
        print(f"\nData Preview - {schema_name}.{table_name}")
        print("="*50)
        
        try:
            # Use safe SQL composition
            query = SQL("SELECT * FROM {schema}.{table} LIMIT %s").format(
                schema=Identifier(schema_name),
                table=Identifier(table_name)
            )
            
            results = self.db_connection.execute_query(
                self.current_environment, query, (10,))
            
            if results:
                print(f"Showing {len(results)} rows")
                for i, row in enumerate(results[:10], 1):
                    print(f"Row {i}: {dict(row)}")
            else:
                print("INFO: No data found in table")
                
        except Exception as e:
            self.logger.error(f"Error previewing data: {e}")
            print(f"ERROR: Error previewing data: {e}")
        
        self._pause_for_user()

    def _table_browser_safe(self, database_name: str, schema_name: str) -> None:
        """Browse tables with safe SQL composition."""
        print(f"\nTable Browser - {database_name}.{schema_name}")
        print("="*60)
        
        try:
            # Create direct connection to the specific database
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            env_config = config['environments'][self.current_environment]
            conn_params = {
                'host': env_config['host'],
                'port': env_config['port'],
                'database': database_name,
                'user': env_config['username'],
                'password': env_config['password'],
                **env_config.get('connection_args', {})
            }
            
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(cursor_factory=RealDictCursor, **conn_params)
            cursor = conn.cursor()
            
            # Get tables with safe SQL composition
            query = SQL("""
            SELECT t.table_name,
                   COALESCE(s.n_live_tup, 0) as estimated_rows,
                   pg_size_pretty(
                       pg_total_relation_size({schema} || '.' || quote_ident(t.table_name))
                   ) as size,
                   t.table_type
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s ON s.tablename = t.table_name 
                AND s.schemaname = t.table_schema
            WHERE t.table_schema = %s
            ORDER BY 
                CASE WHEN t.table_type = 'BASE TABLE' THEN 1 ELSE 2 END,
                pg_total_relation_size({schema} || '.' || quote_ident(t.table_name)) DESC NULLS LAST
            """).format(schema=Literal(schema_name))
            
            cursor.execute(query, (schema_name,))
            tables = cursor.fetchall()
            
            if not tables:
                print("ERROR: No tables found in this schema")
                self._pause_for_user()
                cursor.close()
                conn.close()
                return
            
            print("Available Tables:")
            print(f"{'#':<3} {'Table':<30} {'Type':<12} {'Rows':<12} {'Size':<12}")
            print("-"*75)
            
            for i, table in enumerate(tables, 1):
                rows_str = f"{table['estimated_rows']:,}" if table['estimated_rows'] else "0"
                table_type = table['table_type'][:10] + ".." if len(table['table_type']) > 10 else table['table_type']
                print(f"{i:<3} {table['table_name']:<30} {table_type:<12} {rows_str:<12} {table['size']:<12}")
            
            print(f"{len(tables)+1:<3} Back to schema browser")
            
            if self.non_interactive:
                print("NON-INTERACTIVE: Would analyze first table")
                choice = 1
            else:
                choice = self._get_user_choice(1, len(tables)+1)
            
            if choice <= len(tables):
                selected_table = tables[choice-1]['table_name']
                print(f"Selected table: {selected_table}")
                self.logger.info(f"Table selected: {selected_table}")
                
                cursor.close()
                conn.close()
                
                # Analyze this table
                self._table_analyzer_safe(database_name, schema_name, selected_table)
            else:
                cursor.close()
                conn.close()
            
        except Exception as e:
            self.logger.error(f"Table browsing error: {e}")
            print(f"ERROR: Error browsing tables: {e}")
            self._pause_for_user()
    
    def _schema_browser(self, database_name: str) -> None:
        """Browse schemas in selected database."""
        print(f"\nSchema Browser - {database_name}")
        print("="*60)
        
        try:
            # Get schemas with table counts
            query = """
            SELECT table_schema as schema_name,
                   COUNT(table_name) as table_count
            FROM information_schema.tables 
            WHERE table_catalog = %s
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            GROUP BY table_schema
            ORDER BY table_schema
            """
            
            schemas = self.db_connection.execute_query(
                self.current_environment, query, (database_name,))
            
            if not schemas:
                print("ERROR: No user schemas found")
                input("\nPress Enter to continue...")
                return
            
            print("Available Schemas:")
            print(f"{'#':<3} {'Schema':<30} {'Tables':<10}")
            print("-"*45)
            
            for i, schema in enumerate(schemas, 1):
                print(f"{i:<3} {schema['schema_name']:<30} {schema['table_count']:<10}")
            
            print(f"{len(schemas)+1:<3} Back to database browser")
            
            choice = self._get_user_choice(1, len(schemas)+1)
            
            if choice <= len(schemas):
                selected_schema = schemas[choice-1]['schema_name']
                self._table_browser(database_name, selected_schema)
            else:
                self._database_browser()
            
        except Exception as e:
            self.logger.error(f"Schema browsing error: {e}")
            print(f"ERROR: Error browsing schemas: {e}")
            input("\nPress Enter to continue...")
    
    def _table_browser(self, database_name: str, schema_name: str) -> None:
        """Browse tables in selected schema."""
        print(f"\nTable Browser - {database_name}.{schema_name}")
        print("="*70)
        
        try:
            # Get detailed table information
            query = """
            SELECT 
                t.table_name,
                COALESCE(s.n_live_tup, 0) as estimated_rows,
                pg_size_pretty(
                    pg_total_relation_size(quote_ident(%s)||'.'||quote_ident(t.table_name))
                ) as size,
                t.table_type
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s ON s.tablename = t.table_name 
                AND s.schemaname = t.table_schema
            WHERE t.table_schema = %s
            ORDER BY 
                CASE WHEN t.table_type = 'BASE TABLE' THEN 1 ELSE 2 END,
                pg_total_relation_size(quote_ident(%s)||'.'||quote_ident(t.table_name)) DESC NULLS LAST
            """
            
            tables = self.db_connection.execute_query(
                self.current_environment, query, (schema_name, schema_name, schema_name))
            
            if not tables:
                print("ERROR: No tables found in this schema")
                input("\nPress Enter to continue...")
                return
            
            print("Available Tables:")
            print(f"{'#':<3} {'Table Name':<35} {'Type':<8} {'Rows':<12} {'Size':<12}")
            print("-"*75)
            
            for i, table in enumerate(tables, 1):
                table_type = "Table" if table['table_type'] == 'BASE TABLE' else "View"
                rows = f"{table['estimated_rows']:,}" if table['estimated_rows'] else "N/A"
                print(f"{i:<3} {table['table_name']:<35} {table_type:<8} {rows:<12} {table['size']:<12}")
            
            print(f"{len(tables)+1:<3} Back to schema browser")
            print(f"{len(tables)+2:<3} Analyze entire schema")
            
            choice = self._get_user_choice(1, len(tables)+2)
            
            if choice <= len(tables):
                selected_table = tables[choice-1]['table_name']
                self._table_analyzer(database_name, schema_name, selected_table)
            elif choice == len(tables)+2:
                self._schema_analyzer(database_name, schema_name)
            else:
                self._schema_browser(database_name)
            
        except Exception as e:
            self.logger.error(f"Table browsing error: {e}")
            print(f"ERROR: Error browsing tables: {e}")
            input("\nPress Enter to continue...")
    
    def _table_analyzer(self, database_name: str, schema_name: str, table_name: str) -> None:
        """Comprehensive table analysis with operations menu."""
        full_table_name = f"{schema_name}.{table_name}"
        
        while True:
            print(f"\nTable Analyzer - {full_table_name}")
            print("="*60)
            print("1. Preview data (first 10 rows)")
            print("2. Show table structure")
            print("3. Column statistics")
            print("4. NULL value analysis")
            print("5. Find duplicate rows")
            print("6. Show indexes")
            print("7. Generate CREATE statement")
            print("8. Export table structure")
            print("9. Back to table browser")
            
            choice = self._get_user_choice(1, 9)
            
            try:
                if choice == 1:
                    self._preview_table_data(schema_name, table_name)
                elif choice == 2:
                    self._show_table_structure(schema_name, table_name)
                elif choice == 3:
                    self._show_column_statistics(schema_name, table_name)
                elif choice == 4:
                    self._analyze_null_values(schema_name, table_name)
                elif choice == 5:
                    self._find_duplicate_rows(schema_name, table_name)
                elif choice == 6:
                    self._show_table_indexes(schema_name, table_name)
                elif choice == 7:
                    self._generate_create_statement(schema_name, table_name)
                elif choice == 8:
                    self._export_table_structure(schema_name, table_name)
                elif choice == 9:
                    self._table_browser(database_name, schema_name)
                    return
                    
            except Exception as e:
                self.logger.error(f"Table analysis operation failed: {e}")
                print(f"ERROR: Operation failed: {e}")
                input("\nPress Enter to continue...")
    
    def _preview_table_data(self, schema_name: str, table_name: str) -> None:
        """Preview first 10 rows of table data."""
        print(f"\nData Preview - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            query = f"SELECT * FROM {schema_name}.{table_name} LIMIT 10"
            results = self.db_connection.execute_query(self.current_environment, query)
            
            if results:
                # Display in a formatted table
                columns = list(results[0].keys())
                
                # Calculate column widths
                col_widths = {}
                for col in columns:
                    col_widths[col] = max(len(col), 15)  # Minimum width 15
                
                # Header
                header = " | ".join(f"{col[:15]:<{min(15, col_widths[col])}}" for col in columns)
                print(header)
                print("-" * len(header))
                
                # Data rows
                for row in results:
                    row_data = " | ".join(
                        f"{str(row[col])[:15]:<{min(15, col_widths[col])}}" 
                        for col in columns
                    )
                    print(row_data)
                
                print(f"\nShowing {len(results)} rows")
            else:
                print("INFO: No data found in table")
                
        except Exception as e:
            print(f"ERROR: Error previewing data: {e}")
        
        input("\nPress Enter to continue...")
    
    def _show_table_structure(self, schema_name: str, table_name: str) -> None:
        """Show detailed table structure."""
        print(f"\nTable Structure - {schema_name}.{table_name}")
        print("-"*70)
        
        try:
            query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_connection.execute_query(
                self.current_environment, query, (schema_name, table_name))
            
            if columns:
                print(f"{'#':<3} {'Column':<25} {'Type':<20} {'Nullable':<8} {'Default':<15}")
                print("-"*75)
                
                for col in columns:
                    col_type = col['data_type']
                    if col['character_maximum_length']:
                        col_type += f"({col['character_maximum_length']})"
                    
                    nullable = "YES" if col['is_nullable'] == 'YES' else "NO"
                    default = str(col['column_default'])[:14] if col['column_default'] else ""
                    
                    print(f"{col['ordinal_position']:<3} {col['column_name']:<25} "
                          f"{col_type:<20} {nullable:<8} {default:<15}")
                
                print(f"\nTable has {len(columns)} columns")
            else:
                print("ERROR: No column information found")
                
        except Exception as e:
            print(f"ERROR: Error getting table structure: {e}")
        
        input("\nPress Enter to continue...")
    
    def _show_column_statistics(self, schema_name: str, table_name: str) -> None:
        """Show statistics for numeric columns."""
        print(f"\nColumn Statistics - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            # Get numeric columns
            numeric_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            AND data_type IN ('integer', 'bigint', 'numeric', 'real', 'double precision', 'smallint')
            ORDER BY ordinal_position
            """
            
            numeric_cols = self.db_connection.execute_query(
                self.current_environment, numeric_query, (schema_name, table_name))
            
            if numeric_cols:
                for col in numeric_cols:
                    col_name = col['column_name']
                    stats_query = f"""
                    SELECT 
                        MIN({col_name}) as min_val,
                        MAX({col_name}) as max_val,
                        AVG({col_name}) as avg_val,
                        COUNT(DISTINCT {col_name}) as distinct_count,
                        COUNT({col_name}) as non_null_count
                    FROM {schema_name}.{table_name}
                    WHERE {col_name} IS NOT NULL
                    """
                    
                    stats = self.db_connection.execute_query(self.current_environment, stats_query)
                    if stats and stats[0]['min_val'] is not None:
                        s = stats[0]
                        print(f"{col_name}:")
                        print(f"   Range: {s['min_val']} → {s['max_val']}")
                        if s['avg_val'] is not None:
                            print(f"   Average: {float(s['avg_val']):.2f}")
                        print(f"   Distinct values: {s['distinct_count']:,}")
                        print(f"   Non-null count: {s['non_null_count']:,}")
                        print()
                
                print("Column statistics completed")
            else:
                print("INFO: No numeric columns found")
            
        except Exception as e:
            print(f"ERROR: Error getting column statistics: {e}")
        
        input("\nPress Enter to continue...")
    
    def _analyze_null_values(self, schema_name: str, table_name: str) -> None:
        """Analyze NULL values in each column."""
        print(f"\nNULL Value Analysis - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            # Get all columns
            cols_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_connection.execute_query(
                self.current_environment, cols_query, (schema_name, table_name))
            
            if columns:
                print(f"{'Column':<30} {'NULL Count':<12} {'NULL %':<10}")
                print("-"*55)
                
                for col in columns:
                    col_name = col['column_name']
                    null_query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT({col_name}) as non_null_rows,
                        COUNT(*) - COUNT({col_name}) as null_rows
                    FROM {schema_name}.{table_name}
                    """
                    
                    result = self.db_connection.execute_query(self.current_environment, null_query)
                    if result:
                        r = result[0]
                        null_pct = (r['null_rows'] / r['total_rows'] * 100) if r['total_rows'] > 0 else 0
                        print(f"{col_name:<30} {r['null_rows']:<12,} {null_pct:<10.1f}%")
                
                print("\nNULL analysis completed")
            else:
                print("ERROR: No columns found")
            
        except Exception as e:
            print(f"ERROR: Error analyzing NULL values: {e}")
        
        input("\nPress Enter to continue...")
    
    def _find_duplicate_rows(self, schema_name: str, table_name: str) -> None:
        """Find duplicate rows in the table."""
        print(f"\nDuplicate Row Analysis - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            # Get row count first
            count_query = f"SELECT COUNT(*) as total_rows FROM {schema_name}.{table_name}"
            count_result = self.db_connection.execute_query(self.current_environment, count_query)
            total_rows = count_result[0]['total_rows'] if count_result else 0
            
            if total_rows > 100000:
                print(f"⚠️  Large table ({total_rows:,} rows) - analysis may take time")
                proceed = input("Continue? (y/N): ").strip().lower()
                if proceed != 'y':
                    return
            
            # Check for duplicates using a hash approach for large tables
            if total_rows > 10000:
                # For large tables, use a sampling approach
                dup_query = f"""
                SELECT COUNT(*) as duplicate_count
                FROM (
                    SELECT COUNT(*) as cnt
                    FROM {schema_name}.{table_name}
                    GROUP BY (SELECT string_agg(COALESCE(column_name::text, 'NULL'), '|') 
                             FROM (SELECT * FROM {schema_name}.{table_name} LIMIT 1) t)
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            else:
                # For smaller tables, do full analysis
                cols_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """
                
                columns = self.db_connection.execute_query(
                    self.current_environment, cols_query, (schema_name, table_name))
                
                if columns:
                    col_list = ", ".join([col['column_name'] for col in columns])
                    dup_query = f"""
                    SELECT COUNT(*) as duplicate_groups,
                           SUM(cnt - 1) as total_duplicates
                    FROM (
                        SELECT {col_list}, COUNT(*) as cnt
                        FROM {schema_name}.{table_name}
                        GROUP BY {col_list}
                        HAVING COUNT(*) > 1
                    ) dup_groups
                    """
            
            duplicates = self.db_connection.execute_query(self.current_environment, dup_query)
            
            if duplicates and duplicates[0]:
                if 'duplicate_groups' in duplicates[0]:
                    dup_groups = duplicates[0]['duplicate_groups'] or 0
                    total_dups = duplicates[0]['total_duplicates'] or 0
                    print(f"Duplicate Analysis Results:")
                    print(f"   Total rows: {total_rows:,}")
                    print(f"   Duplicate groups: {dup_groups:,}")
                    print(f"   Total duplicate rows: {total_dups:,}")
                    
                    if dup_groups > 0:
                        pct = (total_dups / total_rows * 100)
                        print(f"   Duplication rate: {pct:.2f}%")
                    else:
                        print("No duplicate rows found")
                else:
                    print("Duplicate analysis completed (large table sampling)")
            else:
                print("No duplicate rows found")
            
        except Exception as e:
            print(f"ERROR: Error finding duplicates: {e}")
        
        input("\nPress Enter to continue...")
    
    def _show_table_indexes(self, schema_name: str, table_name: str) -> None:
        """Show indexes for the table."""
        print(f"\nIndex Information - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            index_query = """
            SELECT 
                indexname,
                indexdef,
                pg_size_pretty(pg_relation_size(indexname::regclass)) as size
            FROM pg_indexes 
            WHERE schemaname = %s AND tablename = %s
            ORDER BY indexname
            """
            
            indexes = self.db_connection.execute_query(
                self.current_environment, index_query, (schema_name, table_name))
            
            if indexes:
                for i, idx in enumerate(indexes, 1):
                    print(f"{i}. Index: {idx['indexname']}")
                    print(f"   Size: {idx['size']}")
                    print(f"   Definition: {idx['indexdef']}")
                    print()
                
                print(f"✅ Found {len(indexes)} indexes")
            else:
                print("INFO: No indexes found for this table")
            
        except Exception as e:
            print(f"❌ Error getting index information: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _generate_create_statement(self, schema_name: str, table_name: str) -> None:
        """Generate CREATE TABLE statement."""
        print(f"\nCREATE TABLE Statement - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            structure_query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_connection.execute_query(
                self.current_environment, structure_query, (schema_name, table_name))
            
            if columns:
                print(f"CREATE TABLE {schema_name}.{table_name} (")
                col_defs = []
                
                for col in columns:
                    col_def = f"    {col['column_name']} {col['data_type']}"
                    
                    if col['character_maximum_length']:
                        col_def += f"({col['character_maximum_length']})"
                    
                    if col['is_nullable'] == 'NO':
                        col_def += " NOT NULL"
                    
                    if col['column_default']:
                        col_def += f" DEFAULT {col['column_default']}"
                    
                    col_defs.append(col_def)
                
                print(",\n".join(col_defs))
                print(");")
                print("\n✅ CREATE statement generated")
            else:
                print("❌ Could not retrieve table structure")
            
        except Exception as e:
            print(f"❌ Error generating CREATE statement: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _export_table_structure(self, schema_name: str, table_name: str) -> None:
        """Export table structure to CSV file."""
        print(f"\nExporting Table Structure - {schema_name}.{table_name}")
        print("-"*60)
        
        try:
            import csv
            from datetime import datetime
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{schema_name}_{table_name}_structure_{timestamp}.csv"
            
            structure_query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """
            
            columns = self.db_connection.execute_query(
                self.current_environment, structure_query, (schema_name, table_name))
            
            if columns:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ordinal_position', 'column_name', 'data_type', 
                                 'character_maximum_length', 'is_nullable', 'column_default']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for col in columns:
                        writer.writerow(col)
                
                file_path = Path(filename).absolute()
                print(f"✅ Table structure exported to: {file_path}")
                print(f"📊 Exported {len(columns)} columns")
            else:
                print("❌ No column data to export")
            
        except Exception as e:
            print(f"❌ Error exporting structure: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _schema_analyzer(self, database_name: str, schema_name: str) -> None:
        """Analyze entire schema."""
        print(f"\n🔍 Schema Analysis - {database_name}.{schema_name}")
        print("="*60)
        print("🚧 This feature analyzes all tables in the schema...")
        print("(Extended analysis capabilities can be added here)")
        
        try:
            # Get table count and total size
            query = """
            SELECT 
                COUNT(*) as table_count,
                pg_size_pretty(SUM(pg_total_relation_size(quote_ident(%s)||'.'||quote_ident(table_name)))) as total_size
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """
            
            stats = self.db_connection.execute_query(
                self.current_environment, query, (schema_name, schema_name))
            
            if stats:
                s = stats[0]
                print(f"📊 Schema Statistics:")
                print(f"   Tables: {s['table_count']}")
                print(f"   Total size: {s['total_size']}")
            
        except Exception as e:
            print(f"❌ Schema analysis error: {e}")
        
        input("\n📌Press Enter to continue...")
        self._table_browser(database_name, schema_name)
    
    def _run_complete_analysis(self) -> None:
        """Run complete archaeological analysis."""
        if not self._ensure_environment_selected():
            return
        
        print(f"\n🏛️  Complete Archaeological Analysis - {self.current_environment.title()}")
        print("="*60)
        print("⚠️  This comprehensive analysis may take several minutes...")
        print("It will analyze database structure, relationships, and patterns.")
        
        proceed = input("\nProceed with complete analysis? (y/N): ").strip().lower()
        if proceed != 'y':
            return
        
        try:
            from data_archaeologist.archaeologist import DataArchaeologist
            
            archaeologist = DataArchaeologist(self.config_file)
            print("\n🔍 Starting comprehensive discovery...")
            
            results = archaeologist.run_complete_discovery(
                self.current_environment,
                parallel_execution=True
            )
            
            self.last_analysis = {
                'type': 'complete',
                'environment': self.current_environment,
                'results': results,
                'timestamp': time.time()
            }
            
            print("✅ Complete archaeological analysis finished!")
            print("📊 Results stored for viewing and export")
            
        except ImportError:
            print("❌ Complete analysis module not available")
            print("Using basic database summary instead...")
            self._run_database_summary()
        except Exception as e:
            self.logger.error(f"Complete analysis failed: {e}")
            print(f"❌ Analysis failed: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _view_last_results(self) -> None:
        """View results from last analysis."""
        print("\n📈 Last Analysis Results")
        print("="*50)
        
        if not self.last_analysis:
            print("INFO: No previous analysis results available")
            print("Please run an analysis first")
            input("\n📌Press Enter to continue...")
            return
        
        try:
            analysis = self.last_analysis
            analysis_time = time.strftime(
                "%Y-%m-%d %H:%M:%S", 
                time.localtime(analysis['timestamp'])
            )
            
            print(f"Analysis Type: {analysis['type'].title()}")
            print(f"Environment: {analysis['environment'].title()}")
            print(f"Timestamp: {analysis_time}")
            print("-"*50)
            
            if analysis['type'] == 'summary' and 'results' in analysis:
                results = analysis['results']
                print(f"📊 Database Summary Results:")
                print(f"   Total tables analyzed: {len(results)}")
                
                if results:
                    print(f"\n🔝 Top 10 Tables by Size:")
                    print(f"{'Table':<40} {'Rows':<12} {'Size':<12}")
                    print("-"*65)
                    
                    for table in results[:10]:
                        rows = f"{table['rows']:,}" if table['rows'] else "N/A"
                        print(f"{table['full_table_name']:<40} {rows:<12} {table['size_human']:<12}")
            
            elif analysis['type'] == 'complete':
                print("🏛️  Complete archaeological analysis results available")
                print("   (Detailed results can be exported)")
            
        except Exception as e:
            self.logger.error(f"Error displaying results: {e}")
            print(f"❌ Error displaying results: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _export_results(self) -> None:
        """Export analysis results to file."""
        print("\nExport Analysis Results")
        print("="*50)
        
        if not self.last_analysis:
            print("INFO: No analysis results available to export")
            input("\n📌Press Enter to continue...")
            return
        
        print("Export Formats:")
        print("1. JSON format")
        print("2. CSV format")
        print("3. Text report")
        print("4. Cancel")
        
        choice = self._get_user_choice(1, 4)
        
        if choice == 4:
            return
        
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            env = self.last_analysis['environment']
            analysis_type = self.last_analysis['type']
            
            if choice == 1:  # JSON
                import json
                filename = f"analysis_{analysis_type}_{env}_{timestamp}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(self.last_analysis, f, indent=2, default=str)
                print(f"✅ JSON export saved: {filename}")
                
            elif choice == 2:  # CSV
                import csv
                filename = f"analysis_{analysis_type}_{env}_{timestamp}.csv"
                
                if analysis_type == 'summary' and 'results' in self.last_analysis:
                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=['schema', 'table', 'full_table_name', 'rows', 'size_bytes', 'size_human'])
                        writer.writeheader()
                        writer.writerows(self.last_analysis['results'])
                    print(f"✅ CSV export saved: {filename}")
                else:
                    print("❌ CSV export not available for this analysis type")
                    
            elif choice == 3:  # Text report
                filename = f"analysis_{analysis_type}_{env}_{timestamp}.txt"
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(f"Data Archaeologist Analysis Report\n")
                    f.write(f"{'='*50}\n")
                    f.write(f"Environment: {env}\n")
                    f.write(f"Analysis Type: {analysis_type}\n")
                    f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    if analysis_type == 'summary' and 'results' in self.last_analysis:
                        results = self.last_analysis['results']
                        f.write(f"Database Summary Results\n")
                        f.write(f"Total tables: {len(results)}\n\n")
                        
                        for table in results:
                            f.write(f"Table: {table['full_table_name']}\n")
                            f.write(f"  Rows: {table['rows']:,}\n")
                            f.write(f"  Size: {table['size_human']}\n\n")
                
                print(f"✅ Text report saved: {filename}")
            
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            print(f"❌ Export failed: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _configuration_management(self) -> None:
        """Configuration management menu."""
        print("\n⚙️  Configuration Management")
        print("="*50)
        
        print("1. 🔍 View current configuration")
        print("2. ✅ Validate configuration")
        print("3. 🔌 Test all connections")
        print("4. 📊 Show environment details")
        print("5. 🔙 Back to main menu")
        
        choice = self._get_user_choice(1, 5)
        
        if choice == 1:
            self._view_configuration()
        elif choice == 2:
            self._validate_configuration()
        elif choice == 3:
            self._test_all_connections()
        elif choice == 4:
            self._show_environment_details()
        # choice == 5 returns to main menu
    
    def _view_configuration(self) -> None:
        """View current configuration."""
        print("\n🔍 Current Configuration")
        print("-"*50)
        
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            print(f"Configuration file: {self.config_file}")
            
            if 'environments' in config:
                envs = config['environments']
                print(f"Environments configured: {len(envs)}")
                
                for env_name, env_config in envs.items():
                    print(f"\n📍 {env_name.title()}:")
                    print(f"   Host: {env_config.get('host', 'Not specified')}")
                    print(f"   Port: {env_config.get('port', 'Not specified')}")
                    print(f"   Database: {env_config.get('database', 'Not specified')}")
                    print(f"   Username: {env_config.get('username', 'Not specified')}")
                    print(f"   Description: {env_config.get('description', 'No description')}")
            
            if 'analysis_settings' in config:
                settings = config['analysis_settings']
                print(f"\n📊 Analysis Settings:")
                for key, value in settings.items():
                    print(f"   {key}: {value}")
            
        except Exception as e:
            print(f"❌ Error reading configuration: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _show_environment_details(self) -> None:
        """Show detailed environment information."""
        print("\n📊 Environment Details")
        print("-"*50)
        
        try:
            if not self.db_connection:
                self.db_connection = DatabaseConnection(self.config_file)
            
            environments = self.db_connection.get_available_environments()
            
            for env in environments:
                print(f"\n🌐 {env.title()} Environment:")
                try:
                    if test_database_connection(self.db_connection, env):
                        print("   Status: ✅ Connected")
                        
                        # Get basic database info
                        version_query = "SELECT version() as version"
                        version_result = self.db_connection.execute_query(env, version_query)
                        if version_result:
                            version = version_result[0]['version']
                            print(f"   Version: {version.split(',')[0]}")
                        
                        # Get database size
                        size_query = """
                        SELECT pg_size_pretty(pg_database_size(current_database())) as size
                        """
                        size_result = self.db_connection.execute_query(env, size_query)
                        if size_result:
                            print(f"   Size: {size_result[0]['size']}")
                        
                    else:
                        print("   Status: ❌ Connection failed")
                        
                except Exception as e:
                    print(f"   Status: ❌ Error - {str(e)[:50]}...")
            
        except Exception as e:
            print(f"❌ Error getting environment details: {e}")
        
        input("\n📌Press Enter to continue...")
    
    def _ensure_environment_selected(self) -> bool:
        """Ensure an environment is selected."""
        if not self.current_environment:
            print("❌ Please select an environment first")
            print("Use option 2 from the main menu to select an environment")
            input("\n📌Press Enter to continue...")
            return False
        return True
    
    def _show_table_structure_safe(self, environment, schema, table):
        """Show table structure using safe SQL composition."""
        try:
            # Get column information using safe SQL
            query = sql.SQL("""
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """)
            
            result = self.db_connection.execute_query(environment, query, (schema, table))
            
            if result:
                print(f"\n📋 Structure of {schema}.{table}:")
                print("-" * 80)
                for row in result:
                    nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                    max_len = f"({row['character_maximum_length']})" if row['character_maximum_length'] else ""
                    default = f"DEFAULT {row['column_default']}" if row['column_default'] else ""
                    print(f"{row['ordinal_position']:2d}. {row['column_name']:25s} {row['data_type']}{max_len:15s} {nullable:8s} {default}")
                print("-" * 80)
                print(f"Total columns: {len(result)}")
            else:
                print(f"No structure information found for {schema}.{table}")
                
        except Exception as e:
            print(f"❌ Error showing table structure: {e}")

    def _show_column_statistics_safe(self, environment, schema, table):
        """Show column statistics using safe SQL composition."""
        try:
            # Get column names first
            columns_query = sql.SQL("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """)
            
            columns = self.db_connection.execute_query(environment, columns_query, (schema, table))
            
            if not columns:
                print(f"No columns found for {schema}.{table}")
                return
            
            print(f"\n📊 Column Statistics for {schema}.{table}:")
            print("-" * 100)
            
            # For each column, get basic statistics
            for col in columns:
                col_name = col['column_name']
                data_type = col['data_type']
                
                # Use safe SQL composition for column statistics
                stats_query = sql.SQL("""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT({column}) as non_null_count,
                        COUNT(DISTINCT {column}) as distinct_count,
                        (COUNT(*) - COUNT({column})) as null_count
                    FROM {schema}.{table}
                """).format(
                    column=sql.Identifier(col_name),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table)
                )
                
                try:
                    stats = self.db_connection.execute_query(environment, stats_query)
                    if stats:
                        s = stats[0]
                        null_pct = (s['null_count'] / s['total_rows'] * 100) if s['total_rows'] > 0 else 0
                        distinct_pct = (s['distinct_count'] / s['non_null_count'] * 100) if s['non_null_count'] > 0 else 0
                        
                        print(f"{col_name:25s} {data_type:15s} | "
                              f"Nulls: {s['null_count']:>6,} ({null_pct:5.1f}%) | "
                              f"Distinct: {s['distinct_count']:>6,} ({distinct_pct:5.1f}%)")
                              
                except Exception as e:
                    print(f"{col_name:25s} {data_type:15s} | Error: {str(e)[:40]}")
            
            print("-" * 100)
            
        except Exception as e:
            print(f"❌ Error showing column statistics: {e}")

    def _analyze_null_values_safe(self, environment, schema, table):
        """Analyze null values using safe SQL composition."""
        try:
            # Get columns first
            columns_query = sql.SQL("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """)
            
            columns = self.db_connection.execute_query(environment, columns_query, (schema, table))
            
            if not columns:
                print(f"No columns found for {schema}.{table}")
                return
            
            print(f"\n🔍 Null Value Analysis for {schema}.{table}:")
            print("-" * 80)
            
            null_analysis = []
            
            for col in columns:
                col_name = col['column_name']
                is_nullable = col['is_nullable']
                
                # Safe SQL for null analysis
                null_query = sql.SQL("""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT({column}) as non_null_rows,
                        (COUNT(*) - COUNT({column})) as null_rows,
                        ROUND(((COUNT(*) - COUNT({column})) * 100.0 / COUNT(*)), 2) as null_percentage
                    FROM {schema}.{table}
                """).format(
                    column=sql.Identifier(col_name),
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table)
                )
                
                try:
                    result = self.db_connection.execute_query(environment, null_query)
                    if result:
                        r = result[0]
                        null_analysis.append({
                            'column': col_name,
                            'nullable': is_nullable,
                            'null_count': r['null_rows'],
                            'null_percentage': r['null_percentage'],
                            'total_rows': r['total_rows']
                        })
                        
                        # Highlight high null percentages
                        status = "🔴" if r['null_percentage'] > 50 else "🟡" if r['null_percentage'] > 10 else "🟢"
                        print(f"{status} {col_name:25s} | {r['null_rows']:>8,} nulls ({r['null_percentage']:>5.1f}%) | Nullable: {is_nullable}")
                        
                except Exception as e:
                    print(f"❌ {col_name:25s} | Error: {str(e)[:40]}")
            
            print("-" * 80)
            
            # Summary
            if null_analysis:
                high_null_cols = [a for a in null_analysis if a['null_percentage'] > 50]
                medium_null_cols = [a for a in null_analysis if 10 < a['null_percentage'] <= 50]
                
                print(f"\n📊 Summary:")
                print(f"• Columns with >50% nulls: {len(high_null_cols)}")
                print(f"• Columns with 10-50% nulls: {len(medium_null_cols)}")
                print(f"• Total analyzed columns: {len(null_analysis)}")
                
                if high_null_cols:
                    print(f"\n🔴 High null columns: {', '.join([a['column'] for a in high_null_cols])}")
            
        except Exception as e:
            print(f"❌ Error analyzing null values: {e}")

    def _find_duplicate_rows_safe(self, environment, schema, table, limit=10):
        """Find duplicate rows using safe SQL composition."""
        try:
            # Get total row count first
            count_query = sql.SQL("SELECT COUNT(*) as total_rows FROM {schema}.{table}").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)
            )
            
            count_result = self.db_connection.execute_query(environment, count_query)
            total_rows = count_result[0]['total_rows'] if count_result else 0
            
            print(f"\n🔍 Duplicate Row Analysis for {schema}.{table} ({total_rows:,} total rows):")
            print("-" * 80)
            
            # Get all columns for grouping
            columns_query = sql.SQL("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """)
            
            columns = self.db_connection.execute_query(environment, columns_query, (schema, table))
            
            if not columns:
                print("No columns found")
                return
            
            # Build column list for GROUP BY
            column_names = [col['column_name'] for col in columns]
            column_identifiers = [sql.Identifier(name) for name in column_names]
            
            # Find duplicate rows using safe SQL
            duplicate_query = sql.SQL("""
                SELECT {columns}, COUNT(*) as duplicate_count
                FROM {schema}.{table}
                GROUP BY {columns}
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
                LIMIT %s
            """).format(
                columns=sql.SQL(', ').join(column_identifiers),
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)
            )
            
            duplicates = self.db_connection.execute_query(environment, duplicate_query, (limit,))
            
            if duplicates:
                print(f"Found {len(duplicates)} sets of duplicate rows (showing top {limit}):")
                print("-" * 80)
                
                total_duplicate_rows = 0
                for i, dup in enumerate(duplicates, 1):
                    count = dup['duplicate_count']
                    total_duplicate_rows += count - 1  # Subtract 1 to count only extras
                    
                    print(f"\n{i}. Duplicate set with {count} occurrences:")
                    # Show first few column values
                    for j, col_name in enumerate(column_names[:5]):  # Show only first 5 columns
                        value = dup.get(col_name, 'N/A')
                        if isinstance(value, str) and len(value) > 30:
                            value = value[:27] + "..."
                        print(f"   {col_name}: {value}")
                    
                    if len(column_names) > 5:
                        print(f"   ... and {len(column_names) - 5} more columns")
                
                print("-" * 80)
                print(f"📊 Summary:")
                print(f"• Total rows: {total_rows:,}")
                print(f"• Duplicate row sets: {len(duplicates)}")
                print(f"• Extra duplicate rows: {total_duplicate_rows:,}")
                print(f"• Duplicate percentage: {(total_duplicate_rows / total_rows * 100):0.2f}%")
                
            else:
                print("🟢 No duplicate rows found!")
            
        except Exception as e:
            print(f"❌ Error finding duplicate rows: {e}")

    def _preview_table_data_safe(self, environment, schema, table, limit=10):
        """Preview table data using safe SQL composition."""
        try:
            # Use safe SQL composition for data preview
            preview_query = sql.SQL("SELECT * FROM {schema}.{table} LIMIT %s").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)
            )
            
            result = self.db_connection.execute_query(environment, preview_query, (limit,))
            
            if result:
                print(f"\n👀 Preview of {schema}.{table} (first {limit} rows):")
                print("-" * 100)
                
                # Get column names
                columns = list(result[0].keys())
                
                # Print header
                header = " | ".join([f"{col[:15]:15s}" for col in columns])
                print(header)
                print("-" * len(header))
                
                # Print data rows
                for row in result:
                    row_data = []
                    for col in columns:
                        value = row[col]
                        if value is None:
                            value_str = "NULL"
                        elif isinstance(value, str):
                            value_str = value[:15]
                        else:
                            value_str = str(value)[:15]
                        row_data.append(f"{value_str:15s}")
                    
                    print(" | ".join(row_data))
                
                print("-" * 100)
                print(f"Showing {len(result)} of ? rows")
            else:
                print(f"No data found in {schema}.{table}")
                
        except Exception as e:
            print(f"❌ Error previewing table data: {e}")
    
    def _cleanup(self) -> None:
        """Cleanup resources."""
        if self.db_connection:
            try:
                # Close database connections if needed
                pass
            except Exception as e:
                self.logger.error(f"Cleanup error: {e}")


def main():
    """Main entry point."""
    try:
        explorer = DatabaseExplorer()
        explorer.run()
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

