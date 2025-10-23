#!/usr/bin/env python3
"""
Database Discovery Toolkit - Principal Data Architect Edition

A comprehensive, interactive command-line toolkit for deep database discovery 
and architectural auditing. This toolkit implements a three-layer discovery 
process for understanding and documenting database environments.

Author: Principal Data Architect
Version: 1.0.0
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import time
from datetime import datetime


@dataclass
class TableInfo:
    """Data class for table information."""
    schema: str
    name: str
    row_count: int
    size_mb: float
    columns: List[str]


@dataclass
class ColumnInfo:
    """Data class for column information."""
    name: str
    data_type: str
    null_percentage: float
    unique_percentage: float
    sample_values: List[Any]


@dataclass
class ForeignKeyCandidate:
    """Data class for foreign key candidates."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    confidence: str
    match_percentage: float


class DatabaseToolkit:
    """
    Principal Data Architect's Database Discovery Toolkit
    
    A comprehensive tool for three-layer database analysis:
    - Layer 1: Physical Survey (What is there?)
    - Layer 2: Logical Blueprint (How does it connect?)
    - Layer 3: Architectural Audit (Is it well-designed?)
    """
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initialize the Database Toolkit.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.current_environment = None
        self.current_engine = None
        self.table_cache = {}
        
    def _load_config(self) -> Dict:
        """
        Load database configuration from JSON file.
        
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is invalid JSON
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            # Support both 'connections' and 'environments' config formats
            if 'connections' not in config and 'environments' not in config:
                raise ValueError("Configuration must contain 'connections' or 'environments' section")
                
            return config
            
        except FileNotFoundError:
            print(f"ERROR: Configuration file '{self.config_path}' not found!")
            print("Please create a config.json file with database connections.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in configuration file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: Failed to load configuration: {e}")
            sys.exit(1)
    
    def _create_engine(self, environment: str) -> None:
        """
        Create SQLAlchemy engine for the specified environment.
        
        Args:
            environment: Environment name (staging, production, backup)
            
        Raises:
            ValueError: If environment doesn't exist in config
            SQLAlchemyError: If connection fails
        """
        # Support both config formats
        connections = self.config.get('connections', self.config.get('environments', {}))
        
        if environment not in connections:
            available = list(connections.keys())
            raise ValueError(f"Environment '{environment}' not found. Available: {available}")
        
        try:
            env_config = connections[environment]
            
            # Handle different config formats
            if 'url' in env_config:
                # Simple URL format
                url = env_config['url']
            else:
                # Detailed format - construct URL
                url = f"postgresql://{env_config['username']}:{env_config['password']}@{env_config['host']}:{env_config['port']}/{env_config['database']}"
            
            self.current_engine = create_engine(url, pool_pre_ping=True)
            
            # Test connection
            with self.current_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.current_environment = environment
            print(f"✓ Successfully connected to {environment} environment")
            
        except SQLAlchemyError as e:
            print(f"ERROR: Failed to connect to {environment}: {e}")
            raise
        except Exception as e:
            print(f"ERROR: Unexpected error connecting to {environment}: {e}")
            raise
    
    def _execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame with query results
            
        Raises:
            SQLAlchemyError: If query execution fails
        """
        try:
            with self.current_engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except SQLAlchemyError as e:
            print(f"ERROR: Query execution failed: {e}")
            raise
        except Exception as e:
            print(f"ERROR: Unexpected error executing query: {e}")
            raise
    
    def _get_database_list(self) -> List[str]:
        """
        Get list of all databases available to the user.
        
        Returns:
            List of database names
        """
        query = """
        SELECT datname 
        FROM pg_database 
        WHERE datistemplate = false 
        AND datallowconn = true
        ORDER BY datname
        """
        
        try:
            df = self._execute_query(query)
            return df['datname'].tolist()
        except Exception as e:
            print(f"WARNING: Could not get database list: {e}")
            return []

    def _get_schema_list(self) -> List[str]:
        """
        Get list of all schemas in the current database.
        
        Returns:
            List of schema names
        """
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
        """
        
        df = self._execute_query(query)
        return df['schema_name'].tolist()

    def _get_table_list(self, schema: str = None) -> List[Tuple[str, str]]:
        """
        Get list of all tables in the database or specific schema.
        
        Args:
            schema: Optional schema name to filter by
            
        Returns:
            List of (schema, table_name) tuples
        """
        if schema:
            query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            AND table_schema = :schema
            ORDER BY table_name
            """
            df = self._execute_query(query, {'schema': schema})
        else:
            query = """
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'sys')
            ORDER BY table_schema, table_name
            """
            df = self._execute_query(query)
        
        return [(row['table_schema'], row['table_name']) for _, row in df.iterrows()]

    def _get_table_list_for_schema(self, schema: str) -> List[str]:
        """
        Get list of table names for a specific schema.
        
        Args:
            schema: Schema name
            
        Returns:
            List of table names
        """
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_type = 'BASE TABLE'
        AND table_schema = :schema
        ORDER BY table_name
        """
        
        df = self._execute_query(query, {'schema': schema})
        return df['table_name'].tolist()
    
    def _select_database(self) -> Optional[str]:
        """
        Interactive database selection from numbered list.
        
        Returns:
            Selected database name or None if cancelled
        """
        databases = self._get_database_list()
        
        if not databases:
            print("No databases found.")
            return None
        
        print(f"\nAvailable Databases:")
        print("-" * 40)
        for i, db in enumerate(databases, 1):
            print(f"{i:2}. {db}")
        print(f"{len(databases)+1:2}. Cancel")
        
        while True:
            try:
                choice = input(f"\nSelect database (1-{len(databases)+1}): ").strip()
                if not choice:
                    continue
                
                choice_num = int(choice)
                if choice_num == len(databases) + 1:
                    return None
                elif 1 <= choice_num <= len(databases):
                    selected_db = databases[choice_num - 1]
                    print(f"Selected database: {selected_db}")
                    return selected_db
                else:
                    print(f"Please enter a number between 1 and {len(databases)+1}")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nCancelled by user.")
                return None

    def _select_schema(self) -> Optional[str]:
        """
        Interactive schema selection from numbered list.
        
        Returns:
            Selected schema name or None if cancelled
        """
        schemas = self._get_schema_list()
        
        if not schemas:
            print("No schemas found.")
            return None
        
        print(f"\nAvailable Schemas:")
        print("-" * 40)
        for i, schema in enumerate(schemas, 1):
            print(f"{i:2}. {schema}")
        print(f"{len(schemas)+1:2}. Cancel")
        
        while True:
            try:
                choice = input(f"\nSelect schema (1-{len(schemas)+1}): ").strip()
                if not choice:
                    continue
                
                choice_num = int(choice)
                if choice_num == len(schemas) + 1:
                    return None
                elif 1 <= choice_num <= len(schemas):
                    selected_schema = schemas[choice_num - 1]
                    print(f"Selected schema: {selected_schema}")
                    return selected_schema
                else:
                    print(f"Please enter a number between 1 and {len(schemas)+1}")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nCancelled by user.")
                return None

    def _select_table(self, schema: str = None) -> Optional[Tuple[str, str]]:
        """
        Interactive table selection from numbered list.
        
        Args:
            schema: Optional schema to filter tables
            
        Returns:
            Tuple of (schema, table_name) or None if cancelled
        """
        if schema:
            tables = [(schema, table) for table in self._get_table_list_for_schema(schema)]
        else:
            tables = self._get_table_list()
        
        if not tables:
            print("No tables found.")
            return None
        
        print(f"\nAvailable Tables{' in ' + schema if schema else ''}:")
        print("-" * 60)
        print(f"{'#':<3} {'Schema':<20} {'Table':<30}")
        print("-" * 60)
        
        for i, (table_schema, table_name) in enumerate(tables, 1):
            print(f"{i:<3} {table_schema:<20} {table_name:<30}")
        print(f"{len(tables)+1:<3} Cancel")
        
        while True:
            try:
                choice = input(f"\nSelect table (1-{len(tables)+1}): ").strip()
                if not choice:
                    continue
                
                choice_num = int(choice)
                if choice_num == len(tables) + 1:
                    return None
                elif 1 <= choice_num <= len(tables):
                    selected_table = tables[choice_num - 1]
                    print(f"Selected table: {selected_table[0]}.{selected_table[1]}")
                    return selected_table
                else:
                    print(f"Please enter a number between 1 and {len(tables)+1}")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nCancelled by user.")
                return None

    def _get_user_choice(self, min_val: int, max_val: int, prompt: str = "Select option") -> Optional[int]:
        """
        Get a validated user choice within a range.
        
        Args:
            min_val: Minimum valid value
            max_val: Maximum valid value
            prompt: Custom prompt message
            
        Returns:
            Selected number or None if cancelled
        """
        while True:
            try:
                choice = input(f"\n{prompt} ({min_val}-{max_val}): ").strip()
                if not choice:
                    continue
                
                choice_num = int(choice)
                if min_val <= choice_num <= max_val:
                    return choice_num
                else:
                    print(f"Please enter a number between {min_val} and {max_val}")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nCancelled by user.")
                return None
        """
        Get row count and size for a specific table.
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            Tuple of (row_count, size_mb)
        """
        try:
            # Row count query
            count_query = f"SELECT COUNT(*) as row_count FROM {schema}.{table}"
            count_df = self._execute_query(count_query)
            row_count = count_df.iloc[0]['row_count']
            
            # Size query (PostgreSQL specific)
            size_query = """
            SELECT pg_total_relation_size(schemaname||'.'||tablename) / 1024.0 / 1024.0 as size_mb
            FROM pg_tables 
            WHERE schemaname = :schema AND tablename = :table
            """
            size_df = self._execute_query(size_query, {'schema': schema, 'table': table})
            size_mb = size_df.iloc[0]['size_mb'] if not size_df.empty else 0.0
            
            return row_count, size_mb
            
        except Exception as e:
            print(f"WARNING: Could not get stats for {schema}.{table}: {e}")
            return 0, 0.0
    
    def _get_table_size_and_count(self, schema: str, table: str) -> Tuple[int, float]:
        """
        Get row count and size for a specific table.
        
        Args:
            schema: Schema name
            table: Table name
            
        Returns:
            Tuple of (row_count, size_mb)
        """
        try:
            # Row count query
            count_query = f"SELECT COUNT(*) as row_count FROM {schema}.{table}"
            count_df = self._execute_query(count_query)
            row_count = count_df.iloc[0]['row_count']
            
            # Size query (PostgreSQL specific)
            size_query = """
            SELECT pg_total_relation_size(schemaname||'.'||tablename) / 1024.0 / 1024.0 as size_mb
            FROM pg_tables 
            WHERE schemaname = :schema AND tablename = :table
            """
            size_df = self._execute_query(size_query, {'schema': schema, 'table': table})
            size_mb = size_df.iloc[0]['size_mb'] if not size_df.empty else 0.0
            
            return row_count, size_mb
            
        except Exception as e:
            print(f"WARNING: Could not get stats for {schema}.{table}: {e}")
            return 0, 0.0

    def _get_column_profile(self, schema: str, table: str, column: str) -> ColumnInfo:
        """
        Get detailed profile information for a specific column.
        
        Args:
            schema: Schema name
            table: Table name
            column: Column name
            
        Returns:
            ColumnInfo object with column statistics
        """
        try:
            # Basic column info
            info_query = """
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_schema = :schema 
            AND table_name = :table 
            AND column_name = :column
            """
            info_df = self._execute_query(info_query, {
                'schema': schema, 'table': table, 'column': column
            })
            data_type = info_df.iloc[0]['data_type'] if not info_df.empty else 'unknown'
            
            # Statistics query
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count,
                COUNT(DISTINCT {column}) as unique_count
            FROM {schema}.{table}
            """
            stats_df = self._execute_query(stats_query)
            
            total = stats_df.iloc[0]['total_count']
            non_null = stats_df.iloc[0]['non_null_count']
            unique = stats_df.iloc[0]['unique_count']
            
            null_percentage = ((total - non_null) / total * 100) if total > 0 else 0
            unique_percentage = (unique / total * 100) if total > 0 else 0
            
            # Sample values
            sample_query = f"""
            SELECT DISTINCT {column} 
            FROM {schema}.{table} 
            WHERE {column} IS NOT NULL 
            LIMIT 5
            """
            sample_df = self._execute_query(sample_query)
            sample_values = sample_df[column].tolist() if not sample_df.empty else []
            
            return ColumnInfo(
                name=column,
                data_type=data_type,
                null_percentage=null_percentage,
                unique_percentage=unique_percentage,
                sample_values=sample_values
            )
            
        except Exception as e:
            print(f"WARNING: Could not profile column {schema}.{table}.{column}: {e}")
            return ColumnInfo(column, 'unknown', 0.0, 0.0, [])
    
    # ============================================================================
    # LAYER 1: PHYSICAL SURVEY TOOLS
    # ============================================================================
    
    def database_wide_summary(self) -> None:
        """
        Option 1: Generate a comprehensive database-wide summary.
        
        Uses parallelism to fetch row counts and storage sizes for all tables,
        then presents a clean, formatted summary sorted by size.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        print("\n" + "="*80)
        print("LAYER 1: DATABASE-WIDE PHYSICAL SURVEY")
        print("="*80)
        print(f"Environment: {self.current_environment}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            tables = self._get_table_list()
            if not tables:
                print("No tables found in the database.")
                return
            
            print(f"\nDiscovered {len(tables)} tables. Analyzing in parallel...")
            
            # Use ThreadPoolExecutor for parallel processing
            table_stats = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_table = {
                    executor.submit(self._get_table_size_and_count, schema, table): (schema, table)
                    for schema, table in tables
                }
                
                for future in as_completed(future_to_table):
                    schema, table = future_to_table[future]
                    try:
                        row_count, size_mb = future.result()
                        table_stats.append({
                            'Schema': schema,
                            'Table': table,
                            'Row Count': f"{row_count:,}",
                            'Size (MB)': f"{size_mb:.2f}",
                            'Size_Numeric': size_mb
                        })
                    except Exception as e:
                        print(f"ERROR processing {schema}.{table}: {e}")
            
            # Sort by size descending
            table_stats.sort(key=lambda x: x['Size_Numeric'], reverse=True)
            
            # Remove the numeric sorting column
            for stat in table_stats:
                del stat['Size_Numeric']
            
            # Display results
            df = pd.DataFrame(table_stats)
            print("\nDATABASE SUMMARY (Sorted by Size):")
            print("-" * 80)
            print(df.to_string(index=False))
            
            # Summary statistics
            total_size = sum(float(stat['Size (MB)']) for stat in table_stats)
            total_rows = sum(int(stat['Row Count'].replace(',', '')) for stat in table_stats)
            
            print(f"\nSUMMARY STATISTICS:")
            print(f"Total Tables: {len(table_stats)}")
            print(f"Total Rows: {total_rows:,}")
            print(f"Total Size: {total_size:.2f} MB")
            
        except Exception as e:
            print(f"ERROR: Failed to generate database summary: {e}")
    
    def schema_browser_analysis(self) -> None:
        """
        Option 3: Interactive schema browser and analysis.
        
        Allows users to select a schema and see detailed analysis
        of all tables within that schema.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        # Interactive schema selection
        print("\n" + "="*80)
        print("SCHEMA BROWSER & ANALYSIS")
        print("="*80)
        
        selected_schema = self._select_schema()
        if not selected_schema:
            print("Schema selection cancelled.")
            return
        
        try:
            # Get tables in the selected schema
            tables = self._get_table_list_for_schema(selected_schema)
            if not tables:
                print(f"No tables found in schema '{selected_schema}'.")
                return
            
            print(f"\nAnalyzing {len(tables)} tables in schema '{selected_schema}'...")
            
            # Analyze all tables in the schema in parallel
            table_stats = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_table = {
                    executor.submit(self._get_table_size_and_count, selected_schema, table): table
                    for table in tables
                }
                
                for future in as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        row_count, size_mb = future.result()
                        table_stats.append({
                            'Table': table,
                            'Row Count': f"{row_count:,}",
                            'Size (MB)': f"{size_mb:.2f}",
                            'Size_Numeric': size_mb
                        })
                    except Exception as e:
                        print(f"ERROR processing {selected_schema}.{table}: {e}")
            
            # Sort by size descending
            table_stats.sort(key=lambda x: x['Size_Numeric'], reverse=True)
            
            # Remove the numeric sorting column
            for stat in table_stats:
                del stat['Size_Numeric']
            
            # Display results
            print(f"\nSCHEMA ANALYSIS: {selected_schema}")
            print("-" * 60)
            df = pd.DataFrame(table_stats)
            print(df.to_string(index=False))
            
            # Summary statistics
            total_size = sum(float(stat['Size (MB)']) for stat in table_stats)
            total_rows = sum(int(stat['Row Count'].replace(',', '')) for stat in table_stats)
            
            print(f"\nSCHEMA SUMMARY:")
            print(f"Schema: {selected_schema}")
            print(f"Total Tables: {len(table_stats)}")
            print(f"Total Rows: {total_rows:,}")
            print(f"Total Size: {total_size:.2f} MB")
            
            # Offer additional analysis
            print(f"\nAdditional Analysis Options:")
            print("1. Analyze specific table in detail")
            print("2. Find relationships within this schema")
            print("3. Check for duplicates across tables")
            print("4. Return to main menu")
            
            choice = self._get_user_choice(1, 4, "Select analysis option")
            if choice == 1:
                # Select and analyze specific table
                selected_table = self._select_table(selected_schema)
                if selected_table:
                    schema, table = selected_table
                    self._analyze_single_table(schema, table)
            elif choice == 2:
                self._analyze_schema_relationships(selected_schema)
            elif choice == 3:
                self._check_schema_duplicates(selected_schema)
                
        except Exception as e:
            print(f"ERROR: Failed to analyze schema {selected_schema}: {e}")
    
    def _analyze_single_table(self, schema: str, table: str) -> None:
        """Quick analysis of a single table."""
        print(f"\nQuick Analysis: {schema}.{table}")
        print("-" * 40)
        
        try:
            # Basic stats
            row_count, size_mb = self._get_table_size_and_count(schema, table)
            print(f"Rows: {row_count:,}")
            print(f"Size: {size_mb:.2f} MB")
            
            # Column count
            columns_query = """
            SELECT COUNT(*) as column_count
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            """
            col_df = self._execute_query(columns_query, {'schema': schema, 'table': table})
            col_count = col_df.iloc[0]['column_count']
            print(f"Columns: {col_count}")
            
        except Exception as e:
            print(f"ERROR analyzing {schema}.{table}: {e}")
    
    def _analyze_schema_relationships(self, schema: str) -> None:
        """Analyze relationships within a schema."""
        print(f"\nAnalyzing relationships within schema: {schema}")
        print("This would analyze FK relationships between tables in the same schema...")
        # Implementation would go here
    
    def _check_schema_duplicates(self, schema: str) -> None:
        """Check for duplicates across tables in a schema."""
        print(f"\nChecking for duplicates across tables in schema: {schema}")
        print("This would look for potential duplicate data across tables...")
        # Implementation would go here

    def detailed_table_profiler(self) -> None:
        """
        Option 2: Generate detailed profile for a specific table.
        
        Provides column-by-column breakdown including data types,
        null percentages, and uniqueness percentages.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        # Interactive table selection
        print("\n" + "="*80)
        print("DETAILED TABLE PROFILER - SELECT TABLE")
        print("="*80)
        
        selected_table = self._select_table()
        if not selected_table:
            print("Table selection cancelled.")
            return
        
        schema, table = selected_table
        
        print(f"\n" + "="*80)
        print(f"LAYER 1: DETAILED TABLE PROFILE - {schema}.{table}")
        print("="*80)
        
        try:
            # Get column information
            columns_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            columns_df = self._execute_query(columns_query, {'schema': schema, 'table': table})
            
            if columns_df.empty:
                print(f"Table {schema}.{table} not found or has no columns.")
                return
            
            columns = columns_df['column_name'].tolist()
            
            print(f"Profiling {len(columns)} columns in parallel...")
            
            # Profile columns in parallel
            column_profiles = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_column = {
                    executor.submit(self._get_column_profile, schema, table, col): col
                    for col in columns
                }
                
                for future in as_completed(future_to_column):
                    column = future_to_column[future]
                    try:
                        profile = future.result()
                        column_profiles.append({
                            'Column': profile.name,
                            'Data Type': profile.data_type,
                            'Null %': f"{profile.null_percentage:.1f}%",
                            'Unique %': f"{profile.unique_percentage:.1f}%",
                            'Sample Values': str(profile.sample_values[:3])
                        })
                    except Exception as e:
                        print(f"ERROR profiling column {column}: {e}")
            
            # Sort by column order
            column_profiles.sort(key=lambda x: columns.index(x['Column']))
            
            # Display results
            df = pd.DataFrame(column_profiles)
            print(f"\nTABLE PROFILE: {schema}.{table}")
            print("-" * 80)
            print(df.to_string(index=False))
            
            # Get table stats
            row_count, size_mb = self._get_table_size_and_count(schema, table)
            print(f"\nTABLE STATISTICS:")
            print(f"Total Rows: {row_count:,}")
            print(f"Total Columns: {len(columns)}")
            print(f"Size: {size_mb:.2f} MB")
            
        except Exception as e:
            print(f"ERROR: Failed to profile table {schema}.{table}: {e}")
    
    # ============================================================================
    # LAYER 2: LOGICAL BLUEPRINT TOOLS
    # ============================================================================
    
    def automated_pk_detection(self) -> None:
        """
        Option 3: Automated Primary Key detection for a table.
        
        Analyzes all columns and recommends strong Primary Key candidates
        based on uniqueness and null constraints.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        # Interactive table selection
        print("\n" + "="*80)
        print("PRIMARY KEY DETECTION - SELECT TABLE")
        print("="*80)
        
        selected_table = self._select_table()
        if not selected_table:
            print("Table selection cancelled.")
            return
        
        schema, table = selected_table
        
        print(f"\n" + "="*80)
        print(f"LAYER 2: PRIMARY KEY DETECTION - {schema}.{table}")
        print("="*80)
        
        try:
            # Get existing primary key
            pk_query = """
            SELECT column_name
            FROM information_schema.key_column_usage k
            JOIN information_schema.table_constraints t
            ON k.constraint_name = t.constraint_name
            WHERE t.constraint_type = 'PRIMARY KEY'
            AND k.table_schema = :schema
            AND k.table_name = :table
            ORDER BY k.ordinal_position
            """
            pk_df = self._execute_query(pk_query, {'schema': schema, 'table': table})
            
            if not pk_df.empty:
                existing_pk = pk_df['column_name'].tolist()
                print(f"EXISTING PRIMARY KEY: {', '.join(existing_pk)}")
            else:
                print("NO EXISTING PRIMARY KEY FOUND")
            
            # Get all columns
            columns_query = """
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            columns_df = self._execute_query(columns_query, {'schema': schema, 'table': table})
            
            if columns_df.empty:
                print(f"Table {schema}.{table} not found.")
                return
            
            columns = columns_df['column_name'].tolist()
            
            print(f"\nAnalyzing {len(columns)} columns for PK candidates...")
            
            # Analyze each column
            pk_candidates = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_column = {
                    executor.submit(self._analyze_pk_candidate, schema, table, col): col
                    for col in columns
                }
                
                for future in as_completed(future_to_column):
                    column = future_to_column[future]
                    try:
                        result = future.result()
                        if result:
                            pk_candidates.append(result)
                    except Exception as e:
                        print(f"ERROR analyzing column {column}: {e}")
            
            # Sort by score (uniqueness * (100 - null_percentage))
            pk_candidates.sort(key=lambda x: x['Score'], reverse=True)
            
            # Display results
            print(f"\nPRIMARY KEY CANDIDATES:")
            print("-" * 80)
            
            if pk_candidates:
                df = pd.DataFrame(pk_candidates)
                print(df.to_string(index=False))
                
                # Recommendations
                print(f"\nRECOMMENDATIONS:")
                perfect_candidates = [c for c in pk_candidates if c['Score'] >= 100]
                if perfect_candidates:
                    print(f"✓ PERFECT CANDIDATES (100% unique, 0% null): {', '.join([c['Column'] for c in perfect_candidates])}")
                else:
                    top_candidate = pk_candidates[0]
                    if top_candidate['Score'] >= 95:
                        print(f"✓ STRONG CANDIDATE: {top_candidate['Column']} (Score: {top_candidate['Score']:.1f})")
                    else:
                        print(f"⚠ WEAK CANDIDATES: Consider composite key or adding surrogate key")
            else:
                print("No suitable primary key candidates found.")
                print("RECOMMENDATION: Consider adding a surrogate key (auto-increment ID)")
            
        except Exception as e:
            print(f"ERROR: Failed to analyze primary keys for {schema}.{table}: {e}")
    
    def _analyze_pk_candidate(self, schema: str, table: str, column: str) -> Optional[Dict]:
        """
        Analyze a column as a potential primary key candidate.
        
        Args:
            schema: Schema name
            table: Table name
            column: Column name
            
        Returns:
            Dictionary with candidate analysis or None if unsuitable
        """
        try:
            # Get column statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column}) as non_null_count,
                COUNT(DISTINCT {column}) as unique_count
            FROM {schema}.{table}
            """
            stats_df = self._execute_query(stats_query)
            
            total = stats_df.iloc[0]['total_count']
            non_null = stats_df.iloc[0]['non_null_count']
            unique = stats_df.iloc[0]['unique_count']
            
            if total == 0:
                return None
            
            null_percentage = ((total - non_null) / total * 100)
            unique_percentage = (unique / total * 100)
            
            # Score: uniqueness weighted by non-null percentage
            score = unique_percentage * (100 - null_percentage) / 100
            
            # Only return candidates with reasonable scores
            if score >= 50:  # At least 50% viable
                return {
                    'Column': column,
                    'Unique %': f"{unique_percentage:.1f}%",
                    'Null %': f"{null_percentage:.1f}%",
                    'Score': score,
                    'Recommendation': self._get_pk_recommendation(unique_percentage, null_percentage)
                }
            
            return None
            
        except Exception as e:
            print(f"WARNING: Could not analyze PK candidate {column}: {e}")
            return None
    
    def _get_pk_recommendation(self, unique_pct: float, null_pct: float) -> str:
        """Get recommendation text for PK candidate."""
        if unique_pct == 100 and null_pct == 0:
            return "PERFECT"
        elif unique_pct >= 99 and null_pct <= 1:
            return "EXCELLENT"
        elif unique_pct >= 95 and null_pct <= 5:
            return "GOOD"
        elif unique_pct >= 80 and null_pct <= 10:
            return "FAIR"
        else:
            return "POOR"
    
    def automated_fk_suggester(self) -> None:
        """
        Option 4: Automated Foreign Key relationship suggester.
        
        Analyzes potential Foreign Key relationships by finding columns
        with matching names and compatible data types across tables.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        # Interactive table selection
        print("\n" + "="*80)
        print("FOREIGN KEY SUGGESTER - SELECT SOURCE TABLE")
        print("="*80)
        
        selected_table = self._select_table()
        if not selected_table:
            print("Table selection cancelled.")
            return
        
        source_schema, source_table = selected_table
        
        print(f"\n" + "="*80)
        print(f"LAYER 2: FOREIGN KEY SUGGESTIONS - {source_schema}.{source_table}")
        print("="*80)
        
        try:
            # Get source table columns
            source_columns_query = """
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            source_df = self._execute_query(source_columns_query, {
                'schema': source_schema, 'table': source_table
            })
            
            if source_df.empty:
                print(f"Source table {source_schema}.{source_table} not found.")
                return
            
            # Get all other tables
            all_tables = self._get_table_list()
            target_tables = [(s, t) for s, t in all_tables if not (s == source_schema and t == source_table)]
            
            if not target_tables:
                print("No other tables found for comparison.")
                return
            
            print(f"Analyzing relationships with {len(target_tables)} other tables...")
            
            # Find potential FK relationships in parallel
            fk_candidates = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_table = {
                    executor.submit(self._find_fk_candidates, source_schema, source_table, 
                                  source_df, target_schema, target_table): (target_schema, target_table)
                    for target_schema, target_table in target_tables
                }
                
                for future in as_completed(future_to_table):
                    target_schema, target_table = future_to_table[future]
                    try:
                        candidates = future.result()
                        fk_candidates.extend(candidates)
                    except Exception as e:
                        print(f"ERROR analyzing {target_schema}.{target_table}: {e}")
            
            # Sort by confidence and match percentage
            fk_candidates.sort(key=lambda x: (
                {'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}[x.confidence],
                x.match_percentage
            ), reverse=True)
            
            # Display results
            print(f"\nFOREIGN KEY SUGGESTIONS:")
            print("-" * 80)
            
            if fk_candidates:
                # Group by confidence level
                high_confidence = [c for c in fk_candidates if c.confidence == 'HIGH']
                medium_confidence = [c for c in fk_candidates if c.confidence == 'MEDIUM']
                low_confidence = [c for c in fk_candidates if c.confidence == 'LOW']
                
                for level, candidates in [('HIGH CONFIDENCE', high_confidence), 
                                        ('MEDIUM CONFIDENCE', medium_confidence),
                                        ('LOW CONFIDENCE', low_confidence)]:
                    if candidates:
                        print(f"\n{level} RELATIONSHIPS:")
                        for candidate in candidates[:5]:  # Show top 5 per level
                            print(f"  ✓ {candidate.source_table}.{candidate.source_column} -> "
                                f"{candidate.target_table}.{candidate.target_column} "
                                f"({candidate.match_percentage:.1f}% match)")
                
                print(f"\nTOTAL SUGGESTIONS: {len(fk_candidates)}")
                
            else:
                print("No potential foreign key relationships found.")
                print("This could indicate:")
                print("- Table is a root/dimension table")
                print("- Column naming conventions don't follow patterns")
                print("- Data types are incompatible")
            
        except Exception as e:
            print(f"ERROR: Failed to analyze foreign key relationships: {e}")
    
    def _find_fk_candidates(self, source_schema: str, source_table: str, source_df: pd.DataFrame,
                           target_schema: str, target_table: str) -> List[ForeignKeyCandidate]:
        """
        Find foreign key candidates between source and target tables.
        
        Returns:
            List of ForeignKeyCandidate objects
        """
        candidates = []
        
        try:
            # Get target table columns
            target_columns_query = """
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            target_df = self._execute_query(target_columns_query, {
                'schema': target_schema, 'table': target_table
            })
            
            if target_df.empty:
                return candidates
            
            # Look for potential matches
            for _, source_row in source_df.iterrows():
                source_col = source_row['column_name']
                source_type = source_row['data_type']
                
                for _, target_row in target_df.iterrows():
                    target_col = target_row['column_name']
                    target_type = target_row['data_type']
                    
                    # Check if this could be a FK relationship
                    confidence, match_pct = self._evaluate_fk_relationship(
                        source_schema, source_table, source_col, source_type,
                        target_schema, target_table, target_col, target_type
                    )
                    
                    if confidence:
                        candidates.append(ForeignKeyCandidate(
                            source_table=f"{source_schema}.{source_table}",
                            source_column=source_col,
                            target_table=f"{target_schema}.{target_table}",
                            target_column=target_col,
                            confidence=confidence,
                            match_percentage=match_pct
                        ))
            
        except Exception as e:
            print(f"WARNING: Error finding FK candidates for {target_schema}.{target_table}: {e}")
        
        return candidates
    
    def _evaluate_fk_relationship(self, source_schema: str, source_table: str, source_col: str, source_type: str,
                                 target_schema: str, target_table: str, target_col: str, target_type: str) -> Tuple[Optional[str], float]:
        """
        Evaluate potential FK relationship between two columns.
        
        Returns:
            Tuple of (confidence_level, match_percentage) or (None, 0) if not viable
        """
        try:
            # Data type compatibility check
            if not self._are_types_compatible(source_type, target_type):
                return None, 0
            
            # Column name similarity check
            name_score = self._calculate_name_similarity(source_col, target_col)
            if name_score < 0.3:  # Minimum name similarity threshold
                return None, 0
            
            # Sample data validation (check if source values exist in target)
            validation_query = f"""
            WITH source_sample AS (
                SELECT DISTINCT {source_col} as val
                FROM {source_schema}.{source_table}
                WHERE {source_col} IS NOT NULL
                LIMIT 100
            ),
            target_values AS (
                SELECT DISTINCT {target_col} as val
                FROM {target_schema}.{target_table}
                WHERE {target_col} IS NOT NULL
            )
            SELECT 
                COUNT(s.val) as sample_count,
                COUNT(t.val) as match_count
            FROM source_sample s
            LEFT JOIN target_values t ON s.val = t.val
            """
            
            validation_df = self._execute_query(validation_query)
            if validation_df.empty:
                return None, 0
            
            sample_count = validation_df.iloc[0]['sample_count']
            match_count = validation_df.iloc[0]['match_count']
            
            if sample_count == 0:
                return None, 0
            
            match_percentage = (match_count / sample_count) * 100
            
            # Determine confidence level
            if match_percentage >= 80 and name_score >= 0.7:
                confidence = 'HIGH'
            elif match_percentage >= 60 and name_score >= 0.5:
                confidence = 'MEDIUM'
            elif match_percentage >= 30 and name_score >= 0.3:
                confidence = 'LOW'
            else:
                return None, 0
            
            return confidence, match_percentage
            
        except Exception as e:
            print(f"WARNING: Could not evaluate FK relationship: {e}")
            return None, 0
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible for FK relationships."""
        # Normalize types
        type1 = type1.lower()
        type2 = type2.lower()
        
        # Exact match
        if type1 == type2:
            return True
        
        # Integer types
        int_types = {'integer', 'int', 'int4', 'int8', 'bigint', 'smallint'}
        if type1 in int_types and type2 in int_types:
            return True
        
        # String types
        string_types = {'varchar', 'char', 'text', 'character varying', 'character'}
        if any(t in type1 for t in string_types) and any(t in type2 for t in string_types):
            return True
        
        return False
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity score between column names."""
        name1 = name1.lower()
        name2 = name2.lower()
        
        # Exact match
        if name1 == name2:
            return 1.0
        
        # Common FK patterns
        if name1.endswith('_id') and name2 == 'id':
            return 0.9
        if name1.endswith('_id') and name2 == name1[:-3] + '_id':
            return 0.8
        if name1 == name2 + '_id' or name2 == name1 + '_id':
            return 0.8
        
        # Substring matching
        if name1 in name2 or name2 in name1:
            return 0.6
        
        # Levenshtein-like similarity (simplified)
        max_len = max(len(name1), len(name2))
        if max_len == 0:
            return 0
        
        common_chars = len(set(name1) & set(name2))
        return common_chars / max_len * 0.5
    
    # ============================================================================
    # LAYER 3: ARCHITECTURAL AUDIT TOOLS
    # ============================================================================
    
    def schema_redundancy_checker(self) -> None:
        """
        Option 5: Check for redundant table schemas.
        
        Compares table schemas to find tables with high similarity
        that might be candidates for consolidation.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        print(f"\n" + "="*80)
        print("LAYER 3: SCHEMA REDUNDANCY ANALYSIS")
        print("="*80)
        
        try:
            # Get all tables with their schemas
            tables = self._get_table_list()
            if len(tables) < 2:
                print("Need at least 2 tables for redundancy analysis.")
                return
            
            print(f"Analyzing {len(tables)} tables for schema redundancy...")
            
            # Get schema information for all tables in parallel
            table_schemas = {}
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_table = {
                    executor.submit(self._get_table_schema, schema, table): (schema, table)
                    for schema, table in tables
                }
                
                for future in as_completed(future_to_table):
                    schema, table = future_to_table[future]
                    try:
                        table_schema = future.result()
                        if table_schema:
                            table_schemas[f"{schema}.{table}"] = table_schema
                    except Exception as e:
                        print(f"ERROR getting schema for {schema}.{table}: {e}")
            
            if len(table_schemas) < 2:
                print("Insufficient table schemas retrieved for analysis.")
                return
            
            # Compare all table pairs
            redundant_pairs = []
            table_names = list(table_schemas.keys())
            
            for i, table1 in enumerate(table_names):
                for j, table2 in enumerate(table_names[i+1:], i+1):
                    similarity = self._calculate_schema_similarity(
                        table_schemas[table1], 
                        table_schemas[table2]
                    )
                    
                    if similarity >= 0.75:  # 75% similarity threshold
                        redundant_pairs.append({
                            'Table 1': table1,
                            'Table 2': table2,
                            'Similarity': f"{similarity*100:.1f}%",
                            'Recommendation': self._get_redundancy_recommendation(similarity)
                        })
            
            # Sort by similarity descending
            redundant_pairs.sort(key=lambda x: float(x['Similarity'].rstrip('%')), reverse=True)
            
            # Display results
            print(f"\nSCHEMA REDUNDANCY ANALYSIS:")
            print("-" * 80)
            
            if redundant_pairs:
                df = pd.DataFrame(redundant_pairs)
                print(df.to_string(index=False))
                
                print(f"\nSUMMARY:")
                print(f"✓ Found {len(redundant_pairs)} potentially redundant table pairs")
                
                high_redundancy = [p for p in redundant_pairs if float(p['Similarity'].rstrip('%')) >= 95]
                if high_redundancy:
                    print(f"⚠ HIGH PRIORITY: {len(high_redundancy)} pairs with >95% similarity")
                
            else:
                print("✓ No significant schema redundancy detected.")
                print("All tables appear to have unique structures.")
            
        except Exception as e:
            print(f"ERROR: Failed to analyze schema redundancy: {e}")
    
    def _get_table_schema(self, schema: str, table: str) -> Optional[Dict[str, str]]:
        """
        Get the schema (column names and types) for a table.
        
        Returns:
            Dictionary mapping column names to data types
        """
        try:
            query = """
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            df = self._execute_query(query, {'schema': schema, 'table': table})
            
            if df.empty:
                return None
            
            return dict(zip(df['column_name'], df['data_type']))
            
        except Exception as e:
            print(f"WARNING: Could not get schema for {schema}.{table}: {e}")
            return None
    
    def _calculate_schema_similarity(self, schema1: Dict[str, str], schema2: Dict[str, str]) -> float:
        """
        Calculate similarity between two table schemas.
        
        Returns:
            Similarity score between 0 and 1
        """
        if not schema1 or not schema2:
            return 0.0
        
        # Get column sets
        cols1 = set(schema1.keys())
        cols2 = set(schema2.keys())
        
        # Calculate column name similarity
        common_cols = cols1 & cols2
        total_cols = cols1 | cols2
        
        if not total_cols:
            return 0.0
        
        name_similarity = len(common_cols) / len(total_cols)
        
        # Calculate data type similarity for common columns
        type_matches = 0
        for col in common_cols:
            if self._are_types_compatible(schema1[col], schema2[col]):
                type_matches += 1
        
        type_similarity = type_matches / len(common_cols) if common_cols else 0
        
        # Combined similarity (weighted average)
        return (name_similarity * 0.7) + (type_similarity * 0.3)
    
    def _get_redundancy_recommendation(self, similarity: float) -> str:
        """Get recommendation based on similarity score."""
        if similarity >= 0.95:
            return "CONSOLIDATE"
        elif similarity >= 0.85:
            return "REVIEW"
        else:
            return "MONITOR"
    
    def duplicate_row_finder(self) -> None:
        """
        Option 6: Find duplicate rows in a specific table.
        
        Analyzes a table for fully duplicate rows and reports
        the number of duplicated records.
        """
        if not self.current_engine:
            print("ERROR: No database connection. Please switch to an environment first.")
            return
        
        # Interactive table selection
        print("\n" + "="*80)
        print("DUPLICATE ROW FINDER - SELECT TABLE")
        print("="*80)
        
        selected_table = self._select_table()
        if not selected_table:
            print("Table selection cancelled.")
            return
        
        schema, table = selected_table
        
        print(f"\n" + "="*80)
        print(f"LAYER 3: DUPLICATE ROW ANALYSIS - {schema}.{table}")
        print("="*80)
        
        try:
            # First, get table row count
            count_query = f"SELECT COUNT(*) as total_rows FROM {schema}.{table}"
            count_df = self._execute_query(count_query)
            total_rows = count_df.iloc[0]['total_rows']
            
            if total_rows == 0:
                print("Table is empty - no duplicates possible.")
                return
            
            print(f"Analyzing {total_rows:,} rows for duplicates...")
            
            # Get column list
            columns_query = """
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
            """
            columns_df = self._execute_query(columns_query, {'schema': schema, 'table': table})
            
            if columns_df.empty:
                print(f"Could not retrieve columns for {schema}.{table}")
                return
            
            columns = columns_df['column_name'].tolist()
            column_list = ', '.join(columns)
            
            # Find duplicates
            duplicate_query = f"""
            WITH duplicate_groups AS (
                SELECT {column_list}, COUNT(*) as duplicate_count
                FROM {schema}.{table}
                GROUP BY {column_list}
                HAVING COUNT(*) > 1
            )
            SELECT 
                COUNT(*) as duplicate_groups,
                SUM(duplicate_count) as total_duplicate_rows,
                SUM(duplicate_count - 1) as excess_rows
            FROM duplicate_groups
            """
            
            duplicate_df = self._execute_query(duplicate_query)
            
            if duplicate_df.empty or duplicate_df.iloc[0]['duplicate_groups'] is None:
                print("✓ No duplicate rows found!")
                print("Table has completely unique rows.")
            else:
                duplicate_groups = duplicate_df.iloc[0]['duplicate_groups'] or 0
                total_duplicate_rows = duplicate_df.iloc[0]['total_duplicate_rows'] or 0
                excess_rows = duplicate_df.iloc[0]['excess_rows'] or 0
                
                print(f"\nDUPLICATE ROW ANALYSIS:")
                print("-" * 40)
                print(f"Total Rows: {total_rows:,}")
                print(f"Unique Rows: {total_rows - excess_rows:,}")
                print(f"Duplicate Groups: {duplicate_groups:,}")
                print(f"Total Duplicate Rows: {total_duplicate_rows:,}")
                print(f"Excess Rows: {excess_rows:,}")
                print(f"Duplication Rate: {(excess_rows/total_rows)*100:.2f}%")
                
                if excess_rows > 0:
                    # Show sample duplicates
                    sample_query = f"""
                    SELECT {column_list}, COUNT(*) as occurrences
                    FROM {schema}.{table}
                    GROUP BY {column_list}
                    HAVING COUNT(*) > 1
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                    """
                    
                    sample_df = self._execute_query(sample_query)
                    
                    print(f"\nSAMPLE DUPLICATE GROUPS (Top 5):")
                    print("-" * 40)
                    print(sample_df.to_string(index=False))
                    
                    print(f"\nRECOMMENDATION:")
                    if excess_rows / total_rows > 0.1:  # > 10% duplicates
                        print("⚠ HIGH duplication rate - investigate data quality issues")
                    else:
                        print("✓ Manageable duplication rate - consider cleanup if needed")
            
        except Exception as e:
            print(f"ERROR: Failed to analyze duplicates in {schema}.{table}: {e}")
    
    # ============================================================================
    # UTILITY FUNCTIONS
    # ============================================================================
    
    def switch_environment(self) -> None:
        """
        Option 7: Switch to a different database environment.
        
        Allows user to select and connect to a different environment
        without restarting the application.
        """
        print(f"\n" + "="*60)
        print("ENVIRONMENT SWITCHER")
        print("="*60)
        
        available_envs = list(self.config.get('connections', self.config.get('environments', {})).keys())
        
        print("Available environments:")
        for i, env in enumerate(available_envs, 1):
            status = " (CURRENT)" if env == self.current_environment else ""
            print(f"{i:2}. {env}{status}")
        print(f"{len(available_envs)+1:2}. Cancel")
        
        try:
            choice = self._get_user_choice(1, len(available_envs)+1, "Select environment")
            if not choice or choice == len(available_envs) + 1:
                print("Environment switch cancelled.")
                return
            
            selected_env = available_envs[choice - 1]
            
            if selected_env == self.current_environment:
                print(f"Already connected to {selected_env} environment.")
                
                # Offer to switch database within the same environment
                print(f"\nWould you like to switch to a different database in {selected_env}?")
                switch_db = input("Switch database? (y/n): ").strip().lower()
                if switch_db in ['y', 'yes']:
                    self._switch_database()
                return
            
            print(f"Connecting to {selected_env} environment...")
            self._create_engine(selected_env)
            
            # After successful connection, offer to select specific database
            if self.current_engine:
                print(f"\nWould you like to switch to a specific database?")
                switch_db = input("Switch database? (y/n): ").strip().lower()
                if switch_db in ['y', 'yes']:
                    self._switch_database()
            
        except Exception as e:
            print(f"ERROR: Failed to switch environment: {e}")
    
    def _switch_database(self) -> None:
        """
        Switch to a different database within the current environment.
        """
        try:
            databases = self._get_database_list()
            if not databases:
                print("No databases available.")
                return
            
            print(f"\nAvailable Databases:")
            print("-" * 40)
            for i, db in enumerate(databases, 1):
                print(f"{i:2}. {db}")
            print(f"{len(databases)+1:2}. Keep current database")
            
            choice = self._get_user_choice(1, len(databases)+1, "Select database")
            if not choice or choice == len(databases) + 1:
                print("Database selection cancelled.")
                return
            
            selected_db = databases[choice - 1]
            
            # Update the engine to use the selected database
            connections = self.config.get('connections', self.config.get('environments', {}))
            env_config = connections[self.current_environment]
            
            if 'url' in env_config:
                # Extract base URL and replace database
                import re
                base_url = re.sub(r'/[^/]*$', f'/{selected_db}', env_config['url'])
            else:
                # Construct new URL with selected database
                base_url = f"postgresql://{env_config['username']}:{env_config['password']}@{env_config['host']}:{env_config['port']}/{selected_db}"
            
            # Create new engine with selected database
            from sqlalchemy import create_engine
            self.current_engine = create_engine(base_url, pool_pre_ping=True)
            
            # Test connection
            with self.current_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print(f"✓ Successfully switched to database: {selected_db}")
            
        except Exception as e:
            print(f"ERROR: Failed to switch database: {e}")
    
    def display_menu(self) -> None:
        """Display the main menu."""
        print(f"\n" + "="*80)
        print("DATABASE DISCOVERY TOOLKIT - Principal Data Architect Edition")
        print("="*80)
        
        env_status = f"Connected to: {self.current_environment}" if self.current_environment else "No connection"
        print(f"Status: {env_status}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print(f"\n[ LAYER 1: Physical Survey Tools ]")
        print("1. Database-Wide Summary")
        print("2. Detailed Table Profiler")
        print("3. Schema Browser & Analysis")
        
        print(f"\n[ LAYER 2: Logical Blueprint Tools ]")
        print("4. Automated Primary Key (PK) Detection")
        print("5. Automated Foreign Key (FK) Suggester")
        
        print(f"\n[ LAYER 3: Architectural Audit Tools ]")
        print("6. Schema Redundancy Checker")
        print("7. Duplicate Row Finder")
        
        print(f"\n[ Utility Functions ]")
        print("8. Switch Active Environment/Database")
        print("9. Exit")
        
        print("-" * 80)
    
    def run(self) -> None:
        """
        Main application loop.
        
        Displays menu and handles user input for interactive database discovery.
        """
        print("Welcome to the Database Discovery Toolkit!")
        print("Principal Data Architect Edition")
        
        # Initial environment selection
        if not self.current_environment:
            print("\nInitial setup: Please select a database environment.")
            self.switch_environment()
            
            if not self.current_environment:
                print("No database connection established. Exiting.")
                return
        
        while True:
            try:
                self.display_menu()
                choice = input("Select an option (1-9): ").strip()
                
                if choice == '1':
                    self.database_wide_summary()
                elif choice == '2':
                    self.detailed_table_profiler()
                elif choice == '3':
                    self.schema_browser_analysis()
                elif choice == '4':
                    self.automated_pk_detection()
                elif choice == '5':
                    self.automated_fk_suggester()
                elif choice == '6':
                    self.schema_redundancy_checker()
                elif choice == '7':
                    self.duplicate_row_finder()
                elif choice == '8':
                    self.switch_environment()
                elif choice == '9':
                    print("\nThank you for using the Database Discovery Toolkit!")
                    print("Happy data engineering! 🚀")
                    break
                else:
                    print("Invalid choice. Please select a number between 1-9.")
                
                # Pause between operations
                if choice in ['1', '2', '3', '4', '5', '6', '7']:
                    input("\nPress Enter to continue...")
                    
            except KeyboardInterrupt:
                print("\n\nOperation cancelled by user.")
                print("Returning to main menu...")
            except Exception as e:
                print(f"\nERROR: Unexpected error: {e}")
                print("Returning to main menu...")


def main():
    """Main entry point for the Database Discovery Toolkit."""
    try:
        toolkit = DatabaseToolkit()
        toolkit.run()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
