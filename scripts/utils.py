import json
import os
import sqlalchemy as sa
from sqlalchemy import text
import sys

def load_config(config_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')):
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in config file {config_path}")
        sys.exit(1)

def get_connection(env='staging', allow_demo_fallback=False):
    """Get SQLAlchemy engine for the specified environment."""
    try:
        config = load_config()
        db_url = config['databases'][env]['url']
        
        # Check if it's a placeholder URL
        if 'localhost:5432' in db_url and ('user:pass' in db_url or 'username:password' in db_url):
            if allow_demo_fallback:
                print(f"⚠️  Warning: Using placeholder credentials for '{env}' environment")
                print("🎯 Automatically switching to demo mode for safe testing")
                return None  # Signal to use demo mode
            else:
                print(f"❌ Error: Placeholder credentials detected for environment '{env}'")
                print(f"Database URL: {db_url}")
                print("\nPlease either:")
                print("1. Update config.json with real database credentials")
                print("2. Use --demo flag for testing with mock data")
                sys.exit(1)
        
        engine = sa.create_engine(db_url)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
        
    except KeyError:
        print(f"Error: Environment '{env}' not found in config")
        sys.exit(1)
    except sa.exc.OperationalError as e:
        if allow_demo_fallback:
            print(f"⚠️  Warning: Cannot connect to database for environment '{env}'")
            print("🎯 Automatically switching to demo mode")
            return None  # Signal to use demo mode
        else:
            print(f"❌ Error: Cannot connect to database for environment '{env}'")
            print(f"Database URL: {db_url}")
            print(f"Error details: {str(e)}")
            print("\nPlease check:")
            print("1. Database server is running")
            print("2. Credentials are correct in config.json")
            print("3. Network connectivity to database host")
            print("\nFor testing without a real database, use --demo mode")
            sys.exit(1)
    except Exception as e:
        print(f"Unexpected error connecting to database: {str(e)}")
        sys.exit(1)

def prompt_for_environment():
    """Interactive prompt to select database environment or demo mode."""
    print("\n🔍 Database Discovery Tool")
    print("=" * 50)
    print("Select your data source:")
    print("1. Demo Mode (mock data, no database required)")
    print("2. Staging Database")
    print("3. Production Database") 
    print("4. Backup Database")
    print("5. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-5): ").strip()
            if choice == '1':
                return 'demo', True
            elif choice == '2':
                return 'staging', False
            elif choice == '3':
                return 'production', False
            elif choice == '4':
                return 'backup', False
            elif choice == '5':
                print("Goodbye!")
                sys.exit(0)
            else:
                print("❌ Invalid choice. Please enter 1-5.")
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            sys.exit(0)

def get_tables(engine):
    """Retrieve list of tables in the database."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        return [row[0] for row in result]

def get_row_count(engine, table):
    """Get row count for a table."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        return result.scalar()

def get_table_size(engine, table):
    """Get approximate table size in bytes (PostgreSQL specific)."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT pg_total_relation_size('{table}')"))
        return result.scalar()

def profile_column(engine, table, column):
    """Profile a column: data type, null percentage, unique count."""
    with engine.connect() as conn:
        # Data type
        dtype_res = conn.execute(text(f"SELECT data_type FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{column}'"))
        dtype = dtype_res.scalar()
        
        # Null percentage
        null_res = conn.execute(text(f"SELECT (COUNT(*) - COUNT({column})) * 100.0 / COUNT(*) AS null_pct FROM {table}"))
        null_pct = null_res.scalar()
        
        # Unique count
        unique_res = conn.execute(text(f"SELECT COUNT(DISTINCT {column}) FROM {table}"))
        unique = unique_res.scalar()
        
        return {'data_type': dtype, 'null_percentage': null_pct, 'unique_count': unique}

# Add more utilities as needed for performance-optimized queries
