#!/usr/bin/env python3
"""
Test script for the Database Discovery Toolkit

This script validates that the toolkit can be imported and initialized properly.
"""

try:
    from database_toolkit import DatabaseToolkit
    print("✓ Successfully imported DatabaseToolkit")
    
    # Test initialization
    toolkit = DatabaseToolkit()
    print("✓ Successfully initialized toolkit")
    
    # Test config loading
    config = toolkit.config
    print(f"✓ Configuration loaded with environments: {list(config.get('environments', config.get('connections', {})).keys())}")
    
    print("\n" + "="*60)
    print("DATABASE DISCOVERY TOOLKIT - VALIDATION COMPLETE")
    print("="*60)
    print("✓ All components loaded successfully")
    print("✓ Ready for database discovery operations")
    print("\nTo run the full toolkit, execute:")
    print("python database_toolkit.py")
    
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    print("Please install required dependencies:")
    print("pip install pandas sqlalchemy psycopg2-binary")
    
except Exception as e:
    print(f"ERROR: {e}")
    print("Please check your configuration and try again.")
