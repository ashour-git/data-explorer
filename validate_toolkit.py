"""Quick validation of Database Discovery Toolkit"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.getcwd())

def main():
    print("Database Discovery Toolkit - Quick Validation")
    print("=" * 50)
    
    try:
        print("Step 1: Testing imports...")
        import pandas as pd
        print("✓ pandas imported")
        
        from sqlalchemy import create_engine
        print("✓ SQLAlchemy imported")
        
        import concurrent.futures
        print("✓ concurrent.futures imported")
        
        print("\nStep 2: Testing toolkit import...")
        from database_toolkit import DatabaseToolkit
        print("✓ DatabaseToolkit imported")
        
        print("\nStep 3: Testing toolkit initialization...")
        toolkit = DatabaseToolkit()
        print("✓ Toolkit initialized")
        
        print("\nStep 4: Checking configuration...")
        config = toolkit.config
        envs = config.get('environments', config.get('connections', {}))
        print(f"✓ Found {len(envs)} environments: {list(envs.keys())}")
        
        print("\nVALIDATION COMPLETE - TOOLKIT READY!")
        print("To run the interactive toolkit:")
        print("python database_toolkit.py")
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()
