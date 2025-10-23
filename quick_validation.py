#!/usr/bin/env python3
"""
Quick validation script to test the fixes
"""

import sys
import os
import py_compile

def test_syntax():
    """Test Python syntax of main scripts."""
    scripts_to_test = [
        "scripts/1_summarize_database.py",
        "scripts/interactive_workflow.py", 
        "scripts/database_summary_real.py"
    ]
    
    print("=== Syntax Validation ===")
    for script in scripts_to_test:
        try:
            py_compile.compile(script, doraise=True)
            print(f"✓ {script} - Syntax OK")
        except py_compile.PyCompileError as e:
            print(f"✗ {script} - Syntax Error: {e}")
            return False
    return True

def test_config():
    """Test configuration loading."""
    print("\n=== Configuration Test ===")
    try:
        import json
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        environments = config.get('environments', {})
        print(f"✓ Configuration loaded successfully")
        print(f"✓ Available environments: {list(environments.keys())}")
        
        for env_name, env_config in environments.items():
            required_fields = ['host', 'port', 'database', 'username', 'password']
            missing = [field for field in required_fields if field not in env_config]
            if missing:
                print(f"✗ {env_name} missing fields: {missing}")
                return False
            else:
                print(f"✓ {env_name} configuration complete")
        return True
        
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        return False

def main():
    """Run validation tests."""
    print("Data Archaeologist Framework - Quick Validation")
    print("=" * 50)
    
    tests = [
        test_syntax(),
        test_config()
    ]
    
    print("\n=== Summary ===")
    if all(tests):
        print("✅ All validation tests PASSED!")
        print("\nYour workspace is ready with 3 database servers:")
        print("  • staging - staging-dbpostgresql.postgres.database.azure.com")
        print("  • production - levelup-postgres-db.postgres.database.azure.com")
        print("  • backup - levelup-backup.postgres.database.azure.com")
        print("\nNext steps:")
        print("  python scripts/interactive_workflow.py")
        print("  or: python scripts/1_summarize_database.py --server staging")
    else:
        print("❌ Some tests FAILED - please check the errors above")

if __name__ == "__main__":
    main()
