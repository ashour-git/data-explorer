"""
Data Archaeologist Workflow Scripts
Simple command-line tools for running individual analysis layers
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data_archaeologist.core import setup_logging, DatabaseConnection
from data_archaeologist.archaeologist import DataArchaeologist

def run_layer1_only(environment: str):
    """Run only Layer 1 physical analysis."""
    setup_logging('layer1_analysis.log')
    
    from data_archaeologist.layer1_physical import DatabaseInventory, TableSizingAnalyzer, ColumnProfiler
    
    db_conn = DatabaseConnection()
    
    print(f"Running Layer 1 Physical Analysis for {environment}")
    print("="*60)
    
    # Database Inventory
    print("1. Database and Schema Inventory...")
    inventory = DatabaseInventory(db_conn)
    inv_report = inventory.generate_inventory_report(environment)
    print(f"   Report: {inv_report}")
    
    # Table Sizing
    print("2. Table Sizing Analysis...")
    sizing = TableSizingAnalyzer(db_conn)
    size_report = sizing.generate_sizing_report(environment)
    print(f"   Report: {size_report}")
    
    # Column Profiling (limited)
    print("3. Column Profiling...")
    profiler = ColumnProfiler(db_conn)
    col_report = profiler.generate_column_profile_report(environment)
    print(f"   Report: {col_report}")
    
    print("\nLayer 1 Analysis Complete!")

def run_layer2_only(environment: str):
    """Run only Layer 2 logical analysis."""
    setup_logging('layer2_analysis.log')
    
    from data_archaeologist.layer2_logical import PrimaryKeyDetective, ForeignKeyDetective, CardinalityAnalyzer
    
    db_conn = DatabaseConnection()
    
    print(f"Running Layer 2 Logical Analysis for {environment}")
    print("="*60)
    
    # Primary Key Detection
    print("1. Primary Key Detection...")
    pk_detective = PrimaryKeyDetective(db_conn)
    pk_report = pk_detective.generate_primary_key_report(environment)
    print(f"   Report: {pk_report}")
    
    # Foreign Key Detection
    print("2. Foreign Key Detection...")
    fk_detective = ForeignKeyDetective(db_conn)
    fk_report = fk_detective.generate_foreign_key_report(environment)
    print(f"   Report: {fk_report}")
    
    # Cardinality Analysis
    print("3. Cardinality Analysis...")
    card_analyzer = CardinalityAnalyzer(db_conn)
    card_report = card_analyzer.generate_cardinality_report(environment)
    print(f"   Report: {card_report}")
    
    print("\nLayer 2 Analysis Complete!")

def run_layer3_only(environment: str):
    """Run only Layer 3 business analysis."""
    setup_logging('layer3_analysis.log')
    
    from data_archaeologist.layer3_business import BusinessProcessInference
    
    db_conn = DatabaseConnection()
    
    print(f"Running Layer 3 Business Analysis for {environment}")
    print("="*60)
    
    # Business Process Inference
    print("1. Business Process Inference...")
    business = BusinessProcessInference(db_conn)
    business_report = business.generate_business_story_report(environment)
    print(f"   Report: {business_report}")
    
    print("\nLayer 3 Analysis Complete!")

def run_complete_analysis(environment: str):
    """Run complete three-layer analysis."""
    setup_logging('complete_analysis.log')
    
    archaeologist = DataArchaeologist()
    
    print(f"Running Complete Database Archaeology for {environment}")
    print("="*60)
    
    results = archaeologist.run_complete_discovery(environment, parallel_execution=True)
    report_file = archaeologist.export_comprehensive_report(environment)
    
    print(f"\nComplete Analysis Finished!")
    print(f"Comprehensive Report: {report_file}")
    
    # Show summary
    exec_summary = results.get('executive_summary', {})
    print(f"\nKey Findings:")
    for finding in exec_summary.get('key_findings', [])[:3]:
        print(f"  • {finding}")

def show_environments():
    """Show available database environments."""
    try:
        db_conn = DatabaseConnection()
        environments = db_conn.get_available_environments()
        
        print("Available Database Environments:")
        print("="*40)
        for i, env in enumerate(environments, 1):
            print(f"{i}. {env}")
            
            # Test connection
            test_result = db_conn.test_connection(env)
            status = "✓ Connected" if test_result['status'] == 'success' else "✗ Connection Failed"
            print(f"   Status: {status}")
            
            if test_result['status'] == 'success':
                db_info = test_result.get('database_info', {})
                print(f"   Database: {db_info.get('database_name', 'Unknown')}")
                print(f"   Size: {db_info.get('database_size', 'Unknown')}")
            
            print()
            
    except Exception as e:
        print(f"Error loading environments: {e}")

def main():
    """Main workflow interface."""
    if len(sys.argv) < 2:
        print("Data Archaeologist Workflow Tools")
        print("="*40)
        print("Usage:")
        print("  python workflow.py environments          # Show available environments")
        print("  python workflow.py layer1 <environment>  # Run Layer 1 only")
        print("  python workflow.py layer2 <environment>  # Run Layer 2 only") 
        print("  python workflow.py layer3 <environment>  # Run Layer 3 only")
        print("  python workflow.py complete <environment> # Run complete analysis")
        print()
        print("Layer Descriptions:")
        print("  Layer 1: Physical Map - What is physically there?")
        print("  Layer 2: Logical Blueprint - How does it connect?")
        print("  Layer 3: Business Story - Why does it exist?")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'environments':
        show_environments()
        
    elif command == 'layer1':
        if len(sys.argv) < 3:
            print("Error: Please specify environment for Layer 1 analysis")
            sys.exit(1)
        run_layer1_only(sys.argv[2])
        
    elif command == 'layer2':
        if len(sys.argv) < 3:
            print("Error: Please specify environment for Layer 2 analysis")
            sys.exit(1)
        run_layer2_only(sys.argv[2])
        
    elif command == 'layer3':
        if len(sys.argv) < 3:
            print("Error: Please specify environment for Layer 3 analysis")
            sys.exit(1)
        run_layer3_only(sys.argv[2])
        
    elif command == 'complete':
        if len(sys.argv) < 3:
            print("Error: Please specify environment for complete analysis")
            sys.exit(1)
        run_complete_analysis(sys.argv[2])
        
    else:
        print(f"Error: Unknown command '{command}'")
        print("Use 'python workflow.py' to see available commands")
        sys.exit(1)

if __name__ == "__main__":
    main()
