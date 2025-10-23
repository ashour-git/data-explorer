# Data Archaeologist Framework

A professional parallel multi-environment database exploration and analysis platform implementing the three-layer archaeological methodology for reverse-engineering database architecture and business logic.

## Overview

This framework treats databases as archaeological sites built by many people over time. The goal is to reverse-engineer the blueprint of the business processes that created the data structure, moving from physical artifacts to logical relationships to business insights.

**🚀 Professional Features:**
- **Parallel Multi-Environment Analysis** - Concurrent discovery across staging/production/backup
- **Interactive Jupyter Notebooks** - Rich visualizations and widget-based exploration
- **Safe SQL Composition** - Enterprise-grade security with parameterized queries
- **Non-Interactive CI/CD Mode** - Automated analysis for continuous integration
- **Comprehensive Business Insights** - Actionable recommendations from data patterns

## Three-Layer Methodology

### Layer 1: The Physical Map (What is physically there?)

**Initial site survey and raw material inventory**

- **Table Sizing & Granularity**: Identify largest tables by row count and storage size
- **Column Profiling**: Understand data types, memory consumption, and column characteristics  
- **Initial Quality Survey**: Assess NULL percentages and data quality indicators

**Key Insight**: Large tables (millions of rows) are typically fact/action tables where business events are recorded. Small tables (hundreds/thousands of rows) are dimension/lookup tables describing entities.

### Layer 2: The Logical Blueprint (How does it connect?)

**Architectural blueprint reconstruction through relationship discovery**

- **Finding the Anchors (Primary Keys)**: Systematically identify unique, non-null identifiers for each table
- **Finding the Bridges (Foreign Keys)**: Detective work to discover columns that reference primary keys in other tables
- **Understanding Traffic Flow (Cardinality)**: Analyze relationship patterns (one-to-many, many-to-many)

**Key Insight**: Rebuilding the Entity-Relationship Diagram from data clues enables understanding of how systems connect and the foundation for correct JOIN operations.

### Layer 3: The Business Story (Why does it exist?)

**Business process synthesis and historical analysis**

- **Inferring Business Processes**: Identify core business workflows from table relationships
- **Uncovering History**: Discover artifacts of data migrations and system evolution
- **Formulating Actionable Insights**: Provide concrete recommendations for optimization

**Key Insight**: Transform physical and logical analysis into business understanding and strategic recommendations.

## Project Structure

```
data_archaeologist/                # Core analysis framework
├── core/
│   ├── database_connection.py     # Multi-environment connectivity
│   ├── logging_config.py          # Professional logging setup
│   └── utils.py                   # Common utilities
├── layer1_physical/
│   ├── database_inventory.py      # Schema and table discovery
│   ├── table_sizing.py            # Size and granularity analysis
│   └── column_profiling.py        # Column-level data profiling
├── layer2_logical/
│   ├── primary_key_detection.py   # Primary key identification
│   ├── foreign_key_detection.py   # Foreign key discovery
│   └── cardinality_analysis.py    # Relationship pattern analysis
├── layer3_business/
│   └── business_inference.py      # Business process analysis
├── archaeologist.py              # Main orchestration module
└── workflow.py                   # Command-line interface

scripts/                          # Analysis and workflow scripts
├── parallel_discovery.py         # Multi-environment parallel analysis
├── interactive_workflow.py       # Enhanced CLI with safe SQL
├── database_summary_real.py      # Core table analysis functions
├── test_database_connections.py  # Connection validation
└── utils.py                      # Shared utilities

notebooks/                        # Interactive Jupyter analysis
├── 00_setup.ipynb               # Environment setup and validation
├── 01_layer1_physical_map.ipynb # Physical layer interactive analysis
├── 02_layer2_logical_blueprint.ipynb # Logical relationships and keys
└── 03_layer3_business_story.ipynb    # Business insights and recommendations

config.json                       # Multi-environment configuration
launch_explorer.py               # Main entry point
```

## Usage

### Interactive Exploration (Recommended)
```bash
# Launch interactive workflow
python launch_explorer.py

# Or run specific notebooks
jupyter notebook notebooks/00_setup.ipynb
```

### Parallel Multi-Environment Analysis
```bash
# Analyze all environments in parallel
python scripts/parallel_discovery.py

# Non-interactive mode for CI/CD
EXPLORER_NON_INTERACTIVE=1 python scripts/parallel_discovery.py
```

### Layer-by-Layer Analysis
```bash
# Complete archaeological analysis
python -m data_archaeologist.workflow complete staging

# Individual layers
python -m data_archaeologist.workflow layer1 staging  # Physical mapping
python -m data_archaeologist.workflow layer2 staging  # Logical blueprint  
python -m data_archaeologist.workflow layer3 staging  # Business story
```

### Individual Scripts
```bash
# Database overview
python scripts/1_summarize_database.py --server staging

# Table profiling
python scripts/2_profile_table.py --table users --server staging

# Primary key analysis
python scripts/3_find_primary_keys.py --server staging

# Foreign key discovery
python scripts/4_find_foreign_keys.py --server staging

# Schema optimization
python scripts/5_check_schema_redundancy.py --server staging
```

## Configuration

Update `config.json` with database connection details:

```json
{
  "databases": {
    "staging": {
      "host": "staging-server.com",
      "port": 5432,
      "database": "database_name",
      "username": "username",
      "password": "password"
    },
    "production": {
      "host": "production-server.com", 
      "port": 5432,
      "database": "database_name",
      "username": "username",
      "password": "password"
    }
  }
}
```

## Output Examples

### Layer 1: Physical Map
```
DATABASE ANALYSIS REPORT - STAGING DATABASE
Data Source: LIVE DATABASE DATA
================================================================================
Total Tables: 47
Total Rows: 15,847,293
Total Size: 12.3 GB
================================================================================
Table Name               Schema       Rows         Size      
--------------------------------------------------------------------------------
event_logs              public       8,500,000    4.2 GB    
transactions            public       2,100,000    1.8 GB    
user_sessions           public       1,850,000    987.5 MB  
products                catalog      15,000       8.4 MB    
users                   public       12,500       1.2 MB    
```

### Layer 2: Logical Blueprint
```
PRIMARY KEY ANALYSIS REPORT
================================================================================
Tables Analyzed: 47
Primary Keys Found: 44/47 (93.6%)
Composite Keys: 3
Single Column Keys: 41

FOREIGN KEY RELATIONSHIP DISCOVERY
================================================================================
Declared Relationships: 28
Potential Relationships: 15
High Confidence Matches: 12

KEY RELATIONSHIP HUBS:
- users.id: Referenced by 8 tables
- products.id: Referenced by 6 tables  
- organizations.id: Referenced by 4 tables
```

### Layer 3: Business Story
```
BUSINESS PROCESS ANALYSIS
================================================================================
Core Business Domains Identified: 4

E-COMMERCE WORKFLOW:
- Customer management: users → user_profiles → user_sessions
- Product catalog: categories → products → product_variants
- Order processing: orders → order_items → payments
- Inventory: warehouses → inventory → stock_movements

HISTORICAL ARTIFACTS:
- legacy_user_id column (100% NULL) - indicates past migration
- products_v1, products_v2 tables - redundant structure detected

OPTIMIZATION RECOMMENDATIONS:
1. Consolidate product tables to eliminate redundancy
2. Add missing foreign key constraints for data integrity
3. Consider partitioning event_logs table for performance
```

## Dependencies

- Python 3.8+
- PostgreSQL database access
- Required packages: `pip install -r requirements.txt`

## Professional Standards

This framework follows enterprise software development practices:
- Type hints for code clarity
- Comprehensive error handling
- Structured logging
- Modular architecture
- Professional documentation
- Test coverage
- Configuration management
