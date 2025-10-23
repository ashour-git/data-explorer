# Database Discovery Toolkit - Principal Data Architect Edition

A comprehensive, interactive command-line toolkit for deep database discovery and architectural auditing. This toolkit implements a three-layer discovery process for understanding and documenting database environments.

## 🎯 Project Philosophy

The Database Discovery Toolkit embodies a **three-layer discovery process**, moving from surface-level inventory to deep architectural understanding:

### **Layer 1: Physical Survey** - "What is physically there?"
- Database-wide inventory and profiling of tables and columns
- Row counts, storage sizes, and basic statistics
- Detailed column-by-column analysis

### **Layer 2: Logical Blueprint** - "How does it connect?"
- Reverse-engineering relationships between tables
- Automated Primary Key detection
- Intelligent Foreign Key relationship suggestions

### **Layer 3: Architectural Audit** - "Is it well-designed?"
- Schema redundancy analysis
- Duplicate row detection
- Data quality assessment

## 🚀 Key Features

- **Multi-Environment Support**: Connect to staging, production, backup environments
- **Parallel Processing**: High-performance analysis using ThreadPoolExecutor
- **Intelligent Analysis**: AI-powered relationship detection and schema comparison
- **Comprehensive Reporting**: Clean, formatted output with actionable insights
- **Flexible Configuration**: Support for multiple database types and connection formats

## 📋 Prerequisites

- Python 3.8+
- Required packages: `pandas`, `SQLAlchemy`, `psycopg2-binary`
- PostgreSQL database (easily adaptable to other databases)

## ⚙️ Configuration

Create a `config.json` file in the same directory with your database connections:

### Simple URL Format:
```json
{
  "connections": {
    "staging": {
      "url": "postgresql://user:password@staging-host:5432/dbname"
    },
    "production": {
      "url": "postgresql://user:password@production-host:5432/dbname"
    },
    "backup": {
      "url": "postgresql://user:password@backup-host:5432/dbname"
    }
  }
}
```

### Detailed Format:
```json
{
  "environments": {
    "staging": {
      "host": "staging-host",
      "port": 5432,
      "database": "dbname",
      "username": "user",
      "password": "password",
      "connection_args": {
        "sslmode": "require"
      }
    }
  }
}
```

## 🎮 Usage

### Quick Start
```bash
python database_toolkit.py
```

### Validation
```bash
python validate_toolkit.py
```

## 📊 Menu Options

### Layer 1: Physical Survey Tools
1. **Database-Wide Summary**
   - Parallel analysis of all tables
   - Row counts and storage sizes
   - Sorted by size for quick insights

2. **Detailed Table Profiler**
   - Column-by-column breakdown
   - Data types, null percentages, uniqueness
   - Sample values for quick understanding

### Layer 2: Logical Blueprint Tools
3. **Automated Primary Key Detection**
   - Analyzes all columns for PK candidates
   - Scoring based on uniqueness and null constraints
   - Clear recommendations with confidence levels

4. **Automated Foreign Key Suggester**
   - Intelligent relationship discovery
   - Column name pattern matching
   - Data validation with confidence scoring
   - High/Medium/Low confidence categorization

### Layer 3: Architectural Audit Tools
5. **Schema Redundancy Checker**
   - Parallel schema comparison
   - Similarity scoring for potential consolidation
   - Identifies tables with >95% structural similarity

6. **Duplicate Row Finder**
   - Full row duplication analysis
   - Statistical reporting with recommendations
   - Sample duplicate groups for investigation

### Utility Functions
7. **Switch Active Environment**
   - Seamless environment switching
   - Connection validation
   - No restart required

8. **Exit**

## 🏗️ Architecture

### Core Components

- **DatabaseToolkit**: Main class orchestrating all operations
- **TableInfo**: Data structure for table metadata
- **ColumnInfo**: Data structure for column profiling
- **ForeignKeyCandidate**: Data structure for relationship suggestions

### Key Methods

- `database_wide_summary()`: Parallel table analysis
- `detailed_table_profiler()`: Deep column profiling
- `automated_pk_detection()`: Primary key analysis
- `automated_fk_suggester()`: Relationship discovery
- `schema_redundancy_checker()`: Architectural analysis
- `duplicate_row_finder()`: Data quality assessment

### Performance Features

- **ThreadPoolExecutor**: Parallel processing for large databases
- **Connection Pooling**: Efficient database connections
- **Caching**: Reduced redundant queries
- **Batch Processing**: Optimized for large datasets

## 🎯 Use Cases

### Data Engineering
- **New Environment Assessment**: Quickly understand unfamiliar databases
- **Migration Planning**: Identify relationships and dependencies
- **Data Quality Audits**: Find duplicates and inconsistencies

### Database Administration
- **Performance Analysis**: Identify large tables and optimization opportunities
- **Schema Optimization**: Find redundant structures
- **Relationship Documentation**: Auto-generate ER diagrams data

### Data Architecture
- **Technical Debt Assessment**: Identify design issues
- **Consolidation Planning**: Find merge candidates
- **Quality Governance**: Establish data quality baselines

## 📈 Sample Output

### Database-Wide Summary
```
DATABASE SUMMARY (Sorted by Size):
--------------------------------------------------------------------------------
Schema    Table              Row Count    Size (MB)
public    user_transactions  1,234,567    245.67
public    product_catalog    456,789      89.34
public    user_profiles      123,456      34.12
...
```

### Primary Key Analysis
```
PRIMARY KEY CANDIDATES:
--------------------------------------------------------------------------------
Column       Unique %    Null %    Score    Recommendation
user_id      100.0%      0.0%      100.0    PERFECT
email        99.8%       0.1%      99.7     EXCELLENT
username     95.2%       2.1%      93.2     GOOD
```

### Foreign Key Suggestions
```
HIGH CONFIDENCE RELATIONSHIPS:
  ✓ orders.customer_id -> customers.id (98.5% match)
  ✓ order_items.product_id -> products.id (97.2% match)

MEDIUM CONFIDENCE RELATIONSHIPS:
  ✓ reviews.user_id -> users.id (87.3% match)
```

## 🔧 Customization

### Adding New Database Types
Modify the `_are_types_compatible()` method to support additional database type mappings.

### Custom Analysis
Extend the toolkit by adding new methods to the `DatabaseToolkit` class and updating the menu system.

### Output Formats
Modify the display methods to support JSON, CSV, or other output formats.

## 🚨 Error Handling

The toolkit includes comprehensive error handling:
- Database connection failures
- Invalid table/column names
- SQL execution errors
- Configuration issues
- Missing dependencies

## 📝 Best Practices

1. **Start with Layer 1**: Get the physical inventory before diving deeper
2. **Use Multiple Environments**: Compare staging vs production differences
3. **Focus on High-Confidence Results**: Prioritize recommendations with high scores
4. **Validate Suggestions**: Always verify automated suggestions before implementation
5. **Regular Audits**: Run architectural audits periodically

## 🎓 Technical Notes

### SQL Compatibility
- Designed for PostgreSQL but easily adaptable
- Uses standard `information_schema` views
- Custom queries for PostgreSQL-specific features

### Performance Considerations
- Parallel processing for large databases
- Configurable thread pool size
- Connection pooling for efficiency
- Query optimization for large tables

### Security
- No SQL injection vulnerabilities (uses parameterized queries)
- Secure connection handling
- Password protection in configuration

## 🤝 Contributing

This toolkit was designed as a comprehensive solution for database discovery. Contributions welcome for:
- Additional database type support
- New analysis algorithms
- Performance optimizations
- Output format enhancements

## 📄 License

This toolkit is provided as-is for database discovery and architectural analysis purposes.

---

**Happy Database Discovery! 🚀**

*Principal Data Architect Edition - Built for comprehensive database understanding*
