# Data Archaeologist Framework

## Professional Configuration Management

This document outlines the configuration management approach for the Data Archaeologist framework, designed for enterprise database environments.

### Configuration Structure

The framework uses a centralized configuration approach with environment-specific settings:

```json
{
  "environments": {
    "staging": {
      "host": "staging-dbpostgresql.postgres.database.azure.com",
      "port": 5432,
      "database": "postgres",
      "username": "dbuser",
      "password": "${STAGING_DB_PASSWORD}",
      "connection_args": {
        "sslmode": "require",
        "connect_timeout": 30
      }
    },
    "production": {
      "host": "levelup-postgres-db.postgres.database.azure.com",
      "port": 5432,
      "database": "postgres",
      "username": "dbuser",
      "password": "${PROD_DB_PASSWORD}",
      "connection_args": {
        "sslmode": "require",
        "connect_timeout": 30
      }
    }
  }
}
```

### Security Considerations

1. **Environment Variables**: Use environment variables for sensitive data
2. **SSL/TLS**: Always require encrypted connections
3. **Connection Timeouts**: Set appropriate timeout values
4. **Credential Management**: Never commit passwords to version control

### Usage Examples

```python
from data_archaeologist import DataArchaeologist

# Initialize with configuration
archaeologist = DataArchaeologist('config.json')

# Run complete analysis
results = archaeologist.run_complete_discovery('staging')

# Export comprehensive report
report_file = archaeologist.export_comprehensive_report('staging')
```

### Environment Setup

1. Install the framework: `pip install -e .`
2. Configure database connections in `config.json`
3. Set environment variables for passwords
4. Run analysis scripts from the `scripts/` directory

### Best Practices

- Always test connections before running full analysis
- Use the staging environment for development and testing
- Schedule regular discovery runs for monitoring changes
- Export reports to version-controlled locations
- Monitor log files for error patterns and performance issues
