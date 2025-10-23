"""
Core utilities and common functions for Data Archaeologist framework
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import json
from .logging_config import setup_professional_logging

def setup_logging(log_file: str = 'data_archaeologist.log', 
                  level: int = logging.INFO) -> None:
    """Configure logging for the Data Archaeologist framework."""
    # Use professional logging configuration
    setup_professional_logging()

def format_table_name(schema: str, table: str) -> str:
    """Format schema and table name consistently."""
    return f"{schema}.{table}"

def calculate_null_percentage(null_count: int, total_count: int) -> float:
    """Calculate NULL percentage for data quality analysis."""
    if total_count == 0:
        return 0.0
    return (null_count / total_count) * 100

def format_bytes(bytes_value: int) -> str:
    """Format bytes into human-readable format."""
    if bytes_value == 0:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(bytes_value)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"

def export_to_json(data: Dict[str, Any], filename: str) -> None:
    """Export data to JSON file with proper formatting."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Failed to export to {filename}: {e}")

def generate_timestamp() -> str:
    """Generate timestamp string for file naming."""
    return datetime.now().strftime('%Y%m%d_%H%M%S')

class ArchaeologyReport:
    """Base class for generating professional archaeology reports."""
    
    def __init__(self, environment: str):
        self.environment = environment
        self.timestamp = datetime.now().isoformat()
        self.data = {}
    
    def add_section(self, section_name: str, section_data: Any) -> None:
        """Add a section to the report."""
        self.data[section_name] = section_data
    
    def export(self, base_filename: str) -> str:
        """Export report to JSON file."""
        timestamp = generate_timestamp()
        filename = f"{base_filename}_{self.environment}_{timestamp}.json"
        
        report = {
            'metadata': {
                'environment': self.environment,
                'timestamp': self.timestamp,
                'report_type': base_filename
            },
            'data': self.data
        }
        
        export_to_json(report, filename)
        return filename
