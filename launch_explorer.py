#!/usr/bin/env python3
"""
Data Archaeologist Launcher
Professional database exploration interface
"""

import sys
import os
from pathlib import Path

def main():
    """Launch the Data Archaeologist interface."""
    print("🏛️  Data Archaeologist - Professional Database Explorer")
    print("=" * 60)
    
    try:
        # Import and launch
        from scripts.interactive_workflow import DatabaseExplorer
        
        explorer = DatabaseExplorer()
        explorer.run()
        
    except KeyboardInterrupt:
        print("\n👋 Session ended by user")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please ensure all dependencies are installed")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
