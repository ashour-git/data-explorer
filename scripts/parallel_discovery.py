#!/usr/bin/env python3
"""
Parallel Multi-Environment Discovery Engine
Executes DataArchaeologist across staging, production, and backup environments concurrently
Author: Data Archaeologist Team
Version: 2.0
"""

import json
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback
import os
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from data_archaeologist.core.database_connection import DatabaseConnection
    from data_archaeologist.archaeologist import DataArchaeologist
    from data_archaeologist.core.logging_config import setup_logging
except ImportError as e:
    print(f"CRITICAL: Import failed - {e}")
    print("Please ensure all dependencies are installed and PYTHONPATH is configured")
    sys.exit(1)
    print(f"CRITICAL: Import failed - {e}")

class ParallelDiscoveryEngine:
    """Orchestrates parallel discovery across multiple database environments."""
    
    def __init__(self, config_file: str = 'config.json'):
        """Initialize the parallel discovery engine."""
        self.config_file = config_file
        self.config = self._load_config()
        self.logger = setup_logging()
        
        # Analysis settings
        self.analysis_settings = self.config.get('analysis_settings', {})
        self.max_workers = self.analysis_settings.get('max_workers', 3)
        self.target_environments = ['staging', 'production', 'backup']
        
        # Results storage
        self.results = {}
        self.timings = {}
        self.errors = {}
        
    def _load_config(self) -> Dict:
        """Load configuration from file."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            # Validate required environments
            environments = config.get('environments', {})
            missing_envs = [env for env in ['staging', 'production', 'backup'] 
                          if env not in environments]
            
            if missing_envs:
                raise ValueError(f"Missing required environments: {missing_envs}")
                
            return config
            
        except Exception as e:
            print(f"CRITICAL: Configuration loading failed - {e}")
            sys.exit(1)
    
    def _run_environment_discovery(self, environment: str) -> Tuple[str, Dict, float, Optional[Exception]]:
        """Run complete discovery for a single environment."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting discovery for environment: {environment}")
            
            # Initialize DataArchaeologist for this environment
            db_connection = DatabaseConnection(self.config_file)
            archaeologist = DataArchaeologist(
                db_connection=db_connection,
                environment=environment
            )
            
            # Run complete discovery with parallel execution enabled
            results = archaeologist.run_complete_discovery(
                environment=environment,
                parallel_execution=self.analysis_settings.get('parallel_envs', True)
            )
            
            duration = time.time() - start_time
            self.logger.info(f"Completed discovery for {environment} in {duration:.2f}s")
            
            return environment, results, duration, None
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Discovery failed for {environment}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(traceback.format_exc())
            
            return environment, {}, duration, e
    
    def run_parallel_discovery(self) -> Dict:
        """Execute discovery across all environments in parallel."""
        self.logger.info("Starting parallel discovery across environments")
        start_time = time.time()
        
        # Use ThreadPoolExecutor for I/O-bound database operations
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit discovery tasks for all environments
            future_to_env = {
                executor.submit(self._run_environment_discovery, env): env
                for env in self.target_environments
                if env in self.config['environments']
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_env):
                env_name = future_to_env[future]
                
                try:
                    environment, results, duration, error = future.result()
                    
                    self.timings[environment] = duration
                    
                    if error:
                        self.errors[environment] = {
                            'error': str(error),
                            'type': type(error).__name__
                        }
                        self.results[environment] = {
                            'status': 'failed',
                            'error': str(error)
                        }
                    else:
                        self.results[environment] = {
                            'status': 'completed',
                            'data': results,
                            'duration_seconds': duration
                        }
                        
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {env_name}: {e}")
                    self.errors[env_name] = {
                        'error': str(e),
                        'type': type(e).__name__
                    }
        
        total_duration = time.time() - start_time
        
        # Compile summary report
        summary = self._generate_summary_report(total_duration)
        
        # Export results
        output_file = self._export_results(summary)
        
        self.logger.info(f"Parallel discovery completed in {total_duration:.2f}s")
        self.logger.info(f"Results exported to: {output_file}")
        
        return summary
    
    def _generate_summary_report(self, total_duration: float) -> Dict:
        """Generate comprehensive summary report."""
        successful_envs = [env for env in self.results if self.results[env]['status'] == 'completed']
        failed_envs = [env for env in self.results if self.results[env]['status'] == 'failed']
        
        summary = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_duration_seconds': total_duration,
                'target_environments': self.target_environments,
                'successful_environments': successful_envs,
                'failed_environments': failed_envs,
                'success_rate': len(successful_envs) / len(self.target_environments) * 100
            },
            'environment_results': self.results,
            'performance_metrics': {
                'environment_durations': self.timings,
                'parallel_efficiency': total_duration / max(self.timings.values()) if self.timings else 0,
                'total_tables_analyzed': sum(
                    len(self.results[env]['data'].get('layer1_physical', {}).get('tables', []))
                    for env in successful_envs
                    if 'data' in self.results[env]
                )
            },
            'errors': self.errors
        }
        
        return summary
    
    def _export_results(self, summary: Dict) -> str:
        """Export results to timestamped JSON file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"combined_discovery_{timestamp}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            return output_file
            
        except Exception as e:
            self.logger.error(f"Failed to export results: {e}")
            raise
    
    def print_summary(self, summary: Dict) -> None:
        """Print human-readable summary to console."""
        metadata = summary['metadata']
        performance = summary['performance_metrics']
        
        print("\n" + "="*80)
        print("PARALLEL DISCOVERY SUMMARY")
        print("="*80)
        
        print(f"Timestamp: {metadata['timestamp']}")
        print(f"Total Duration: {metadata['total_duration_seconds']:.2f} seconds")
        print(f"Success Rate: {metadata['success_rate']:.1f}%")
        print(f"Total Tables Analyzed: {performance['total_tables_analyzed']}")
        print(f"Parallel Efficiency: {performance['parallel_efficiency']:.2f}x")
        
        print(f"\nEnvironment Results:")
        for env in metadata['target_environments']:
            if env in summary['environment_results']:
                result = summary['environment_results'][env]
                status = result['status']
                duration = self.timings.get(env, 0)
                
                print(f"  {env.title()}: {status.upper()} ({duration:.2f}s)")
                
                if status == 'failed':
                    print(f"    Error: {result.get('error', 'Unknown error')}")
            else:
                print(f"  {env.title()}: NOT PROCESSED")
        
        if summary['errors']:
            print(f"\nErrors Encountered:")
            for env, error_info in summary['errors'].items():
                print(f"  {env}: {error_info['type']} - {error_info['error']}")
        
        print("="*80)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _discover_environment(self, environment: str) -> Tuple[str, Dict, float, Optional[str]]:
        """
        Run complete discovery for a single environment.
        
        Returns:
            Tuple of (environment, results, duration, error_message)
        """
        start_time = time.time()
        error_message = None
        results = {}
        
        try:
            self.logger.info(f"Starting discovery for environment: {environment}")
            
            # Validate environment exists in config
            if environment not in self.config['environments']:
                raise ValueError(f"Environment '{environment}' not found in configuration")
            
            # Initialize DataArchaeologist for this environment
            archaeologist = DataArchaeologist(self.config_file)
            
            # Run complete discovery with parallel execution enabled
            results = archaeologist.run_complete_discovery(
                environment, 
                parallel_execution=self.config['analysis_settings'].get('parallel_envs', True)
            )
            
            # Add metadata
            results['metadata'] = {
                'environment': environment,
                'discovery_timestamp': datetime.now().isoformat(),
                'config_file': self.config_file,
                'parallel_execution': True
            }
            
            duration = time.time() - start_time
            self.logger.info(f"Completed discovery for {environment} in {duration:.2f} seconds")
            
        except Exception as e:
            duration = time.time() - start_time
            error_message = str(e)
            self.logger.error(f"Discovery failed for {environment}: {error_message}")
            
            # Return minimal results with error info
            results = {
                'error': error_message,
                'metadata': {
                    'environment': environment,
                    'discovery_timestamp': datetime.now().isoformat(),
                    'failed': True
                }
            }
        
        return environment, results, duration, error_message
    
    def run_parallel_discovery(self) -> Dict:
        """
        Execute parallel discovery across all target environments.
        
        Returns:
            Combined results dictionary with per-environment data and metadata
        """
        self.logger.info("Starting parallel discovery across environments")
        self.logger.info(f"Target environments: {', '.join(self.target_environments)}")
        
        start_time = time.time()
        combined_results = {
            'discovery_metadata': {
                'start_time': datetime.now().isoformat(),
                'target_environments': self.target_environments,
                'parallel_execution': True,
                'max_workers': self.config['analysis_settings'].get('max_workers', 3)
            },
            'environments': {},
            'summary': {
                'total_environments': len(self.target_environments),
                'successful_environments': 0,
                'failed_environments': 0,
                'total_duration': 0.0,
                'per_environment_durations': {}
            }
        }
        
        # Execute parallel discovery
        max_workers = self.config['analysis_settings'].get('max_workers', 3)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all discovery tasks
            future_to_env = {
                executor.submit(self._discover_environment, env): env 
                for env in self.target_environments
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_env):
                env = future_to_env[future]
                
                try:
                    environment, results, duration, error = future.result()
                    
                    # Store results
                    combined_results['environments'][environment] = results
                    combined_results['summary']['per_environment_durations'][environment] = duration
                    
                    if error:
                        combined_results['summary']['failed_environments'] += 1
                        self.logger.warning(f"Environment {environment} failed: {error}")
                    else:
                        combined_results['summary']['successful_environments'] += 1
                        self.logger.info(f"Environment {environment} completed successfully")
                        
                except Exception as e:
                    self.logger.error(f"Unexpected error processing {env}: {e}")
                    combined_results['summary']['failed_environments'] += 1
                    combined_results['environments'][env] = {
                        'error': f"Unexpected error: {e}",
                        'metadata': {'environment': env, 'failed': True}
                    }
        
        # Finalize summary
        total_duration = time.time() - start_time
        combined_results['summary']['total_duration'] = total_duration
        combined_results['discovery_metadata']['end_time'] = datetime.now().isoformat()
        combined_results['discovery_metadata']['total_duration'] = total_duration
        
        self.logger.info(f"Parallel discovery completed in {total_duration:.2f} seconds")
        self.logger.info(f"Successful: {combined_results['summary']['successful_environments']}, "
                        f"Failed: {combined_results['summary']['failed_environments']}")
        
        return combined_results
    
    def export_results(self, results: Dict, output_dir: str = 'outputs') -> str:
        """
        Export combined results to timestamped JSON file.
        
        Returns:
            Path to exported file
        """
        # Ensure output directory exists
        Path(output_dir).mkdir(exist_ok=True)
        
        # Generate timestamped filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"combined_discovery_{timestamp}.json"
        filepath = Path(output_dir) / filename
        
        # Write results
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Results exported to: {filepath}")
        return str(filepath)
    
    def run(self, export: bool = True) -> Tuple[Dict, Optional[str]]:
        """
        Execute complete parallel discovery workflow.
        
        Returns:
            Tuple of (results, export_path)
        """
        try:
            # Run parallel discovery
            results = self.run_parallel_discovery()
            
            # Export results if requested
            export_path = None
            if export:
                export_path = self.export_results(results)
            
            return results, export_path
            
        except Exception as e:
            self.logger.error(f"Parallel discovery workflow failed: {e}")
            raise


def main():
    """Main entry point for parallel discovery."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Parallel Multi-Environment Database Discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--config', 
        default='config.json',
        help='Configuration file path (default: config.json)'
    )
    parser.add_argument(
        '--no-export',
        action='store_true',
        help='Skip exporting results to file'
    )
    parser.add_argument(
        '--output-dir',
        default='outputs',
        help='Output directory for results (default: outputs)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        print("="*80)
        print("PARALLEL MULTI-ENVIRONMENT DISCOVERY ENGINE")
        print("="*80)
        print(f"Configuration: {args.config}")
        print(f"Target Environments: staging, production, backup")
        print(f"Parallel Execution: Enabled")
        print("="*80)
        
        # Initialize and run discovery engine
        engine = ParallelDiscoveryEngine(args.config)
        results, export_path = engine.run(export=not args.no_export)
        
        # Display summary
        print("\n" + "="*80)
        print("DISCOVERY SUMMARY")
        print("="*80)
        summary = results['summary']
        print(f"Total Environments: {summary['total_environments']}")
        print(f"Successful: {summary['successful_environments']}")
        print(f"Failed: {summary['failed_environments']}")
        print(f"Total Duration: {summary['total_duration']:.2f} seconds")
        
        print("\nPer-Environment Durations:")
        for env, duration in summary['per_environment_durations'].items():
            status = "SUCCESS" if env in results['environments'] and 'error' not in results['environments'][env] else "FAILED"
            print(f"  {env}: {duration:.2f}s ({status})")
        
        if export_path:
            print(f"\nResults exported to: {export_path}")
        
        print("="*80)
        
        # Exit with appropriate code
        if summary['failed_environments'] > 0:
            print(f"WARNING: {summary['failed_environments']} environment(s) failed")
            sys.exit(1)

def main():
    """Main entry point for parallel discovery."""
    # Support non-interactive mode
    non_interactive = os.getenv('EXPLORER_NON_INTERACTIVE', '').lower() in ('1', 'true', 'yes')
    
    if not non_interactive:
        print("Data Archaeologist - Parallel Discovery Engine")
        print("=" * 50)
        input("Press Enter to start parallel discovery across all environments...")
    
    try:
        engine = ParallelDiscoveryEngine()
        summary = engine.run_parallel_discovery()
        engine.print_summary(summary)
        
        if not non_interactive:
            input("\nPress Enter to exit...")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
