#!/usr/bin/env python3
"""
VALD Data Upload Automation Script

This script runs all data processors sequentially.
Designed to be run automatically (e.g., nightly via cron or Task Scheduler).
"""

import os
import sys
import logging
import traceback
from datetime import datetime
from pathlib import Path

# Add the Scripts directory to the Python path
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(scripts_dir))

# Import the processor modules
try:
    from enhanced_cmj_processor import process_cmj_data
    from process_hj import process_hj_data
    from process_imtp import process_imtp_data
    from process_ppu import process_ppu_data
    from newcompositescore import calculate_composite_scores
    from VALDapiHelpers import VALDAPIHelper
except ImportError as e:
    print(f"Error importing processor modules: {e}")
    print("Make sure all required scripts are in the Scripts/ directory")
    sys.exit(1)

# Set up logging
def setup_logging():
    """Set up logging configuration for the automation script."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"vald_automation_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def run_processor(processor_name, processor_func, logger):
    """Run a single processor and handle any errors."""
    logger.info(f"Starting {processor_name}...")
    
    try:
        start_time = datetime.now()
        result = processor_func()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"{processor_name} completed successfully in {duration:.2f} seconds")
        return True, result
        
    except Exception as e:
        logger.error(f"{processor_name} failed: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, str(e)

def main():
    """Main automation function that runs all processors."""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("VALD Data Upload Automation Started")
    logger.info(f"Start time: {datetime.now()}")
    logger.info("=" * 60)
    
    # Define the processors to run
    processors = [
        ("CMJ Processor", process_cmj_data),
        ("HJ Processor", process_hj_data),
        ("IMTP Processor", process_imtp_data),
        ("PPU Processor", process_ppu_data),
        ("Composite Score Calculator", calculate_composite_scores),
    ]
    
    # Track results
    results = []
    successful_processors = 0
    failed_processors = 0
    
    # Run each processor
    for processor_name, processor_func in processors:
        success, result = run_processor(processor_name, processor_func, logger)
        results.append((processor_name, success, result))
        
        if success:
            successful_processors += 1
        else:
            failed_processors += 1
    
    # Summary
    logger.info("=" * 60)
    logger.info("AUTOMATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total processors: {len(processors)}")
    logger.info(f"Successful: {successful_processors}")
    logger.info(f"Failed: {failed_processors}")
    logger.info(f"End time: {datetime.now()}")
    
    # Log individual results
    for processor_name, success, result in results:
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{status}: {processor_name}")
        if not success:
            logger.error(f"  Error: {result}")
    
    logger.info("=" * 60)
    
    # Exit with appropriate code
    if failed_processors > 0:
        logger.warning(f"Automation completed with {failed_processors} failures")
        sys.exit(1)
    else:
        logger.info("All processors completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
