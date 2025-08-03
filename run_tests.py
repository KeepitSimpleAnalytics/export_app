#!/usr/bin/env python3
"""
Test runner for the Air-gapped Data Utility (ADU)

Usage:
    python run_tests.py              # Run all tests
    python run_tests.py worker       # Run only worker tests
    python run_tests.py app          # Run only app tests  
    python run_tests.py database     # Run only database tests
"""

import unittest
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def run_tests(test_module=None):
    """Run tests for the specified module or all tests if none specified"""
    
    if test_module:
        # Run specific test module
        suite = unittest.TestLoader().loadTestsFromName(f'tests.test_{test_module}')
    else:
        # Run all tests
        loader = unittest.TestLoader()
        suite = loader.discover('tests', pattern='test_*.py')
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    print(f"{'='*50}")
    
    # Return exit code based on results
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    test_module = sys.argv[1] if len(sys.argv) > 1 else None
    
    if test_module and test_module not in ['worker', 'app', 'database', 'simple']:
        print(f"Error: Unknown test module '{test_module}'")
        print("Available modules: worker, app, database, simple")
        sys.exit(1)
    
    exit_code = run_tests(test_module)
    sys.exit(exit_code)