#!/usr/bin/env python3
"""
Improved Visibility System Test
==============================
Comprehensive test suite for the GCN bot visibility system.
Tests functionality rather than specific visibility outcomes.

Author: Assistant
Created: 2025-06-09
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

# Add the project path to sys.path for imports
sys.path.append('/home/student1/projects/GCN/gcn_bot')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./test_plots/visibility_test_improved.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class VisibilitySystemTester:
    """Comprehensive test suite for the visibility system."""
    
    def __init__(self):
        """Initialize the tester."""
        self.test_results = []
        self.plots_created = []
        self.test_dir = "./test_plots"
        
        # Ensure test directory exists
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Initialize the visibility plotter
        try:
            from supy.supy.observer.visibility_plotter import VisibilityPlotter
            self.plotter = VisibilityPlotter(logger=logger)
            logger.info("✅ VisibilityPlotter initialized successfully")
        except ImportError as e:
            logger.error(f"❌ Failed to import VisibilityPlotter: {e}")
            sys.exit(1)
    
    def test_basic_functionality(self) -> bool:
        """Test basic functionality of the visibility system."""
        logger.info("\n" + "="*60)
        logger.info("TESTING BASIC FUNCTIONALITY")
        logger.info("="*60)
        
        success_count = 0
        total_tests = 0
        
        # Test 1: Valid coordinates - Southern sky (good for Chile)
        total_tests += 1
        try:
            ra, dec = 180.0, -30.0  # Southern sky target
            plot_path, visibility_info = self.plotter.create_visibility_plot(
                ra=ra, dec=dec, grb_name="FUNC_TEST_01", test_mode=True
            )
            
            # Check if visibility_info is returned and has expected keys
            required_keys = ['status', 'condition', 'message']
            if visibility_info and all(key in visibility_info for key in required_keys):
                logger.info(f"✅ Test 1 PASS: Valid coordinates processed successfully")
                logger.info(f"   Status: {visibility_info.get('status')}")
                logger.info(f"   Condition: {visibility_info.get('condition')}")
                success_count += 1
                
                if plot_path:
                    self.plots_created.append(plot_path)
            else:
                logger.error(f"❌ Test 1 FAIL: Invalid visibility_info structure")
                
        except Exception as e:
            logger.error(f"❌ Test 1 FAIL: Exception occurred: {e}")
        
        # Test 2: Invalid coordinates - should handle gracefully
        total_tests += 1
        try:
            ra, dec = 400.0, 100.0  # Invalid coordinates
            plot_path, visibility_info = self.plotter.create_visibility_plot(
                ra=ra, dec=dec, grb_name="FUNC_TEST_02", test_mode=True
            )
            
            # Should return None for plot_path and error status
            if plot_path is None and visibility_info and visibility_info.get('status') == 'error':
                logger.info(f"✅ Test 2 PASS: Invalid coordinates handled gracefully")
                success_count += 1
            else:
                logger.error(f"❌ Test 2 FAIL: Invalid coordinates not handled properly")
                
        except Exception as e:
            logger.error(f"❌ Test 2 FAIL: Unexpected exception: {e}")
        
        # Test 3: Message formatting
        total_tests += 1
        try:
            # Create a mock visibility_info for message formatting test
            mock_visibility = {
                'status': 'observable_now',
                'condition': 'Good Observing Conditions',
                'observable_end': datetime.now() + timedelta(hours=3),
                'current_altitude': 45.0,
                'current_moon_separation': 40.0,
                'remaining_hours': 3.0
            }
            
            message = self.plotter.format_visibility_message(mock_visibility)
            
            if message and isinstance(message, str) and len(message) > 50:
                logger.info(f"✅ Test 3 PASS: Message formatting works")
                logger.info(f"   Message length: {len(message)} chars")
                success_count += 1
            else:
                logger.error(f"❌ Test 3 FAIL: Message formatting failed")
                
        except Exception as e:
            logger.error(f"❌ Test 3 FAIL: Message formatting exception: {e}")
        
        success_rate = (success_count / total_tests) * 100
        logger.info(f"\nBasic Functionality: {success_count}/{total_tests} tests passed ({success_rate:.1f}%)")
        return success_count == total_tests
    
    def test_coordinate_validation(self) -> bool:
        """Test coordinate validation and edge cases."""
        logger.info("\n" + "="*60)
        logger.info("TESTING COORDINATE VALIDATION")
        logger.info("="*60)
        
        test_cases = [
            # (RA, DEC, expected_result, description)
            (0.0, 0.0, 'valid', "Celestial equator"),
            (360.0, 0.0, 'valid', "RA boundary (360°)"),
            (-10.0, 0.0, 'valid', "Negative RA (should be normalized)"),
            (370.0, 0.0, 'valid', "RA > 360° (should be normalized)"),
            (180.0, 90.0, 'valid', "North pole"),
            (180.0, -90.0, 'valid', "South pole"),
            (180.0, 100.0, 'invalid', "DEC > 90°"),
            (180.0, -100.0, 'invalid', "DEC < -90°"),
        ]
        
        success_count = 0
        total_tests = len(test_cases)
        
        for i, (ra, dec, expected, description) in enumerate(test_cases, 1):
            try:
                plot_path, visibility_info = self.plotter.create_visibility_plot(
                    ra=ra, dec=dec, grb_name=f"COORD_TEST_{i:02d}", test_mode=True
                )
                
                if expected == 'valid':
                    # Should succeed and return visibility_info
                    if visibility_info and 'status' in visibility_info:
                        logger.info(f"✅ Test {i} PASS: {description}")
                        success_count += 1
                        if plot_path:
                            self.plots_created.append(plot_path)
                    else:
                        logger.error(f"❌ Test {i} FAIL: {description} - Expected valid, got invalid")
                
                elif expected == 'invalid':
                    # Should fail gracefully
                    if visibility_info and visibility_info.get('status') == 'error':
                        logger.info(f"✅ Test {i} PASS: {description} - Properly rejected")
                        success_count += 1
                    else:
                        logger.error(f"❌ Test {i} FAIL: {description} - Should have been rejected")
                        
            except Exception as e:
                if expected == 'invalid':
                    logger.info(f"✅ Test {i} PASS: {description} - Exception properly raised")
                    success_count += 1
                else:
                    logger.error(f"❌ Test {i} FAIL: {description} - Unexpected exception: {e}")
        
        success_rate = (success_count / total_tests) * 100
        logger.info(f"\nCoordinate Validation: {success_count}/{total_tests} tests passed ({success_rate:.1f}%)")
        return success_count == total_tests
    
    def test_visibility_analysis(self) -> bool:
        """Test visibility analysis for different scenarios."""
        logger.info("\n" + "="*60)
        logger.info("TESTING VISIBILITY ANALYSIS")
        logger.info("="*60)
        
        # Test coordinates that are realistic for Chile observatory
        test_cases = [
            # (RA, DEC, description)
            (180.0, -30.0, "Southern sky target"),
            (90.0, -20.0, "Eastern sky target"),
            (270.0, -10.0, "Western sky target"),
            (0.0, -40.0, "High declination southern target"),
            (180.0, -80.0, "Circumpolar target"),
        ]
        
        success_count = 0
        total_tests = len(test_cases)
        
        for i, (ra, dec, description) in enumerate(test_cases, 1):
            try:
                plot_path, visibility_info = self.plotter.create_visibility_plot(
                    ra=ra, dec=dec, grb_name=f"VIS_TEST_{i:02d}", test_mode=True
                )
                
                # Check that visibility analysis produces valid results
                if visibility_info and isinstance(visibility_info, dict):
                    status = visibility_info.get('status')
                    condition = visibility_info.get('condition')
                    
                    # Valid statuses
                    valid_statuses = ['observable_now', 'observable_later', 'observable_tomorrow', 'not_observable']
                    
                    if status in valid_statuses and condition:
                        logger.info(f"✅ Test {i} PASS: {description}")
                        logger.info(f"   Status: {status}, Condition: {condition}")
                        success_count += 1
                        
                        if plot_path:
                            self.plots_created.append(plot_path)
                    else:
                        logger.error(f"❌ Test {i} FAIL: {description} - Invalid status or condition")
                else:
                    logger.error(f"❌ Test {i} FAIL: {description} - No valid visibility_info")
                    
            except Exception as e:
                logger.error(f"❌ Test {i} FAIL: {description} - Exception: {e}")
        
        success_rate = (success_count / total_tests) * 100
        logger.info(f"\nVisibility Analysis: {success_count}/{total_tests} tests passed ({success_rate:.1f}%)")
        return success_count == total_tests
    
    def test_message_formatting(self) -> bool:
        """Test message formatting for different visibility scenarios."""
        logger.info("\n" + "="*60)
        logger.info("TESTING MESSAGE FORMATTING")
        logger.info("="*60)
        
        # Test different visibility scenarios
        test_scenarios = [
            {
                'name': 'Currently Observable',
                'visibility_info': {
                    'status': 'observable_now',
                    'condition': 'Excellent Observing Conditions',
                    'observable_end': datetime.now() + timedelta(hours=4),
                    'current_altitude': 65.0,
                    'current_moon_separation': 45.0,
                    'remaining_hours': 4.2
                },
                'expected_keywords': ['observable', 'currently', 'excellent', 'altitude']
            },
            {
                'name': 'Observable Later',
                'visibility_info': {
                    'status': 'observable_later',
                    'condition': 'Observable in a Few Hours',
                    'observable_start': datetime.now() + timedelta(hours=2),
                    'observable_end': datetime.now() + timedelta(hours=6),
                    'hours_until_observable': 2.3,
                    'observable_hours': 4.0,
                    'best_time': datetime.now() + timedelta(hours=4)
                },
                'expected_keywords': ['observable', 'later', 'hours', 'window']
            },
            {
                'name': 'Observable Tomorrow',
                'visibility_info': {
                    'status': 'observable_tomorrow',
                    'condition': 'Likely Observable Tomorrow',
                    'reason': 'Target reaches maximum altitude close to minimum required'
                },
                'expected_keywords': ['observable', 'tomorrow', 'likely']
            },
            {
                'name': 'Not Observable',
                'visibility_info': {
                    'status': 'not_observable',
                    'condition': 'Below Minimum Altitude',
                    'reason': 'Target maximum altitude (25.0°) below minimum required (30°)'
                },
                'expected_keywords': ['not', 'observable', 'below', 'altitude']
            }
        ]
        
        success_count = 0
        total_tests = len(test_scenarios)
        
        for i, scenario in enumerate(test_scenarios, 1):
            try:
                message = self.plotter.format_visibility_message(scenario['visibility_info'])
                
                # Debug output
                logger.debug(f"Test {i} Debug Info:")
                logger.debug(f"  Input status: {scenario['visibility_info']['status']}")
                logger.debug(f"  Message generated: {message}")
                logger.debug(f"  Message length: {len(message) if message else 0}")
                
                # Check if message was generated
                if not message or not isinstance(message, str):
                    logger.error(f"❌ Test {i} FAIL: {scenario['name']} - No message generated or not a string")
                    continue
                
                # Check minimum message length
                if len(message) < 20:
                    logger.error(f"❌ Test {i} FAIL: {scenario['name']} - Message too short ({len(message)} chars)")
                    logger.error(f"   Message: '{message}'")
                    continue
                
                # Check for expected keywords (more flexible approach)
                message_lower = message.lower()
                keywords_found = 0
                total_keywords = len(scenario['expected_keywords'])
                
                for keyword in scenario['expected_keywords']:
                    if keyword.lower() in message_lower:
                        keywords_found += 1
                
                # Require at least 50% of keywords to be present
                if keywords_found >= (total_keywords * 0.5):
                    logger.info(f"✅ Test {i} PASS: {scenario['name']} message formatting")
                    logger.info(f"   Message length: {len(message)} chars")
                    logger.info(f"   Keywords found: {keywords_found}/{total_keywords}")
                    success_count += 1
                else:
                    logger.error(f"❌ Test {i} FAIL: {scenario['name']} - Insufficient keywords")
                    logger.error(f"   Keywords found: {keywords_found}/{total_keywords}")
                    logger.error(f"   Expected: {scenario['expected_keywords']}")
                    logger.error(f"   Message: '{message[:100]}...'")
                    
            except Exception as e:
                logger.error(f"❌ Test {i} FAIL: {scenario['name']} - Exception: {e}")
                import traceback
                logger.error(f"   Traceback: {traceback.format_exc()}")
        
        success_rate = (success_count / total_tests) * 100
        logger.info(f"\nMessage Formatting: {success_count}/{total_tests} tests passed ({success_rate:.1f}%)")
        return success_count == total_tests
    
    def test_timezone_conversion(self) -> bool:
        """Test timezone conversion functionality."""
        logger.info("\n" + "="*60)
        logger.info("TESTING TIMEZONE CONVERSION")
        logger.info("="*60)
        
        success_count = 0
        total_tests = 2
        
        # Test 1: UTC to CLT/KST conversion
        try:
            import pytz
            utc_time = datetime.now(pytz.utc)
            chile_time, korea_time = self.plotter._convert_time_to_clt_kst(utc_time)
            
            if (chile_time and korea_time and 
                chile_time.tzinfo and korea_time.tzinfo):
                logger.info(f"✅ Test 1 PASS: Timezone conversion works")
                logger.info(f"   UTC: {utc_time.strftime('%H:%M')}")
                logger.info(f"   Chile: {chile_time.strftime('%H:%M %Z')}")
                logger.info(f"   Korea: {korea_time.strftime('%H:%M %Z')}")
                success_count += 1
            else:
                logger.error(f"❌ Test 1 FAIL: Timezone conversion failed")
                
        except Exception as e:
            logger.error(f"❌ Test 1 FAIL: Timezone conversion exception: {e}")
        
        # Test 2: Time formatting
        try:
            utc_time = datetime.now(pytz.utc)
            formatted_time = self.plotter._format_time_clt_kst(utc_time)
            
            if (formatted_time and 'CLT' in formatted_time and 'KST' in formatted_time):
                logger.info(f"✅ Test 2 PASS: Time formatting works")
                logger.info(f"   Formatted: {formatted_time}")
                success_count += 1
            else:
                logger.error(f"❌ Test 2 FAIL: Time formatting failed")
                
        except Exception as e:
            logger.error(f"❌ Test 2 FAIL: Time formatting exception: {e}")
        
        success_rate = (success_count / total_tests) * 100
        logger.info(f"\nTimezone Conversion: {success_count}/{total_tests} tests passed ({success_rate:.1f}%)")
        return success_count == total_tests
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all test suites and return results."""
        logger.info("🚀 Starting Comprehensive Visibility System Tests...")
        
        start_time = time.time()
        
        test_results = {
            'basic_functionality': self.test_basic_functionality(),
            'coordinate_validation': self.test_coordinate_validation(),
            'visibility_analysis': self.test_visibility_analysis(),
            'message_formatting': self.test_message_formatting(),
            'timezone_conversion': self.test_timezone_conversion()
        }
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("FINAL TEST SUMMARY")
        logger.info("="*60)
        
        passed_tests = sum(test_results.values())
        total_tests = len(test_results)
        
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"{test_name.replace('_', ' ').title()}: {status}")
        
        success_rate = (passed_tests / total_tests) * 100
        logger.info(f"\nOverall Results: {passed_tests}/{total_tests} test suites passed ({success_rate:.1f}%)")
        
        # Plot summary
        if self.plots_created:
            logger.info(f"\n📁 {len(self.plots_created)} test plots created in {self.test_dir}")
            for plot_path in self.plots_created[-5:]:  # Show last 5
                plot_name = os.path.basename(plot_path)
                logger.info(f"   - {plot_name}")
            if len(self.plots_created) > 5:
                logger.info(f"   ... and {len(self.plots_created) - 5} more")
        
        elapsed_time = time.time() - start_time
        logger.info(f"\n⏱️ Tests completed in {elapsed_time:.1f} seconds")
        
        if passed_tests == total_tests:
            logger.info("🎉 All tests PASSED! Visibility system is working correctly.")
        else:
            logger.warning("⚠️ Some tests FAILED. Please review the issues above.")
        
        return test_results

def main():
    """Main function to run the improved visibility tests."""
    tester = VisibilitySystemTester()
    results = tester.run_all_tests()
    
    # Exit with appropriate code
    if all(results.values()):
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Some tests failed

if __name__ == "__main__":
    main()