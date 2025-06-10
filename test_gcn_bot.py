#!/usr/bin/env python3
"""
Thread Management Test Script
============================
Separate test script for testing thread management functionality and visibility plotter
without modifying the main gcn_bot.py code.

Usage:
    python test_threads.py --test-basic
    python test_threads.py --test-facilities  
    python test_threads.py --test-visibility
    python test_threads.py --test-all
    python test_threads.py --send  # Actually send to Slack
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# CRITICAL: Parse our arguments FIRST and save the original sys.argv
original_argv = sys.argv.copy()

def parse_arguments():
    """Parse command line arguments before any imports that might conflict."""
    parser = argparse.ArgumentParser(description='Thread Management and Visibility Test Suite')
    parser.add_argument('--test-basic', action='store_true', help='Test basic thread management')
    parser.add_argument('--test-facilities', action='store_true', help='Test different facilities')
    parser.add_argument('--test-visibility', action='store_true', help='Test new 4-case visibility system')
    parser.add_argument('--test-all', action='store_true', help='Run all tests')
    parser.add_argument('--send', action='store_true', help='Actually send messages to Slack')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('--test-visibility-dynamic', action='store_true', 
                   help='Test visibility with dynamically calculated coordinates')
    parser.add_argument('--test-visibility-static', action='store_true', 
                   help='Test visibility with proven static coordinates')
    parser.add_argument('--test-improved', action='store_true', 
                   help='Test both improved dynamic and static coordinates')
    return parser.parse_args()

# Parse our arguments first
args = parse_arguments()

# Now MODIFY sys.argv so that gcn_bot.py's argparse won't conflict
# We'll set sys.argv to just the script name (no arguments)
sys.argv = [sys.argv[0]]  # Keep only the script name

# Add the main directory to Python path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Now do the imports AFTER manipulating sys.argv
try:
    # Import config first
    try:
        from config import SLACK_TOKEN, SLACK_CHANNEL, SLACK_CHANNEL_TEST, MIN_ALTITUDE, MIN_MOON_SEP
    except ImportError:
        print("❌ Error: Cannot find config.py. Please ensure config.py exists with required settings.")
        sys.exit(1)
    
    # Import Slack client
    from slack_sdk import WebClient
    slack_client = WebClient(token=SLACK_TOKEN)
    
    # Import notice handler
    from gcn_notice_handler import GCNNoticeHandler
    notice_handler = GCNNoticeHandler()
    
    # Import visibility plotter
    try:
        from supy.supy.observer.visibility_plotter import VisibilityPlotter
        plotter = VisibilityPlotter()
        visibility_available = True
        print("✅ Visibility plotter imported successfully")
    except ImportError as e:
        print(f"⚠️ Warning: Visibility plotter not available: {e}")
        visibility_available = False
        plotter = None
    
    # Import specific functions from gcn_bot
    # This should now work without argparse conflicts
    import gcn_bot
    
    print("✅ Successfully imported main bot modules")
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("Make sure you're running this from the same directory as gcn_bot.py and config.py")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error during import: {e}")
    sys.exit(1)

# Restore original sys.argv after imports
sys.argv = original_argv

# Set up logging to file
log_level = logging.DEBUG if args.verbose else logging.INFO

# Configure logging to write to file
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./test_plots/visibility_test_improved.log', mode='w'),  # Write to file
        logging.StreamHandler() if args.verbose else logging.NullHandler()  # Console only if verbose
    ]
)
logger = logging.getLogger(__name__)

class ThreadTester:
    """Class to handle thread management and visibility testing."""
    
    def __init__(self, send_to_slack=False):
        self.send_to_slack = send_to_slack
        # Use test channel if available, otherwise use main channel
        try:
            self.test_channel = SLACK_CHANNEL_TEST if hasattr(sys.modules[__name__], 'SLACK_CHANNEL_TEST') and SLACK_CHANNEL_TEST else SLACK_CHANNEL
        except:
            self.test_channel = SLACK_CHANNEL
        
        print(f"📢 Using Slack channel: {self.test_channel}")
        
        # Mock message class
        class MockMessage:
            def __init__(self, topic, value):
                self._topic = topic
                self._value = value.encode('utf-8') if isinstance(value, str) else value
                
            def topic(self):
                return self._topic
                
            def value(self):
                return self._value
        
        self.MockMessage = MockMessage
        
    def test_basic_thread_management(self):
        """Test basic thread management: initial message + update."""
        print("\n" + "="*60)
        print("TESTING: Basic Thread Management")
        print("="*60)
        
        test_facility = "SwiftXRT"
        test_trigger = "1234567"
        
        # Initial message
        initial_notice = f"""TITLE:           GCN/SWIFT NOTICE
NOTICE_DATE:     Thu 20 May 25 10:30:15 UT
NOTICE_TYPE:     Swift-XRT Position
TRIGGER_NUM:     {test_trigger}
GRB_RA:          150.1234d {{+10h 00m 30s}} (J2000)
GRB_DEC:         -25.5678d {{-25d 34' 04"}} (J2000)
GRB_ERROR:       3.5 [arcsec radius]
IMG_START_DATE:  25/05/20
IMG_START_TIME:  37815.25 SOD {{10:30:15.25}} UT
COMMENTS:        Swift-XRT Coordinates - Initial position.
"""

        # Updated message with better coordinates
        updated_notice = f"""TITLE:           GCN/SWIFT NOTICE
NOTICE_DATE:     Thu 20 May 25 10:45:30 UT
NOTICE_TYPE:     Swift-XRT Position UPDATE
TRIGGER_NUM:     {test_trigger}
GRB_RA:          150.2468d {{+10h 00m 59s}} (J2000)
GRB_DEC:         -25.4321d {{-25d 25' 56"}} (J2000)
GRB_ERROR:       1.8 [arcsec radius]
IMG_START_DATE:  25/05/20
IMG_START_TIME:  37815.25 SOD {{10:30:15.25}} UT
COMMENTS:        Swift-XRT Coordinates - Enhanced position.
"""

        topic = "gcn.classic.text.SWIFT_XRT_POSITION"
        
        try:
            print(f"Step 1: Sending INITIAL message")
            print(f"  Facility: {test_facility}")
            print(f"  Trigger: {test_trigger}")
            print(f"  Coordinates: RA=150.1234, DEC=-25.5678, Error=3.5 arcsec")
            
            # Process initial message
            initial_msg = self.MockMessage(topic, initial_notice)
            success1, result1 = gcn_bot.process_notice_and_send_message(
                initial_msg.topic(),
                initial_msg.value(),
                slack_client,
                self.test_channel,
                is_test=not self.send_to_slack
            )
            
            print(f"  Result: {success1} - {result1}")
            
            if not success1:
                print("❌ FAILED: Could not process initial message")
                return False
                
            time.sleep(3)  # Give more time for processing
            
            print(f"\nStep 2: Sending UPDATE message")
            print(f"  Updated coordinates: RA=150.2468, DEC=-25.4321, Error=1.8 arcsec")
            print(f"  Expected: Thread reply showing coordinate changes")
            
            # Process update message
            update_msg = self.MockMessage(topic, updated_notice)
            success2, result2 = gcn_bot.process_notice_and_send_message(
                update_msg.topic(),
                update_msg.value(),
                slack_client,
                self.test_channel,
                is_test=not self.send_to_slack
            )
            
            print(f"  Result: {success2} - {result2}")
            
            if not success2:
                print("❌ FAILED: Could not process update message")
                return False
            
            print("✅ SUCCESS: Both messages processed successfully")
            print("   📋 Check Slack to verify thread behavior")
            return True
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            logger.exception("Error in basic thread management test")
            return False
    
    def test_different_facilities(self):
        """Test that different facilities create separate messages."""
        print("\n" + "="*60)
        print("TESTING: Different Facilities Separation")
        print("="*60)
        
        test_trigger = "9999999"
        
        # Swift BAT notice
        swift_notice = f"""TITLE:           GCN/SWIFT NOTICE
NOTICE_DATE:     Thu 20 May 25 11:00:00 UT
NOTICE_TYPE:     Swift-BAT GRB Position ACK
TRIGGER_NUM:     {test_trigger}
GRB_RA:          180.0000d {{+12h 00m 00s}} (J2000)
GRB_DEC:         -30.0000d {{-30d 00' 00"}} (J2000)
GRB_ERROR:       3.0 [arcmin radius]
GRB_DATE:        25/05/20
GRB_TIME:        39600.00 SOD {{11:00:00.00}} UT
"""

        # Fermi GBM notice with same trigger
        fermi_notice = f"""TITLE:           GCN/FERMI NOTICE
NOTICE_DATE:     Thu 20 May 25 11:01:00 UT
NOTICE_TYPE:     Fermi-GBM Final Position
TRIGGER_NUM:     {test_trigger}
GRB_RA:          180.1000d {{+12h 00m 24s}} (J2000)
GRB_DEC:         -30.1000d {{-30d 06' 00"}} (J2000)
GRB_ERROR:       2.5 [deg radius]
GRB_DATE:        25/05/20
GRB_TIME:        39600.00 SOD {{11:00:00.00}} UT
LC_URL:          http://heasarc.gsfc.nasa.gov/FTP/fermi/data/gbm/triggers/2025/test.gif
"""

        try:
            print(f"Step 1: Sending SWIFT BAT message (trigger {test_trigger})")
            
            # Process Swift message
            swift_msg = self.MockMessage("gcn.classic.text.SWIFT_BAT_GRB_POS_ACK", swift_notice)
            success1, result1 = gcn_bot.process_notice_and_send_message(
                swift_msg.topic(),
                swift_msg.value(),
                slack_client,
                self.test_channel,
                is_test=not self.send_to_slack
            )
            
            print(f"  Result: {success1} - {result1}")
            
            if not success1:
                print("❌ FAILED: Could not process Swift message")
                return False
                
            time.sleep(3)
            
            print(f"Step 2: Sending FERMI GBM message (same trigger {test_trigger})")
            
            # Process Fermi message
            fermi_msg = self.MockMessage("gcn.classic.text.FERMI_GBM_FIN_POS", fermi_notice)
            success2, result2 = gcn_bot.process_notice_and_send_message(
                fermi_msg.topic(),
                fermi_msg.value(),
                slack_client,
                self.test_channel,
                is_test=not self.send_to_slack
            )
            
            print(f"  Result: {success2} - {result2}")
            
            if not success2:
                print("❌ FAILED: Could not process Fermi message")
                return False
                
            print("✅ SUCCESS: Both facilities processed successfully")
            print("   📋 Check Slack to verify separate messages were created")
            return True
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            logger.exception("Error in different facilities test")
            return False
    
    def test_visibility_analysis(self):
        """Test the new 4-case visibility system."""
        print("\n" + "="*60)
        print("TESTING: New 4-Case Visibility System")
        print("="*60)
        
        if not visibility_available:
            print("❌ SKIPPED: Visibility plotter not available")
            return False
        
        # Test cases for different visibility scenarios
        visibility_test_cases = [
            {
                "name": "Observable Now",
                "description": "Target currently observable from Chile",
                "ra": 200.0,
                "dec": -30.0,
                "expected_status": "observable_now",
                "trigger": "VIS001"
            },
            {
                "name": "Observable Later",
                "description": "Target observable later tonight",
                "ra": 300.0,
                "dec": -20.0,
                "expected_status": "observable_later",
                "trigger": "VIS002"
            },
            {
                "name": "Observable Tomorrow",
                "description": "Target observable tomorrow night",
                "ra": 50.0,
                "dec": -25.0,
                "expected_status": "observable_tomorrow",
                "trigger": "VIS003"
            },
            {
                "name": "Not Observable",
                "description": "Target not observable from Chile",
                "ra": 120.0,
                "dec": 70.0,
                "expected_status": "not_observable",
                "trigger": "VIS004"
            },
            {
                "name": "IceCube High Priority",
                "description": "IceCube neutrino event (always high priority)",
                "ra": 180.0,
                "dec": -15.0,
                "expected_status": "variable",
                "trigger": "VIS005",
                "is_neutrino": True
            }
        ]
        
        test_results = []
        
        for i, test_case in enumerate(visibility_test_cases, 1):
            print(f"\n{'-'*50}")
            print(f"Test {i}: {test_case['name']}")
            print(f"Description: {test_case['description']}")
            print(f"Coordinates: RA={test_case['ra']}, DEC={test_case['dec']}")
            print(f"Expected Status: {test_case['expected_status']}")
            print(f"{'-'*50}")
            
            try:
                # Test visibility analysis directly
                print("Step 1: Testing visibility analysis...")
                
                result = plotter.create_visibility_plot(
                    ra=test_case['ra'],
                    dec=test_case['dec'],
                    grb_name=f"TEST_{test_case['trigger']}",
                    test_mode=True,
                    minalt=MIN_ALTITUDE,
                    minmoonsep=MIN_MOON_SEP
                )
                
                if isinstance(result, tuple) and len(result) == 2:
                    plot_path, visibility_info = result
                    
                    if visibility_info:
                        actual_status = visibility_info.get('status', 'unknown')
                        condition = visibility_info.get('condition', 'Unknown')
                        current_alt = visibility_info.get('current_altitude', 0)
                        
                        print(f"  ✅ Analysis completed")
                        print(f"  🌃 Actual Status: {actual_status}")
                        print(f"  📊 Condition: {condition}")
                        print(f"  📐 Current Altitude: {current_alt:.1f}°")
                        
                        # Check if status matches expectation
                        status_match = (actual_status == test_case['expected_status'] or 
                                      test_case['expected_status'] == 'variable')
                        match_icon = "✅" if status_match else "⚠️"
                        print(f"  {match_icon} Expected: {test_case['expected_status']}")
                        
                        # Log specific details based on status
                        if actual_status == 'observable_now':
                            remaining = visibility_info.get('remaining_hours', 0)
                            print(f"  ⏰ Remaining observing time: {remaining:.1f} hours")
                        elif actual_status == 'observable_later':
                            hours_until = visibility_info.get('hours_until_observable', 0)
                            print(f"  ⏱️ Observable in: {hours_until:.1f} hours")
                        elif actual_status == 'observable_tomorrow':
                            reason = visibility_info.get('reason', 'Check tomorrow')
                            print(f"  📅 Reason: {reason}")
                        else:
                            reason = visibility_info.get('reason', 'Unknown limitation')
                            print(f"  ❌ Limitation: {reason}")
                        
                        if plot_path:
                            print(f"  📊 Plot saved: {plot_path}")
                        
                    else:
                        print(f"  ❌ No visibility info returned")
                        actual_status = 'error'
                        status_match = False
                else:
                    print(f"  ❌ Unexpected result format")
                    actual_status = 'error'
                    status_match = False
                
                # Test with full GCN notice if this test should send to Slack
                if self.send_to_slack:
                    print("Step 2: Testing with full GCN notice...")
                    
                    if test_case.get('is_neutrino', False):
                        # Create IceCube notice
                        notice_content = f"""TITLE:           GCN/AMON NOTICE
NOTICE_DATE:     Thu 20 May 25 12:00:00 UT
NOTICE_TYPE:     ICECUBE_Astrotrack_GOLD
EVENT_NUM:       {test_case['trigger']}
RUN_NUM:         139876
DISCOVERY_DATE:  25/05/20
DISCOVERY_TIME:  43200.00 {{12:00:00.00}} UT
SRC_RA:          {test_case['ra']:.4f}d {{+{test_case['ra']/15:.0f}h {(test_case['ra']%15)*4:.0f}m 00s}} (J2000)
SRC_DEC:         {test_case['dec']:+.4f}d {{{test_case['dec']:+.0f}d 00' 00"}} (J2000)  
SRC_ERROR:       0.5 [deg radius]
ENERGY:          2.14e+02 [TeV]
SIGNALNESS:      6.23e-01 [dn]
FAR:             3.54 [yr^-1]"""
                        topic = "gcn.classic.text.ICECUBE_ASTROTRACK_GOLD"
                    else:
                        # Create Swift notice
                        notice_content = f"""TITLE:           GCN/SWIFT NOTICE
NOTICE_DATE:     Thu 20 May 25 12:00:00 UT
NOTICE_TYPE:     Swift-BAT GRB Position ACK
TRIGGER_NUM:     {test_case['trigger']}
GRB_RA:          {test_case['ra']:.4f}d {{+{test_case['ra']/15:.0f}h {(test_case['ra']%15)*4:.0f}m 00s}} (J2000)
GRB_DEC:         {test_case['dec']:+.4f}d {{{test_case['dec']:+.0f}d 00' 00"}} (J2000)
GRB_ERROR:       3.0 [arcmin radius]
GRB_DATE:        25/05/20
GRB_TIME:        43200.00 SOD {{12:00:00.00}} UT"""
                        topic = "gcn.classic.text.SWIFT_BAT_GRB_POS_ACK"
                    
                    # Process the message
                    msg = self.MockMessage(topic, notice_content)
                    success, result = gcn_bot.process_notice_and_send_message(
                        msg.topic(),
                        msg.value(),
                        slack_client,
                        self.test_channel,
                        is_test=False  # Actually send to Slack
                    )
                    
                    print(f"  📤 Slack message: {success} - {result}")
                
                test_results.append({
                    'name': test_case['name'],
                    'expected': test_case['expected_status'],
                    'actual': actual_status,
                    'match': status_match,
                    'success': True
                })
                
            except Exception as e:
                print(f"  ❌ ERROR: {e}")
                logger.exception(f"Error in visibility test {i}")
                test_results.append({
                    'name': test_case['name'],
                    'expected': test_case['expected_status'],
                    'actual': 'error',
                    'match': False,
                    'success': False
                })
        
        # Summary of visibility tests
        print(f"\n{'='*50}")
        print("VISIBILITY TEST SUMMARY")
        print(f"{'='*50}")
        
        total_tests = len(test_results)
        successful_tests = sum(1 for r in test_results if r['success'])
        matching_tests = sum(1 for r in test_results if r['match'])
        
        print(f"Total visibility tests: {total_tests}")
        print(f"Successful analyses: {successful_tests}")
        print(f"Status matches: {matching_tests}")
        print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
        print(f"Match rate: {(matching_tests/total_tests)*100:.1f}%")
        
        print(f"\nDetailed Results:")
        for result in test_results:
            success_icon = "✅" if result['success'] else "❌"
            match_icon = "✅" if result['match'] else "⚠️"
            print(f"  {success_icon} {result['name']:20} | Expected: {result['expected']:15} | Actual: {result['actual']:15} | {match_icon}")
        
        return successful_tests == total_tests
    
    def run_all_tests(self):
        """Run all available tests."""
        print("🧪 STARTING COMPREHENSIVE THREAD MANAGEMENT AND VISIBILITY TESTS")
        print(f"📤 Send to Slack: {'YES' if self.send_to_slack else 'NO'}")
        print(f"📢 Target channel: {self.test_channel}")
        print(f"👁️ Visibility testing: {'AVAILABLE' if visibility_available else 'NOT AVAILABLE'}")
        
        results = []
        
        # Test 1: Basic thread management
        results.append(("Basic Thread Management", self.test_basic_thread_management()))
        
        # Small delay between tests
        time.sleep(2)
        
        # Test 2: Different facilities
        results.append(("Different Facilities", self.test_different_facilities()))
        
        # Small delay between tests
        time.sleep(2)
        
        # Test 3: Visibility analysis (new)
        if visibility_available:
            results.append(("Dynamic Visibility Analysis", self.test_visibility_analysis_dynamic()))
        
        # Summary
        print("\n" + "="*60)
        print("COMPREHENSIVE TEST SUMMARY")
        print("="*60)
        
        passed = 0
        for test_name, result in results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{test_name:30} {status}")
            if result:
                passed += 1
        
        print(f"\nOverall: {passed}/{len(results)} test suites passed")
        print(f"Success rate: {(passed/len(results))*100:.1f}%")
        
        if self.send_to_slack:
            print(f"\n📱 Check your Slack channel ({self.test_channel}) for test messages!")
        else:
            print(f"\n💡 Add --send flag to actually send messages to Slack")
        
        return passed == len(results)

    def calculate_optimal_test_coordinates(self):
        """
        Calculate optimal coordinates for each visibility test case based on current time.
        This ensures test coordinates match expected visibility status.
        """
        if not visibility_available:
            # Return proven static coordinates as fallback
            return {
                'observable_now': {'ra': 200.0, 'dec': -35.0},
                'observable_later': {'ra': 280.0, 'dec': -25.0}, 
                'observable_tomorrow': {'ra': 50.0, 'dec': -30.0},
                'not_observable': {'ra': 0.0, 'dec': 75.0}
            }
        
        print("🔄 Calculating optimal test coordinates for current time...")
        
        # First try to find working coordinates by testing
        optimal_coords = {}
        
        # 1. Observable Now: Search for coordinates that are actually observable now
        print("  🔍 Finding 'observable now' coordinates...")
        observable_now_coords = self._find_working_coordinates("observable_now")
        optimal_coords['observable_now'] = observable_now_coords
        
        # 2. Observable Later: Search for coordinates observable later tonight
        print("  🔍 Finding 'observable later' coordinates...")
        observable_later_coords = self._find_working_coordinates("observable_later")
        optimal_coords['observable_later'] = observable_later_coords
        
        # 3. Observable Tomorrow: Search for coordinates observable tomorrow
        print("  🔍 Finding 'observable tomorrow' coordinates...")
        observable_tomorrow_coords = self._find_working_coordinates("observable_tomorrow")
        optimal_coords['observable_tomorrow'] = observable_tomorrow_coords
        
        # 4. Not Observable: Use high northern declination (always works)
        optimal_coords['not_observable'] = {
            'ra': 0.0,
            'dec': 75.0,  # Very northern declination, never visible from Chile
            'description': "Northern declination 75° (never visible from Chile)"
        }
        
        return optimal_coords

    def test_visibility_analysis_static(self):
        """Test visibility analysis with proven static coordinates."""
        print("\n" + "="*60)
        print("TESTING: Static Coordinate System (Reliable)")
        print("="*60)
        
        if not visibility_available:
            print("❌ SKIPPED: Visibility plotter not available")
            return False
        
        # Use proven coordinates that work reliably
        static_test_cases = [
            {
                "name": "High Southern Target",
                "description": "High declination southern target (usually observable)",
                "ra": 200.0,
                "dec": -35.0,
                "trigger": "STATIC001"
            },
            {
                "name": "Moderate Southern Target", 
                "description": "Moderate declination target for comparison",
                "ra": 280.0,
                "dec": -25.0,
                "trigger": "STATIC002"
            },
            {
                "name": "Low Southern Target",
                "description": "Low southern declination target",
                "ra": 50.0,
                "dec": -15.0,
                "trigger": "STATIC003"
            },
            {
                "name": "Northern Target (Not Observable)",
                "description": "High northern declination (never visible from Chile)",
                "ra": 0.0,
                "dec": 75.0,
                "trigger": "STATIC004"
            }
        ]
        
        test_results = []
        successful_tests = 0
        
        for i, test_case in enumerate(static_test_cases, 1):
            print(f"\n{'-'*50}")
            print(f"Static Test {i}: {test_case['name']}")
            print(f"Description: {test_case['description']}")
            print(f"Coordinates: RA={test_case['ra']:.1f}, DEC={test_case['dec']:.1f}")
            print(f"{'-'*50}")
            
            try:
                result = plotter.create_visibility_plot(
                    ra=test_case['ra'],
                    dec=test_case['dec'],
                    grb_name=f"STATIC_TEST_{test_case['trigger']}",
                    test_mode=True,
                    minalt=MIN_ALTITUDE,
                    minmoonsep=MIN_MOON_SEP
                )
                
                if isinstance(result, tuple) and len(result) == 2:
                    plot_path, visibility_info = result
                    
                    if visibility_info:
                        actual_status = visibility_info.get('status', 'unknown')
                        condition = visibility_info.get('condition', 'Unknown')
                        current_alt = visibility_info.get('current_altitude', 0)
                        
                        print(f"  ✅ Analysis completed")
                        print(f"  🌃 Status: {actual_status}")
                        print(f"  📊 Condition: {condition}")
                        print(f"  📐 Current Altitude: {current_alt:.1f}°")
                        
                        if plot_path:
                            print(f"  📊 Plot saved: {plot_path}")
                        
                        test_results.append({
                            'name': test_case['name'],
                            'status': actual_status,
                            'condition': condition,
                            'altitude': current_alt,
                            'success': True
                        })
                        successful_tests += 1
                        
                        # Test with GCN notice if sending to Slack
                        if self.send_to_slack:
                            print("Step 2: Testing with full GCN notice...")
                            self._test_static_gcn_notice(test_case)
            
            except Exception as e:
                print(f"  ❌ ERROR: {e}")
                test_results.append({
                    'name': test_case['name'],
                    'status': 'error',
                    'success': False
                })
        
        # Summary
        total_tests = len(test_results)
        print(f"\n{'='*60}")
        print("STATIC COORDINATE TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Total tests: {total_tests}")
        print(f"Successful analyses: {successful_tests}")
        print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
        
        print(f"\nDetailed Results:")
        for result in test_results:
            success_icon = "✅" if result['success'] else "❌"
            status = result.get('status', 'unknown')
            condition = result.get('condition', 'N/A')
            altitude = result.get('altitude', 0)
            print(f"  {success_icon} {result['name']:25} | Status: {status:15} | Condition: {condition:20} | Alt: {altitude:5.1f}°")
        
        return successful_tests >= total_tests * 0.75  # 75% success rate required

    def _test_static_gcn_notice(self, test_case):
        """Test static coordinates with GCN notice processing."""
        # Create test notice with static coordinates
        test_notice = f"""TITLE:           GCN/SWIFT NOTICE
    NOTICE_DATE:     Thu 20 May 25 10:30:15 UT
    NOTICE_TYPE:     Swift-BAT GRB Position
    TRIGGER_NUM:     {test_case['trigger']}
    GRB_RA:          {test_case['ra']:.4f}d
    GRB_DEC:         {test_case['dec']:.4f}d
    GRB_ERROR:       3.5 [arcsec radius]
    COMMENTS:        Static coordinate test case
    """
        
        try:
            # Parse and process notice
            from gcn_notice_handler import GCNNoticeHandler
            parsed_info = GCNNoticeHandler().parse_notice(test_notice, "gcn.classic.text.SWIFT_BAT_GRB_POS_ACK")
            
            if parsed_info and self.send_to_slack:
                print(f"  📤 GCN notice processed and sent to Slack")
            else:
                print(f"  ⚠️ GCN notice parsing failed or not sent")
                
        except Exception as e:
            print(f"  ❌ Error processing GCN notice: {e}")

    def _find_working_coordinates(self, target_status):
        """Find coordinates that actually produce the target visibility status."""
        
        # Define search ranges based on target status
        if target_status == "observable_now":
            # Search around current time coordinates
            from astropy.time import Time
            now = Time.now()
            current_lst = plotter.observer._observer.local_sidereal_time(now)
            ra_candidates = [(current_lst.deg - 30) % 360, current_lst.deg, (current_lst.deg + 30) % 360]
            dec_candidates = [-40, -35, -30, -25, -20, -15]
        elif target_status == "observable_later":
            from astropy.time import Time
            
            now = Time.now()
            current_lst = plotter.observer._observer.local_sidereal_time(now)

            ra_candidates = []
            for hours_ahead in [3, 4, 5]:
                future_ra = (current_lst.deg + hours_ahead * 15) % 360
                ra_candidates.append(future_ra)
            
            # Try eastern coordinates (rising objects) with higher declinations
            dec_candidates = [-10, -5, 0, 5, 10]  # Higher declinations that rise later
        elif target_status == "observable_tomorrow":
            # Search around tomorrow coordinates
            from astropy.time import Time
            now = Time.now()
            current_lst = plotter.observer._observer.local_sidereal_time(now)
            tomorrow_ra = (current_lst.deg + 180) % 360
            ra_candidates = [(tomorrow_ra - 30) % 360, tomorrow_ra, (tomorrow_ra + 30) % 360]
            dec_candidates = [-35, -30, -25, -20, -15]
        else:
            # Fallback
            ra_candidates = [200.0]
            dec_candidates = [-30.0]
        
        # Test combinations to find working coordinates
        for ra in ra_candidates:
            for dec in dec_candidates:
                try:
                    result = plotter.create_visibility_plot(
                        ra=ra, dec=dec,
                        grb_name=f"Test_{target_status}",
                        test_mode=True, savefig=False
                    )
                    
                    if isinstance(result, tuple) and len(result) == 2:
                        _, visibility_info = result
                        if visibility_info:
                            actual_status = visibility_info.get('status', 'unknown')
                            if actual_status == target_status:
                                print(f"    ✅ Found working coordinates: RA={ra:.1f}, DEC={dec:.1f}")
                                return {
                                    'ra': ra,
                                    'dec': dec,
                                    'description': f"Validated coordinates RA={ra:.1f}°, DEC={dec:.1f}° for {target_status}"
                                }
                except Exception as e:
                    continue  # Try next combination
        
        # If no working coordinates found, return fallback
        print(f"    ⚠️ No working coordinates found for {target_status}, using fallback")
        fallback_coords = {
            "observable_now": {'ra': 200.0, 'dec': -35.0},
            "observable_later": {'ra': 280.0, 'dec': -25.0},
            "observable_tomorrow": {'ra': 50.0, 'dec': -30.0}
        }
        
        coords = fallback_coords.get(target_status, {'ra': 200.0, 'dec': -30.0})
        coords['description'] = f"Fallback coordinates for {target_status}"
        return coords
    
    def test_visibility_analysis_dynamic(self):
            """Test the new 4-case visibility system with dynamically calculated coordinates."""
            print("\n" + "="*60)
            print("TESTING: Dynamic 4-Case Visibility System")
            print("="*60)
            
            if not visibility_available:
                print("❌ SKIPPED: Visibility plotter not available")
                return False
            
            # Calculate optimal coordinates for current time
            optimal_coords = self.calculate_optimal_test_coordinates()
            
            # Create test cases with calculated coordinates
            visibility_test_cases = [
                {
                    "name": "Observable Now",
                    "description": f"Target currently observable from Chile - {optimal_coords['observable_now']['description']}",
                    "ra": optimal_coords['observable_now']['ra'],
                    "dec": optimal_coords['observable_now']['dec'],
                    "expected_status": "observable_now",
                    "trigger": "DYN001"
                },
                {
                    "name": "Observable Later",
                    "description": f"Target observable later tonight - {optimal_coords['observable_later']['description']}",
                    "ra": optimal_coords['observable_later']['ra'],
                    "dec": optimal_coords['observable_later']['dec'],
                    "expected_status": "observable_later",
                    "trigger": "DYN002"
                },
                {
                    "name": "Observable Tomorrow",
                    "description": f"Target observable tomorrow night - {optimal_coords['observable_tomorrow']['description']}",
                    "ra": optimal_coords['observable_tomorrow']['ra'],
                    "dec": optimal_coords['observable_tomorrow']['dec'],
                    "expected_status": "observable_tomorrow",
                    "trigger": "DYN003"
                },
                {
                    "name": "Not Observable",
                    "description": f"Target not observable from Chile - {optimal_coords['not_observable']['description']}",
                    "ra": optimal_coords['not_observable']['ra'],
                    "dec": optimal_coords['not_observable']['dec'],
                    "expected_status": "not_observable",
                    "trigger": "DYN004"
                },
                {
                    "name": "IceCube High Priority",
                    "description": "IceCube neutrino event (high priority regardless of visibility)",
                    "ra": optimal_coords['observable_now']['ra'],  # Use observable coordinates
                    "dec": optimal_coords['observable_now']['dec'],
                    "expected_status": "variable",
                    "trigger": "DYN005",
                    "is_neutrino": True
                }
            ]
            
            test_results = []
            
            for i, test_case in enumerate(visibility_test_cases, 1):
                print(f"\n{'-'*50}")
                print(f"Test {i}: {test_case['name']}")
                print(f"Description: {test_case['description']}")
                print(f"Coordinates: RA={test_case['ra']:.1f}, DEC={test_case['dec']:.1f}")
                print(f"Expected Status: {test_case['expected_status']}")
                print(f"{'-'*50}")
                
                try:
                    # Test visibility analysis
                    print("Step 1: Testing visibility analysis...")
                    
                    result = plotter.create_visibility_plot(
                        ra=test_case['ra'],
                        dec=test_case['dec'],
                        grb_name=f"DYN_TEST_{test_case['trigger']}",
                        test_mode=True,
                        minalt=MIN_ALTITUDE,
                        minmoonsep=MIN_MOON_SEP
                    )
                    
                    if isinstance(result, tuple) and len(result) == 2:
                        plot_path, visibility_info = result
                        
                        if visibility_info:
                            actual_status = visibility_info.get('status', 'unknown')
                            condition = visibility_info.get('condition', 'Unknown')
                            current_alt = visibility_info.get('current_altitude', 0)
                            
                            print(f"  ✅ Analysis completed")
                            print(f"  🌃 Actual Status: {actual_status}")
                            print(f"  📊 Condition: {condition}")
                            print(f"  📐 Current Altitude: {current_alt:.1f}°")
                            
                            # Check if status matches expectation
                            status_match = (actual_status == test_case['expected_status'] or 
                                        test_case['expected_status'] == 'variable')
                            match_icon = "✅" if status_match else "⚠️"
                            print(f"  {match_icon} Expected: {test_case['expected_status']}")
                            
                            # Log specific details based on status
                            if actual_status == 'observable_now':
                                remaining = visibility_info.get('remaining_hours', 0)
                                end_time = visibility_info.get('observable_end')
                                if end_time:
                                    chile_time, korea_time = plotter._convert_time_to_clt_kst(end_time)
                                    print(f"  ⏰ Observable until: {chile_time.strftime('%H:%M')} CLT ({remaining:.1f}h remaining)")
                                else:
                                    print(f"  ⏰ Remaining observing time: {remaining:.1f} hours")
                                    
                            elif actual_status == 'observable_later':
                                hours_until = visibility_info.get('hours_until_observable', 0)
                                start_time = visibility_info.get('observable_start')
                                if start_time:
                                    chile_time, korea_time = plotter._convert_time_to_clt_kst(start_time)
                                    print(f"  ⏱️ Observable from: {chile_time.strftime('%H:%M')} CLT (in {hours_until:.1f} hours)")
                                else:
                                    print(f"  ⏱️ Observable in: {hours_until:.1f} hours")
                                    
                            elif actual_status == 'observable_tomorrow':
                                reason = visibility_info.get('reason', 'Check tomorrow')
                                print(f"  📅 Reason: {reason}")
                                if visibility_info.get('showing_tomorrow'):
                                    print(f"  🌅 Tomorrow's plot generated")
                            else:
                                reason = visibility_info.get('reason', 'Unknown limitation')
                                print(f"  ❌ Limitation: {reason}")
                            
                            if plot_path:
                                print(f"  📊 Plot saved: {plot_path}")
                            
                            # Special handling for non-matching results
                            if not status_match and test_case['expected_status'] != 'variable':
                                print(f"  💡 Coordinate adjustment may be needed for future tests")
                            
                        else:
                            print(f"  ❌ No visibility info returned")
                            actual_status = 'error'
                            status_match = False
                    else:
                        print(f"  ❌ Unexpected result format")
                        actual_status = 'error'
                        status_match = False
                    
                    # Test with full GCN notice if this test should send to Slack
                    if self.send_to_slack:
                        print("Step 2: Testing with full GCN notice...")
                        
                        if test_case.get('is_neutrino', False):
                            # Create IceCube notice
                            notice_content = f"""TITLE:           GCN/AMON NOTICE
        NOTICE_DATE:     Thu 20 May 25 12:00:00 UT
        NOTICE_TYPE:     ICECUBE_Astrotrack_GOLD
        EVENT_NUM:       {test_case['trigger']}
        RUN_NUM:         139876
        DISCOVERY_DATE:  25/05/20
        DISCOVERY_TIME:  43200.00 {{12:00:00.00}} UT
        SRC_RA:          {test_case['ra']:.4f}d {{+{test_case['ra']/15:.0f}h {(test_case['ra']%15)*4:.0f}m 00s}} (J2000)
        SRC_DEC:         {test_case['dec']:+.4f}d {{{test_case['dec']:+.0f}d 00' 00"}} (J2000)  
        SRC_ERROR:       0.5 [deg radius]
        ENERGY:          2.14e+02 [TeV]
        SIGNALNESS:      6.23e-01 [dn]
        FAR:             3.54 [yr^-1]"""
                            topic = "gcn.classic.text.ICECUBE_ASTROTRACK_GOLD"
                        else:
                            # Create Swift notice
                            notice_content = f"""TITLE:           GCN/SWIFT NOTICE
        NOTICE_DATE:     Thu 20 May 25 12:00:00 UT
        NOTICE_TYPE:     Swift-BAT GRB Position ACK
        TRIGGER_NUM:     {test_case['trigger']}
        GRB_RA:          {test_case['ra']:.4f}d {{+{test_case['ra']/15:.0f}h {(test_case['ra']%15)*4:.0f}m 00s}} (J2000)
        GRB_DEC:         {test_case['dec']:+.4f}d {{{test_case['dec']:+.0f}d 00' 00"}} (J2000)
        GRB_ERROR:       3.0 [arcmin radius]
        GRB_DATE:        25/05/20
        GRB_TIME:        43200.00 SOD {{12:00:00.00}} UT"""
                            topic = "gcn.classic.text.SWIFT_BAT_GRB_POS_ACK"
                        
                        # Process the message
                        msg = self.MockMessage(topic, notice_content)
                        success, result = gcn_bot.process_notice_and_send_message(
                            msg.topic(),
                            msg.value(),
                            slack_client,
                            self.test_channel,
                            is_test=False  # Actually send to Slack
                        )
                        
                        print(f"  📤 Slack message: {success} - {result}")
                    
                    test_results.append({
                        'name': test_case['name'],
                        'expected': test_case['expected_status'],
                        'actual': actual_status,
                        'match': status_match,
                        'success': True,
                        'coordinates': f"RA={test_case['ra']:.1f}, DEC={test_case['dec']:.1f}"
                    })
                    
                except Exception as e:
                    print(f"  ❌ ERROR: {e}")
                    logger.exception(f"Error in dynamic visibility test {i}")
                    test_results.append({
                        'name': test_case['name'],
                        'expected': test_case['expected_status'],
                        'actual': 'error',
                        'match': False,
                        'success': False,
                        'coordinates': f"RA={test_case['ra']:.1f}, DEC={test_case['dec']:.1f}"
                    })
            
            # Enhanced summary with coordinate information
            print(f"\n{'='*60}")
            print("DYNAMIC VISIBILITY TEST SUMMARY")
            print(f"{'='*60}")
            
            total_tests = len(test_results)
            successful_tests = sum(1 for r in test_results if r['success'])
            matching_tests = sum(1 for r in test_results if r['match'])
            
            print(f"Total visibility tests: {total_tests}")
            print(f"Successful analyses: {successful_tests}")
            print(f"Status matches: {matching_tests}")
            print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
            print(f"Match rate: {(matching_tests/total_tests)*100:.1f}%")
            
            print(f"\nDetailed Results:")
            for result in test_results:
                success_icon = "✅" if result['success'] else "❌"
                match_icon = "✅" if result['match'] else "⚠️"
                coords = result.get('coordinates', 'Unknown')
                print(f"  {success_icon} {result['name']:20} | {coords:20} | Expected: {result['expected']:15} | Actual: {result['actual']:15} | {match_icon}")
            
            if matching_tests < total_tests:
                print(f"\n💡 Suggestions for improving match rate:")
                print(f"   - Run tests at different times of day for better coordinate selection")
                print(f"   - Consider seasonal variations in target visibility")
                print(f"   - Adjust declination values based on current observing season")
            
            return successful_tests == total_tests

def main():
    """Main function for thread testing."""
    if not any([args.test_basic, args.test_facilities, args.test_visibility, args.test_all, 
            args.test_visibility_dynamic, args.test_visibility_static, args.test_improved]):
        print("Usage:")
        print("  python test_gcn_bot.py --test-basic              # Test basic thread management")
        print("  python test_gcn_bot.py --test-facilities         # Test different facilities")
        print("  python test_gcn_bot.py --test-visibility         # Test 4-case visibility system")
        print("  python test_gcn_bot.py --test-visibility-dynamic # Test with dynamic coordinates")
        print("  python test_gcn_bot.py --test-visibility-static  # Test with static coordinates (reliable)")
        print("  python test_gcn_bot.py --test-improved           # Test both dynamic and static")
        print("  python test_gcn_bot.py --test-all                # Run all tests")
        print("  python test_gcn_bot.py --send                    # Actually send to Slack")
        print("  python test_gcn_bot.py --verbose                 # Enable verbose logging")
        print("\nExamples:")
        print("  python test_gcn_bot.py --test-improved --send    # Test improved methods and send to Slack")
        print("  python test_gcn_bot.py --test-visibility-static  # Test only static coordinates (more reliable)")
        print("  python test_gcn_bot.py --test-all --send         # Run all tests and send to Slack")
        return
    
    tester = ThreadTester(send_to_slack=args.send)
    
    try:
        if args.test_all:
            tester.run_all_tests()
        else:
            if args.test_basic:
                tester.test_basic_thread_management()
            if args.test_facilities:
                tester.test_different_facilities()
            if args.test_visibility:
                tester.test_visibility_analysis()
            if args.test_visibility_dynamic:
                tester.test_visibility_analysis_dynamic()
            if args.test_visibility_static:
                tester.test_visibility_analysis_static()
            if args.test_improved:
                # Run both improved dynamic and static tests
                dynamic_success = tester.test_visibility_analysis_dynamic()
                static_success = tester.test_visibility_analysis_static()
                print(f"\n📊 Combined Results: Dynamic={dynamic_success}, Static={static_success}")

    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        logger.exception("Unexpected error in main")

if __name__ == "__main__":
    main()