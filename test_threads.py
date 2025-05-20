#!/usr/bin/env python3
"""
Thread Management Test Script
============================
Separate test script for testing thread management functionality
without modifying the main gcn_bot.py code.

Usage:
    python test_threads.py --test-basic
    python test_threads.py --test-facilities  
    python test_threads.py --test-all
    python test_threads.py --send  # Actually send to Slack
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# Parse arguments FIRST, before importing modules that have their own argparse
def parse_arguments():
    """Parse command line arguments before any imports that might conflict."""
    parser = argparse.ArgumentParser(description='Thread Management Test Suite')
    parser.add_argument('--test-basic', action='store_true', help='Test basic thread management')
    parser.add_argument('--test-facilities', action='store_true', help='Test different facilities')
    parser.add_argument('--test-all', action='store_true', help='Run all tests')
    parser.add_argument('--send', action='store_true', help='Actually send messages to Slack')
    
    return parser.parse_args()

# Parse arguments early to avoid conflicts
args = parse_arguments()

# Add the main directory to Python path to import modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Now do the imports AFTER parsing our arguments
try:
    # Import config first
    try:
        from config import SLACK_TOKEN, SLACK_CHANNEL, SLACK_CHANNEL_TEST
    except ImportError:
        print("❌ Error: Cannot find config.py. Please ensure config.py exists with SLACK_TOKEN and SLACK_CHANNEL settings.")
        sys.exit(1)
    
    # Import Slack client
    from slack_sdk import WebClient
    slack_client = WebClient(token=SLACK_TOKEN)
    
    # Import notice handler
    from gcn_notice_handler import GCNNoticeHandler
    notice_handler = GCNNoticeHandler()
    
    # Import specific functions from gcn_bot without executing its main code
    import gcn_bot
    
    print("✅ Successfully imported main bot modules")
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("Make sure you're running this from the same directory as gcn_bot.py")
    sys.exit(1)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ThreadTester:
    """Class to handle thread management testing."""
    
    def __init__(self, send_to_slack=False):
        self.send_to_slack = send_to_slack
        self.test_channel = SLACK_CHANNEL_TEST if SLACK_CHANNEL_TEST else SLACK_CHANNEL
        
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
GRB_DEC:         +25.5678d {{+25d 34' 04"}} (J2000)
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
GRB_DEC:         +25.4321d {{+25d 25' 56"}} (J2000)
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
            print(f"  Coordinates: RA=150.1234, DEC=+25.5678, Error=3.5 arcsec")
            
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
                
            # Check for thread_ts storage - note: this requires thread storage to be implemented
            time.sleep(2)
            
            print(f"\nStep 2: Sending UPDATE message")
            print(f"  Updated coordinates: RA=150.2468, DEC=+25.4321, Error=1.8 arcsec")
            print(f"  Expected: Thread reply showing coordinate changes")
            
            time.sleep(2)  # Brief pause between messages
            
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
            print("   Note: Full thread verification requires thread storage implementation")
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
GRB_DEC:         +30.0000d {{+30d 00' 00"}} (J2000)
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
GRB_DEC:         +30.1000d {{+30d 06' 00"}} (J2000)
GRB_ERROR:       2.5 [deg radius]
GRB_DATE:        25/05/20
GRB_TIME:        39600.00 SOD {{11:00:00.00}} UT
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
                
            time.sleep(2)
            
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
            print("   Note: Check Slack to verify separate messages were created")
            return True
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            logger.exception("Error in different facilities test")
            return False
    
    def run_all_tests(self):
        """Run all available tests."""
        print("🧪 STARTING COMPREHENSIVE THREAD MANAGEMENT TESTS")
        print(f"📤 Send to Slack: {'YES' if self.send_to_slack else 'NO'}")
        
        results = []
        
        # Test 1: Basic thread management
        results.append(("Basic Thread Management", self.test_basic_thread_management()))
        
        # Test 2: Different facilities
        results.append(("Different Facilities", self.test_different_facilities()))
        
        # Summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        
        passed = 0
        for test_name, result in results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{test_name:30} {status}")
            if result:
                passed += 1
        
        print(f"\nTotal: {passed}/{len(results)} tests passed")
        
        if self.send_to_slack:
            print(f"\n📱 Check your Slack channel ({self.test_channel}) for test messages!")
        
        return passed == len(results)

def main():
    """Main function for thread testing."""
    if not any([args.test_basic, args.test_facilities, args.test_all]):
        print("Usage:")
        print("  python test_threads.py --test-basic      # Test basic thread management")
        print("  python test_threads.py --test-facilities # Test different facilities")
        print("  python test_threads.py --test-all        # Run all tests")
        print("  python test_threads.py --send            # Actually send to Slack")
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
                
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        logger.exception("Unexpected error in main")

if __name__ == "__main__":
    main()