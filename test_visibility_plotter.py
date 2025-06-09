#!/usr/bin/env python3
"""
Visibility System Testing Script
Test the visibility plotting and analysis system with various scenarios
"""

import logging
import os
from datetime import datetime, timedelta
from supy.supy.observer.visibility_plotter import VisibilityPlotter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_visibility_system():
    """Comprehensive test of the visibility system."""
    
    # Initialize plotter
    try:
        plotter = VisibilityPlotter(logger=logger)
        logger.info("✅ VisibilityPlotter initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize VisibilityPlotter: {e}")
        return False
    
    # Test scenarios: [RA, DEC, expected_status, description]
    test_cases = [
        # Currently observable from Chile (Southern sky)
        [180.0, -30.0, "observable", "Southern sky target"],
        
        # Observable later tonight (Eastern sky)
        [90.0, -20.0, "observable_later", "Eastern sky target"],
        
        # Not observable (Northern hemisphere)
        [200.0, 60.0, "not_observable", "Northern hemisphere target"],
        
        # Edge case: Near horizon
        [270.0, -10.0, "observable_later", "Near horizon target"],
        
        # Circumpolar (always visible)
        [180.0, -80.0, "observable", "Circumpolar target"],
        
        # Invalid coordinates
        [400.0, 100.0, "error", "Invalid coordinates"]
    ]
    
    results = []
    
    for i, (ra, dec, expected, description) in enumerate(test_cases, 1):
        logger.info(f"\n--- Test Case {i}: {description} ---")
        logger.info(f"Testing RA={ra}°, DEC={dec}°")
        
        try:
            # Create visibility plot
            plot_path, visibility_info = plotter.create_visibility_plot(
                ra=ra, dec=dec,
                grb_name=f"TEST_{i:02d}",
                test_mode=True,  # Save to test_plots directory
                minalt=30,
                minmoonsep=30
            )
            
            status = visibility_info.get('status', 'unknown') if visibility_info else 'failed'
            
            # Log results
            if plot_path:
                logger.info(f"✅ Plot created: {plot_path}")
                logger.info(f"📊 Status: {status}")
                
                if visibility_info:
                    if status == 'observable_now':
                        remaining = visibility_info.get('remaining_hours', 0)
                        logger.info(f"⏰ Observable for {remaining:.1f} more hours")
                    elif status == 'observable_later':
                        hours_until = visibility_info.get('hours_until_observable', 0)
                        logger.info(f"⏰ Observable in {hours_until:.1f} hours")
                    elif status == 'not_observable':
                        reason = visibility_info.get('reason', 'Unknown')
                        logger.info(f"❌ Not observable: {reason}")
                
                # Test message formatting
                formatted_msg = plotter.format_visibility_message(visibility_info)
                logger.info(f"📝 Formatted message length: {len(formatted_msg)} chars")
                
            else:
                logger.warning(f"⚠️ No plot created. Status: {status}")
                if visibility_info and visibility_info.get('message'):
                    logger.info(f"💬 Message: {visibility_info['message']}")
            
            results.append({
                'test_case': i,
                'description': description,
                'ra': ra, 'dec': dec,
                'plot_created': plot_path is not None,
                'status': status,
                'expected': expected,
                'visibility_info': visibility_info
            })
            
        except Exception as e:
            logger.error(f"❌ Test case {i} failed: {e}")
            results.append({
                'test_case': i,
                'description': description,
                'ra': ra, 'dec': dec,
                'plot_created': False,
                'status': 'error',
                'expected': expected,
                'error': str(e)
            })
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("TEST SUMMARY")
    logger.info("="*50)
    
    passed = 0
    total = len(results)
    
    for result in results:
        test_num = result['test_case']
        desc = result['description']
        status = result['status']
        expected = result['expected']
        
        # Simple pass/fail logic
        if expected == "error" and status == "error":
            success = "✅ PASS"
            passed += 1
        elif expected == "observable" and status in ["observable_now", "observable_later"]:
            success = "✅ PASS"
            passed += 1
        elif expected == "observable_later" and status == "observable_later":
            success = "✅ PASS"
            passed += 1
        elif expected == "not_observable" and status == "not_observable":
            success = "✅ PASS"
            passed += 1
        else:
            success = "❌ FAIL"
        
        logger.info(f"Test {test_num}: {desc} - {success}")
        logger.info(f"  Expected: {expected}, Got: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    return passed == total

def test_coordinate_edge_cases():
    """Test edge cases for coordinate handling."""
    logger.info("\n" + "="*50)
    logger.info("TESTING COORDINATE EDGE CASES")
    logger.info("="*50)
    
    plotter = VisibilityPlotter(logger=logger)
    
    edge_cases = [
        [0.0, 0.0, "Celestial equator"],
        [360.0, 0.0, "RA boundary"],
        [180.0, 90.0, "North pole"], 
        [180.0, -90.0, "South pole"],
        [-10.0, 0.0, "Negative RA"],
        [370.0, 0.0, "RA > 360°"],
        [180.0, 100.0, "DEC > 90°"]
    ]
    
    for ra, dec, description in edge_cases:
        logger.info(f"\nTesting: {description} (RA={ra}°, DEC={dec}°)")
        
        try:
            plot_path, visibility_info = plotter.create_visibility_plot(
                ra=ra, dec=dec, grb_name="EDGE_TEST", test_mode=True
            )
            
            if plot_path:
                logger.info(f"✅ Handled gracefully, plot created")
            else:
                status = visibility_info.get('status') if visibility_info else 'unknown'
                logger.info(f"⚠️ No plot created, status: {status}")
                
        except Exception as e:
            logger.info(f"❌ Exception raised: {type(e).__name__}: {e}")

if __name__ == "__main__":
    logger.info("Starting Visibility System Tests...")
    
    # Main functionality test
    main_success = test_visibility_system()
    
    # Edge case test
    test_coordinate_edge_cases()
    
    # Final result
    if main_success:
        logger.info("\n🎉 All main tests PASSED! Visibility system is working correctly.")
    else:
        logger.warning("\n⚠️ Some tests FAILED. Please review the visibility system.")
    
    # Check test plots directory
    test_plots_dir = "./test_plots"
    if os.path.exists(test_plots_dir):
        plot_files = [f for f in os.listdir(test_plots_dir) if f.endswith('.png')]
        logger.info(f"\n📁 {len(plot_files)} test plots created in {test_plots_dir}")
        for plot_file in sorted(plot_files)[-5:]:  # Show last 5 files
            logger.info(f"  - {plot_file}")
    
    logger.info("\nVisibility system testing complete!")