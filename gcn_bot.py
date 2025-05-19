#!/usr/bin/env python3
"""
GCN Alert Monitoring System
==========================
A real-time monitoring system for GCN (Gamma-ray Coordinates Network) alerts 
with Slack bot and data storage capabilities.

Author:         YoungPyo Hong
Updated:        2025-04-03
Version:        2.0.0
License:        MIT
Copyright:      (c) 2025 YoungPyo Hong
"""

import os
import json
import time
import signal
import logging
import argparse
import sys
import pytz
import re
from io import BytesIO
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Any, Optional, Union, List
from threading import Thread, Lock

# Third-party imports
from gcn_kafka import Consumer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Local imports
from gcn_notice_handler import GCNNoticeHandler

# Configure logging with a custom formatter to show source file
class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Add source file and line number to the log message
        record.message = record.getMessage()
        source_info = f"[{record.pathname.split('/')[-1]}:{record.lineno}]"
        
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
            s = f"{record.asctime} - {record.levelname} - {source_info} - {record.message}"
        else:
            s = f"{record.levelname} - {source_info} - {record.message}"
            
        return s

# Set up logging
def setup_logging():
    logger = logging.getLogger('gcn_bot')
    logger.setLevel(logging.INFO)
    
    # Create formatter
    formatter = CustomFormatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler('gcn_bot.log')
    file_handler.setFormatter(formatter)
    
    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    # Silence kafka logs
    logging.getLogger('kafka').setLevel(logging.ERROR)
    
    return logger

# Initialize logger
logger = setup_logging()

############################ Configuration ############################
# Default configuration (will be overridden by config.py if available)
CONNECTION_TIMEOUT = 300  # seconds
SLACK_TOKEN = "your_slack_bot_token"
SLACK_CHANNEL = "your_slack_channel"
SLACK_CHANNEL_TEST = None  # For test messages
GCN_ID = 'your_gcn_client_id'
GCN_SECRET = 'your_gcn_client_secret'

# Visibility settings
MIN_ALTITUDE = 30
MIN_MOON_SEP = 30

# Output file settings
OUTPUT_NOTICE_CSV = 'gcn_notices.csv'
OUTPUT_ASCII = 'grb_targets.ascii'
ASCII_MAX_EVENTS = 10

# ToO email settings
TURN_ON_TOO_EMAIL = False
EMAIL_FROM = "your_email@example.com"
EMAIL_PASSWORD = "your_email_password"
TOO_CONFIG = {
    'singleExposure': 100,
    'imageCount': 3,
    'obsmode': 'Deep',
    'selectedFilters': ['r', 'i'],
    'selectedTelNumber': 1,
    'abortObservation': 'Yes',
    'priority': 'High',
    'gain': 'High',
    'radius': '0',
    'binning': '1',
}

# Topics to monitor
DISPLAY_TOPICS = [
    'gcn.classic.text.AMON_NU_EM_COINC',
    'gcn.classic.text.ICECUBE_CASCADE',
    'gcn.classic.text.HAWC_BURST_MONITOR',
    'gcn.classic.text.ICECUBE_ASTROTRACK_BRONZE',
    'gcn.classic.text.ICECUBE_ASTROTRACK_GOLD',
    'gcn.classic.text.FERMI_GBM_GND_POS',
    'gcn.classic.text.FERMI_LAT_OFFLINE',
    'gcn.classic.text.SWIFT_BAT_GRB_POS_ACK',
    'gcn.classic.text.SWIFT_UVOT_POS',
    'gcn.classic.text.SWIFT_XRT_POSITION',
    'gcn.notices.einstein_probe.wxt.alert'
]

# Try to import config.py to override defaults
try:
    from config import *
except ImportError:
    logger.warning("Configuration file 'config.py' not found. Using default values.")

# Global variables
running = True
last_heartbeat = datetime.now()
heartbeat_lock = Lock()
last_connection_status = True  # True = connected, False = disconnected

# Initialize notice handler (now a required component)
notice_handler = GCNNoticeHandler(
    output_csv=OUTPUT_NOTICE_CSV,
    output_ascii=OUTPUT_ASCII,
    ascii_max_events=ASCII_MAX_EVENTS
)

# Initialize Slack client
slack_client = WebClient(token=SLACK_TOKEN)
slack_channel = SLACK_CHANNEL

# Try to import and initialize visibility plotter
try:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(parent_dir)
    from supy.supy.observer.visibility_plotter import VisibilityPlotter
    plotter = VisibilityPlotter(logger=logger)
    visibility_available = True
    logger.info("Successfully imported visibility plotter from supy package")
except ImportError:
    # logger.warning("Failed to import visibility plotter from supy, trying local import")
    # try:
    #     from visibility_plotter_old import VisibilityPlotter
    #     plotter = VisibilityPlotter(logger=logger)
    #     visibility_available = True
    #     logger.info("Successfully imported local visibility plotter")
    # except ImportError:
    logger.warning("Visibility plotter not available. Skipping visibility plots.")
    visibility_available = False

# Initialize GCN consumer
consumer = Consumer(
    client_id=GCN_ID,
    client_secret=GCN_SECRET
)

# Subscribe to topics
all_topics = DISPLAY_TOPICS + ['gcn.heartbeat']
consumer.subscribe(all_topics)

# Parse command line arguments
parser = argparse.ArgumentParser(description='GCN Alert Monitor')
parser.add_argument('--test', action='store_true', help='Run a test with a GCN test message')
parser.add_argument('--send', action='store_true', help='Actually send test messages to Slack')
args = parser.parse_args()

TEST_SEND_TO_SLACK = args.send

############################## Signal Handling ##############################
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Received shutdown signal")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

############################## Connection Monitoring ##############################
def update_heartbeat():
    """Update the last heartbeat time"""
    global last_heartbeat
    with heartbeat_lock:
        last_heartbeat = datetime.now()
        logger.debug("Updated last heartbeat time")

def check_connection():
    """Monitor connection status and handle reconnections."""
    global last_connection_status, last_heartbeat, consumer, running, all_topics
    
    while running:
        try:
            with heartbeat_lock:
                time_since_last_heartbeat = datetime.now() - last_heartbeat
                currently_connected = time_since_last_heartbeat.total_seconds() < CONNECTION_TIMEOUT
                
                # If connection status has changed
                if currently_connected != last_connection_status:
                    try:
                        if not currently_connected:
                            logger.warning(f"Connection lost. Last heartbeat: {last_heartbeat}")
                            send_connection_status_message(False, last_heartbeat, time_since_last_heartbeat.total_seconds())
                        else:
                            logger.info(f"Connection restored. Last heartbeat: {last_heartbeat}")
                            send_connection_status_message(True)
                            
                        # Update status regardless
                        last_connection_status = currently_connected
                        
                    except Exception as e:
                        logger.error(f"Error processing connection status: {e}")
                
                # Attempt reconnection if needed
                if not currently_connected:
                    logger.info("Attempting to reconnect to GCN...")
                    try:
                        # Close existing consumer first
                        try:
                            consumer.close()
                            logger.info("Closed existing consumer connection")
                        except Exception as close_err:
                            logger.warning(f"Error closing existing consumer: {close_err}")
                        
                        # Initialize new consumer
                        new_consumer = Consumer(
                            client_id=GCN_ID,
                            client_secret=GCN_SECRET
                        )
                        
                        # Subscribe to topics
                        new_consumer.subscribe(all_topics)
                        
                        # Test connection with a short timeout
                        new_consumer.consume(timeout=1)
                        
                        # Replace global consumer with new one
                        consumer = new_consumer
                        logger.info("Successfully reestablished GCN connection")
                        
                        # Update heartbeat
                        last_heartbeat = datetime.now()
                        
                    except Exception as e:
                        logger.error(f"Reconnection attempt failed: {e}")
                        time.sleep(5)  # Wait before retrying
            
        except Exception as e:
            logger.error(f"Error in connection monitoring: {e}")
        
        # Check every minute
        time.sleep(60)

def send_connection_status_message(connected, last_heartbeat=datetime.now(), time_diff=None):
    """Send connection status updates to Slack."""
    try:
        if connected:
            message = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "✅ GCN Connection Restored"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        }
                    }
                ]
            }
        else:
            message = {
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": "⚠️ GCN Connection Alert"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Status:* Connection Lost\n*Last Heartbeat:* " + 
                                    f"{last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')}\n" +
                                    f"*Time Since Last Heartbeat:* {time_diff:.1f} seconds"
                        }
                    }
                ]
            }
            
        slack_client.chat_postMessage(
            channel=slack_channel,
            blocks=message["blocks"]
        )
        logger.info(f"Sent connection {'restoration' if connected else 'loss'} notification to Slack")
    except Exception as e:
        logger.error(f"Failed to send connection status message: {e}")

############################## Message Formatting ##############################
def format_message_from_notice_data(notice_data, topic):
    """
    Format a Slack message directly from parsed notice data.
    
    Args:
        notice_data: Dictionary containing parsed notice information
        topic: The original topic the notice came from
        
    Returns:
        dict: Formatted Slack message blocks
    """
    # Always use facility from notice_data - already normalized by gcn_notice_handler
    facility = notice_data.get('Facility', '')
    
    # Get facility emoji
    emoji = "📡"  # Default emoji
    
    if "Swift" in facility:
        emoji = "⚡"  # Lightning for Swift
    elif "Fermi" in facility:
        emoji = "☢️"  # Telescope for Fermi
    elif "Einstein" in facility:
        emoji = "👨🏻‍🔬"  # Lightning for Einstein Probe
    elif "IceCube" in facility:
        emoji = "❄️"  # Snowflake for IceCube
    elif "HAWC" in facility:
        emoji = "💧"  # Water drop for HAWC
    elif "AMON" in facility:
        emoji = "🔢"  # Cycle for AMON (coincidences)
    
    # Create message sections
    basic_info = []
    if 'Name' in notice_data and notice_data['Name']:
        basic_info.append(f"*GRB_NAME:*    {notice_data['Name']}")
    if 'Trigger_num' in notice_data and notice_data['Trigger_num']:
        basic_info.append(f"*TRIGGER_NUM:*    {notice_data['Trigger_num']}")
    
    # Get and add notice URL
    notice_url = get_notice_url(notice_data)
    if notice_url:
        basic_info.append(f"*NOTICE_URL:*    <{notice_url}|View on GCN>")
    
    # LOCATION section
    location_info = []
    if 'RA' in notice_data and notice_data['RA'] is not None:
        location_info.append(f"*RA:*    {notice_data['RA']} degrees")
    if 'DEC' in notice_data and notice_data['DEC'] is not None:
        location_info.append(f"*DEC:*    {notice_data['DEC']} degrees")
    if 'Error' in notice_data and notice_data['Error'] is not None:
        location_info.append(f"*ERROR:*    {notice_data['Error']} degrees")
    
    # TIMING section
    timing_info = []
    trigger_time = None
    notice_date = None
    
    # Add discovery time
    if 'Discovery_UTC' in notice_data and notice_data['Discovery_UTC']:
        trigger_time = notice_data['Discovery_UTC']
        if isinstance(trigger_time, datetime):
            # Add both UTC and KST times
            kst = trigger_time.astimezone(pytz.timezone('Asia/Seoul'))
            time_str = f"{trigger_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC) / {kst.strftime('%Y-%m-%d %H:%M:%S')} (KST)"
        else:
            time_str = str(trigger_time)
        timing_info.append(f"*TRIGGER_TIME:*    {time_str}")
    
    # Add notice date
    if 'Notice_date' in notice_data and notice_data['Notice_date']:
        notice_date = notice_data['Notice_date']
        if isinstance(notice_date, datetime):
            # Add both UTC and KST times
            kst = notice_date.astimezone(pytz.timezone('Asia/Seoul'))
            time_str = f"{notice_date.strftime('%Y-%m-%d %H:%M:%S')} (UTC) / {kst.strftime('%Y-%m-%d %H:%M:%S')} (KST)"
        else:
            time_str = str(notice_date)
        timing_info.append(f"*NOTICE_DATE:*    {time_str}")
    
    # Add time difference (calculate_time_diff integrated here)
    if trigger_time and notice_date:
        try:
            # Calculate difference in seconds
            diff_seconds = (notice_date - trigger_time).total_seconds()
            
            # Format difference
            hours, remainder = divmod(abs(diff_seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Determine if notice came before or after trigger
            if diff_seconds >= 0:
                time_diff = f"+{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} (Notice after Discovery)"
            else:
                time_diff = f"-{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} (Notice before Discovery)"
                
            timing_info.append(f"*TIME_DIFF:*    {time_diff}")
                
        except Exception as e:
            logger.error(f"Error calculating time difference: {e}")
    
    # Build blocks with emoji in header
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} {facility}"
            }
        }
    ]
    
    # Add sections to blocks
    if basic_info:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*[BASIC INFO]*\n> " + "\n> ".join(basic_info)
            }
        })
    
    if location_info:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*[LOCATION]*\n> " + "\n> ".join(location_info)
            }
        })
    
    if timing_info:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*[TIMING]*\n> " + "\n> ".join(timing_info)
            }
        })
    
    # Add a footer with status info
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"Message processed by GCN Bot at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
        ]
    })
    
    return {"blocks": blocks}

def get_notice_url(notice_data):
    """Get URL for the GCN notice based on facility and trigger number."""
    try:
        facility = notice_data.get('Facility', '')
        trigger_num = notice_data.get('Trigger_num', '')
        
        if not trigger_num:
            return None
            
        # Format varies by facility
        if 'Swift' in facility:
            return f"https://gcn.gsfc.nasa.gov/other/{trigger_num}.swift"
        elif 'Fermi' in facility:
            return f"https://gcn.gsfc.nasa.gov/other/{trigger_num}.fermi"
        elif 'Einstein' in facility or 'EP' in facility:
            return None
        elif 'IceCube' in facility:
            # For IceCube, we need additional run number
            if 'Run_num' in notice_data:
                run_num = notice_data.get('Run_num')
                if 'CASCADE' in facility:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_icecube_cascade/{run_num}_{trigger_num}.amon"
                elif 'GOLD' in facility or 'BRONZE' in facility:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_g_b/{run_num}_{trigger_num}.amon"
            # Try a generic format if Run_num not available
            return f"https://gcn.gsfc.nasa.gov/notices_amon/{trigger_num}.amon"
        else:
            logger.error("No facility matched")
            return None
    except Exception as e:
        logger.error(f"Error getting notice URL: {e}")
        return None

############################## Main Processing ##############################
def process_notice_and_send_message(topic, value, slack_client, slack_channel, is_test=False):
    """
    Process a GCN notice and send to Slack with visibility plot.
    
    Args:
        topic: GCN topic
        value: Message value (usually bytes)
        slack_client: Initialized Slack client
        slack_channel: Slack channel to post to
        is_test: Whether this is a test message (skip database saving)
        
    Returns:
        tuple: (success, message)
    """
    try:
        # Skip test notices in production mode
        if '_TEST' in topic.upper() and not args.test:
            logger.info(f"Skipping test notice from {topic}")
            return False, "Test notice skipped"
        
        # 1. Parse and save the notice with handler
        logger.info(f"Parsing notice from {topic}")
        notice_data = notice_handler.parse_notice(value, topic)
        
        if not notice_data:
            logger.warning(f"Failed to parse notice from {topic}")
            return False, "Failed to parse notice"
        
        # Save to databases (skip if this is a test message)
        csv_status = False
        ascii_status = False
        
        if not is_test:
            # Only save to database if not a test
            csv_status = notice_handler.save_to_csv(notice_data)
            ascii_status = notice_handler.save_to_ascii(notice_data)
            
            # Log database save status
            if csv_status:
                logger.info(f"Saved notice to CSV database")
            else:
                logger.warning(f"Failed to save notice to CSV database")
                
            if ascii_status:
                logger.info(f"Saved/updated event in ASCII database")
            else:
                logger.warning(f"Failed to save/update event in ASCII database")
        else:
            logger.info("Test mode: Skipping database save")
        
        # 2. Format the message for Slack
        slack_message = format_message_from_notice_data(notice_data, topic)
        
        # 3. Process visibility information if coordinates available
        plot_path = None
        visibility_info = None
        visibility_blocks = []
        
        ra = notice_data.get('RA')
        dec = notice_data.get('DEC')
        
        if visibility_available and ra is not None and dec is not None:
            try:
                logger.info(f"Generating visibility plot for {notice_data.get('Name', 'target')}")
                
                result = plotter.create_visibility_plot(
                    ra=ra,
                    dec=dec,
                    grb_name=notice_data.get('Name', ''),
                    test_mode=False,
                    minalt=MIN_ALTITUDE,
                    minmoonsep=MIN_MOON_SEP
                )
                
                plot_path, visibility_info = result
                
                if visibility_info:
                    # Format visibility information
                    visibility_text = plotter.format_visibility_message(visibility_info)
                    
                    # Add visibility blocks
                    visibility_blocks = [
                        {
                            "type": "divider"
                        },
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": "Visibility Information"
                            }
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": visibility_text
                            }
                        }
                    ]
                    
                    logger.info(f"Visibility status: {visibility_info.get('status', 'unknown')}")
                    
            except Exception as e:
                logger.error(f"Error creating visibility plot: {e}")
                visibility_blocks = [
                    {
                        "type": "divider"
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Visibility Analysis Error*\n{str(e)}"
                        }
                    }
                ]
        
        # 4. Add database status to the message
        status_blocks = [{
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"✅ CSV: {'Saved' if csv_status else '❌ Failed'} | "
                           f"✅ ASCII: {'Saved' if ascii_status else '❌ Failed'}"
                }
            ]
        }]
        
        # 5. Combine all blocks
        message_blocks = slack_message.get('blocks', []) + status_blocks + visibility_blocks
        
        # 6. Send the main message
        if not is_test or (is_test and TEST_SEND_TO_SLACK):
            try:
                response = slack_client.chat_postMessage(
                    channel=slack_channel,
                    blocks=message_blocks,
                    text=f"GCN Alert: {notice_data.get('Name', 'New Target')}",
                    unfurl_links=False,
                    unfurl_media=False
                )
                
                thread_ts = response['ts']
                
                # 7. Add LC URL as a thread reply if available (skip for EP)
                facility = notice_data.get('Facility', '')
                if not ('Einstein' in facility or 'EP' in facility):
                    lc_url = notice_data.get('LC_URL', '')
                    if lc_url:
                        logger.info(f"Sending LC URL to thread: {lc_url}")
                        try:
                            lc_message = f"*Light Curve Link*\n<{lc_url}|Click here to view Light Curve>\n_(Note: Image may take a few minutes to generate)_"
                            
                            slack_client.chat_postMessage(
                                channel=slack_channel,
                                thread_ts=thread_ts,
                                text=lc_message,
                                unfurl_links=False
                            )
                        except Exception as e:
                            logger.error(f"Error sending LC URL: {e}")
                    else:
                        logger.info(f"No LC URL found for {notice_data.get('Name', 'target')}")
                
                # 8. Add visibility plot as a thread reply if available
                if plot_path and os.path.exists(plot_path):
                    logger.info(f"Uploading visibility plot to thread")
                    try:
                        plot_response = slack_client.files_upload_v2(
                            file_uploads=[{"file": plot_path}],
                            channel=slack_channel,
                            thread_ts=thread_ts,
                            title=f"Visibility Plot: {notice_data.get('Name', 'Target')}"
                        )
                        
                        # Clean up plot file after sending
                        if not plot_path.startswith('./test_plots'):
                            os.remove(plot_path)
                            
                    except Exception as e:
                        logger.error(f"Error uploading plot: {e}")
                else:
                    logger.info(f"No visibility plot found for {notice_data.get('Name', 'target')}")
                
                # 9. Send ToO email if enabled and target is observable
                if TURN_ON_TOO_EMAIL and visibility_info and visibility_info.get('status') in ['observable_now', 'observable_later']:
                    try:
                        # Import dynamically to avoid dependency if not used
                        from gcn_too_emailer import GCNToOEmailer
                        
                        # Initialize emailer
                        emailer = GCNToOEmailer(
                            email_from=EMAIL_FROM,
                            email_to=["7dt.observation.alert@gmail.com"],
                            email_password=EMAIL_PASSWORD
                        )
                        
                        # Send the email with visibility info
                        email_sent = emailer.process_notice(notice_data, TOO_CONFIG, visibility_info)
                        logger.info(f"ToO email request sent: {email_sent}")
                    except Exception as e:
                        logger.error(f"Error sending ToO email: {e}")
                else:
                    logger.info(f"Skipping ToO email request for {notice_data.get('Name', 'target')}")
                
                return True, "Message sent successfully"
                
            except Exception as e:
                logger.error(f"Error sending Slack message: {e}")
                return False, f"Error sending message: {str(e)}"
            
        else:
            logger.info("Test mode without --send flag: Skipping Slack message")
            return True, "Test completed (no messages sent)"
        
    except Exception as e:
        logger.error(f"Error processing notice: {e}", exc_info=True)
        return False, str(e)

############################## Test Function ##############################
def test_message():
    """Process test messages using predefined test messages for different formats."""
    # Fermi GBM test message
    fermi_test_topic = "gcn.classic.text.FERMI_GBM_FLT_POS"
    fermi_test_value = """TITLE:           GCN/FERMI NOTICE
NOTICE_DATE:     Thu 27 Mar 25 22:22:04 UT
NOTICE_TYPE:     Fermi-GBM Flight Position
RECORD_NUM:      45
TRIGGER_NUM:     764806900
GRB_RA:          291.683d {+19h 26m 44s} (J2000),
                 292.023d {+19h 28m 06s} (current),
                 291.009d {+19h 24m 02s} (1950)
GRB_DEC:          -7.317d {-07d 19' 00"} (J2000),
                  -7.264d {-07d 15' 51"} (current),
                  -7.418d {-07d 25' 04"} (1950)
GRB_ERROR:       35.10 [deg radius, statistical plus systematic]
GRB_INTEN:       195 [cnts/sec]
DATA_SIGNIF:     2.80 [sigma]
INTEG_TIME:      0.032 [sec]
GRB_DATE:        20761 TJD;    86 DOY;   25/03/27
GRB_TIME:        80495.26 SOD {22:21:35.26} UT
GRB_PHI:          59.00 [deg]
GRB_THETA:        70.00 [deg]
DATA_TIME_SCALE: 0.0320 [sec]
HARD_RATIO:      0.00
LOC_ALGORITHM:   3 (version number of)
MOST_LIKELY:     100%  Unreliable location
2nd_MOST_LIKELY:   0%  n/a
DETECTORS:       0,0,0, 0,1,1, 0,0,0, 0,0,0, 0,0,
SUN_POSTN:         6.89d {+00h 27m 33s}   +2.98d {+02d 58' 33"}
SUN_DIST:         75.40 [deg]   Sun_angle= 5.0 [hr] (West of Sun)
MOON_POSTN:      347.85d {+23h 11m 24s}   -6.26d {-06d 15' 31"}
MOON_DIST:        55.41 [deg]
MOON_ILLUM:      3 [%]
GAL_COORDS:       30.36,-11.16 [deg] galactic lon,lat of the burst (or transient)
ECL_COORDS:      292.24, 14.46 [deg] ecliptic lon,lat of the burst (or transient)
LC_URL:          http://heasarc.gsfc.nasa.gov/FTP/fermi/data/gbm/triggers/2025/bn250327932/quicklook/glg_lc_medres34_bn250327932.gif
COMMENTS:        Fermi-GBM Flight-calculated Coordinates.  
COMMENTS:        This trigger occurred at longitude,latitude = 95.42,-9.98 [deg].  
COMMENTS:        The LC_URL file will not be created until ~15 min after the trigger.  
"""

    # Einstein Probe test message (JSON format)
    ep_test_topic = "gcn.notices.einstein_probe.wxt.alert"
    ep_test_value = """{ 
  "$schema": "https://gcn.nasa.gov/schema/v4.3.0/gcn/notices/einstein_probe/wxt/alert.schema.json", 
  "instrument": "WXT", 
  "trigger_time": "2024-03-01T21:46:05.13Z", 
  "id": [ "01708973486" ], 
  "ra": 120, 
  "dec": 40, 
  "ra_dec_error": 0.02, 
  "image_energy_range": [ 0.5, 4 ], 
  "net_count_rate": 1, 
  "image_snr": 1, 
  "additional_info": "The net count rate is derived from an accumulated image (up to 20 min) in 0.5-4 keV, assuming a constant flux. However, it can be significantly lower than the actual count rate of a burst with a duration much shorter than 20 min." 
}"""
    
    logger.info("=== STARTING TEST: Processing test messages ===")
    
    try:
        # Create a mock message object that mimics a Kafka message
        class MockMessage:
            def __init__(self, topic, value):
                self._topic = topic
                self._value = value.encode('utf-8') if isinstance(value, str) else value
                
            def topic(self):
                return self._topic
                
            def value(self):
                return self._value
                
            def error(self):
                return None
                
            def offset(self):
                return 999
        
        # Create and process Fermi test message
        logger.info("Testing Fermi GBM message...")
        fermi_message = MockMessage(fermi_test_topic, fermi_test_value)
        process_notice_and_send_message(
            fermi_message.topic(),
            fermi_message.value(),
            slack_client,
            SLACK_CHANNEL_TEST if SLACK_CHANNEL_TEST else SLACK_CHANNEL,
            is_test=True
        )
        
        # Create and process Einstein Probe test message
        logger.info("Testing Einstein Probe message...")
        ep_message = MockMessage(ep_test_topic, ep_test_value)
        process_notice_and_send_message(
            ep_message.topic(),
            ep_message.value(),
            slack_client,
            SLACK_CHANNEL_TEST if SLACK_CHANNEL_TEST else SLACK_CHANNEL,
            is_test=True
        )
        
        logger.info("=== TEST COMPLETE: Successfully processed test messages ===")
        
    except Exception as e:
        logger.error(f"Error processing test messages: {e}", exc_info=True)

############################## Main Function ##############################
def main():
    """Main function to run the GCN bot."""
    global running
    
    # Start connection monitoring thread
    monitor_thread = Thread(target=check_connection, daemon=True)
    monitor_thread.start()
    
    logger.info("Starting GCN Bot... (Press Ctrl+C to stop)")
    
    # First test Slack connectivity
    try:
        test_response = slack_client.api_test()
        if not test_response["ok"]:
            logger.error(f"Slack authentication failed: {test_response.get('error', 'Unknown error')}")
            logger.warning("Continuing without Slack notifications")
    except Exception as e:
        logger.error(f"Error connecting to Slack: {e}")
        logger.warning("Continuing without Slack notifications")
    
    # If test mode is requested, process test message and exit
    if args.test:
        test_message()
        sys.exit(0)
    
    # Main processing loop
    try:
        while running:
            try:
                # Poll for messages with a 1 second timeout
                for message in consumer.consume(timeout=1):
                    # Exit loop if signaled to stop
                    if not running:
                        break
                    
                    # Check for errors
                    if message.error():
                        error_msg = str(message.error())
                        logger.error(f"Consumer error: {error_msg}")
                        
                        try:
                            slack_client.chat_postMessage(
                                channel=slack_channel,
                                blocks=[
                                    {
                                        "type": "header",
                                        "text": {
                                            "type": "plain_text",
                                            "text": "⚠️ GCN Consumer Error"
                                        }
                                    },
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": f"*Error:* ```{error_msg}```"
                                        }
                                    }
                                ]
                            )
                        except Exception as slack_e:
                            logger.error(f"Error sending error notification to Slack: {slack_e}")
                        continue
                    
                    # Get topic and value
                    topic = message.topic()
                    value = message.value()
                    
                    # Handle heartbeat differently
                    if topic == 'gcn.heartbeat':
                        update_heartbeat()
                        continue
                    
                    # Log message receipt
                    logger.info(f"Received message: topic={topic}, offset={message.offset()}")
                    
                    start_separator = f"{'=' * 20} Starting to process {'=' * 20}"
                    logger.info(start_separator)
                    
                    # Process the notice and send to Slack
                    success, result = process_notice_and_send_message(
                        topic, value, slack_client, slack_channel
                    )
                    
                    if success:
                        end_separator = f"{'=' * 20} Successfully processed {'=' * 20}"
                        logger.info(end_separator)
                    else:
                        end_separator = f"{'=' * 20} Failed to process {'=' * 20}"
                        logger.warning(end_separator)
                
            except Exception as e:
                logger.error(f"Error in message processing: {e}")
                if running:
                    time.sleep(5)  # Wait before retrying
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Critical error in main process: {e}", exc_info=True)
    finally:
        logger.info("Shutting down GCN Bot...")
        consumer.close()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    main()