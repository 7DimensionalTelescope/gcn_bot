#!/usr/bin/env python3
"""
GCN Alert Monitoring System
==========================
A real-time monitoring system for GCN (Gamma-ray Coordinates Network) alerts 
with Slack bot and data storage capabilities.

Basic Information
----------------
Author:         YoungPyo Hong
Created:        2025-01-01
Last Modified:  2025-03-06
Version:        1.3.0
License:        MIT
Copyright:      (c) 2025 YoungPyo Hong

Technical Requirements
---------------------
Dependencies:
    1. Required modules
        - gcn_kafka:     GCN Kafka client library
        - slack_sdk:     Slack API integration
        - time:          Time handling
        - signal:        Signal handling
        - sys:           System-specific parameters and functions
        - json:          JSON parsing and formatting
        - re:            Regular expression operations
        - threading:     Concurrent processing
        - datetime:      Time handling
    2. Optional modules
        - gcn_notice_handler: Save GCN notice data to a CSV file (user module)
        - visibility_plotter: Create visibility plots for Slack (user module)

Key Features
-----------
Real-time Monitoring:
    - Continuous connection status monitoring
    - Heartbeat message tracking
    - Automatic reconnection handling

Data Processing:
    - Classic text format support
    - Modern JSON format support
    - Test notice filtering
    - Redundant data elimination
    - Section-based message formatting
    - Critical information highlighting

Alert Management:
    - Formatted Slack notifications with sections
    - Connection status updates
    - Error handling and reporting
    - CSV and ASCII data storage for GRB events

Usage
-----
Command:
    python gcn_bot.py
    
Configuration:
    1. Connection settings
        - CONNECTION_TIMEOUT: Connection timeout in seconds
    
    2. Slack configuration
        - SLACK_TOKEN:        Slack bot authentication token
        - SLACK_CHANNEL:      Slack channel name
    
    3. GCN configuration
        - GCN_ID:             GCN client identifier
        - GCN_SECRET:         GCN client authentication secret
    
    4. GCNNoticeHandler configuration (optional)
        - TURN_ON_NOTICE:     Enable notice saving
        - OUTPUT_NOTICE_CSV:         CSV file to store the processed notices
        - OUTPUT_ASCII:       ASCII file to store the events

Monitored Facilities
-------------------
Space-based Observatories:
    - Fermi:          Gamma-ray space telescope
        * GBM:        Gamma-ray Burst Monitor
        * LAT:        Large Area Telescope
    - Swift:          Multi-wavelength observatory
        * BAT:        Burst Alert Telescope
        * UVOT:       Ultraviolet/Optical Telescope
        * XRT:        X-Ray Telescope
    - Einstein Probe: Wide-field X-ray telescope

Ground-based Facilities:
    - IceCube:        Neutrino detector
    - HAWC:           High-Altitude Water Cherenkov Gamma-Ray Observatory

Multi-messenger Programs:
    - AMON:           Astrophysical Multimessenger Observatory Network

Message Format
------------
Notices are formatted in a structured way with these sections:
    - [BASIC INFO]:    Title, notice date, notice type, trigger information
    - [LOCATION]:      Coordinates (RA/Dec), error radius
    - [TIMING]:        Discovery date/time or trigger time
    - [ANALYSIS]:      Energy, significance, intensity values, etc.
    - [COMMENTS]:      Original comments with bullet points
    - [ADDITIONAL INFO]: Extra facility-specific information

Important decision-making information is highlighted in bold.

Change Log
----------
1.3.0 / 2025-03-06
    - Implemented section-based message formatting
    - Added bold highlighting for critical information
    - Removed unnecessary message content
    - Added facility name to message headers
    - Improved JSON notice formatting

1.2.1 / 2025-02-05
    - Added logging for better error handling

1.2.0 / 2025-01-31
    - Enhanced message handling system
        * Added '_format_json_message' function for better JSON formatting
        * Improved structured message formatting for Einstein Probe and IceCube
    - Improved error handling and notifications
        * Added detailed consumer error notifications to Slack
        * Enhanced connection status messages with detailed timestamps
    - Enhanced Slack message formatting
        * Restructured message blocks for better readability
        * Added status tracking in message footer
    - Modified configuration settings
        * Removed unused time window parameter
        * Updated documentation to reflect current functionality

1.1.1 / 2025-01-23: 
    - Added configuration setting
    - Added information of gcn_notice_handler in the documentation

1.1.0 / 2025-01-21: 
    - Integrated GCNNoticeHandler for GRB data processing and storage

1.0.3 / 2025-01-19: 
    - Added '_get_facility_name' function for extracting facility name from topic
    - Modified 'format_message_for_slack' function to handle JSON format

1.0.2 / 2025-01-08: 
    - Added JSON format message handling
    - Enhancing GCN server status monitoring using 'gcn.heartbeat'
    - Organized the comments of topics

1.0.1 / 2025-01-05: 
    - Implemented connection monitoring

1.0.0 / 2025-01-01:
    - Initial version release
"""
from io import BytesIO
from datetime import datetime, timezone
from gcn_kafka import Consumer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
import signal
import os
import argparse
import sys
import logging
import json
import re
import pytz
import random
from typing import Dict, Tuple, Any, Optional, Union, List
from threading import Thread, Lock
from datetime import datetime

############################## Logging ############################
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gcn_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('kafka').setLevel(logging.ERROR)

############################## Imports ############################
# Try to import visibility_plotter
try:
    from supy.supy.observer.visibility_plotter import VisibilityPlotter
    # Initialize the plotter
    plotter = VisibilityPlotter(logger=logger)
    visibility_available = True
except ImportError as e:
    logger.warning(f"visibility_plotter module not available. Visibility plots will be disabled. Error: {e}")
    visibility_available = False

# Import notice handler
from gcn_notice_handler import GCNNoticeHandler

# Import ToO emailer
from gcn_too_emailer import GCNToOEmailer

# Import ToO integration
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import threading

############################## Configuration ############################
# Configuration class
class Config:
    """Configuration class with defaults and override capability."""
    
    # Default configuration values
    CONNECTION_TIMEOUT = 300
    SLACK_TOKEN = "your_slack_bot_token"
    SLACK_CHANNEL = "your_slack_channel"
    GCN_ID = 'your_gcn_client_id'
    GCN_SECRET = 'your_gcn_client_secret'
    MIN_ALTITUDE = 30
    MIN_MOON_SEP = 30
    TURN_ON_NOTICE = True
    OUTPUT_CSV = 'gcn_notices.csv'
    OUTPUT_ASCII = 'grb_targets.ascii'
    ASCII_MAX_EVENTS = 10
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
    TOO_USER_GROUP = "too-operators"  # Slack user group for authorized users
    SLACK_APP_TOKEN = "xapp-your-app-token"  # Socket mode app token
    
    def __init__(self):
        """Initialize config and try to load from config.py file."""
        self._load_config_file()
    
    def _load_config_file(self):
        """Load configuration from config.py file if it exists."""
        try:
            import config
            
            # Override defaults with values from config.py
            for attr_name in dir(config):
                if not attr_name.startswith('_'):  # Skip private attributes
                    setattr(self, attr_name, getattr(config, attr_name))
            
            logger.info("Configuration loaded successfully from config.py")
            
        except ImportError:
            logger.error("Configuration file 'config.py' not found.")
            logger.error("Please create a config.py file based on config_template.py.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)

# Initialize global config object
config = Config()

# Configuration variables
CONNECTION_TIMEOUT = config.CONNECTION_TIMEOUT
MIN_ALTITUDE = config.MIN_ALTITUDE
MIN_MOON_SEP = config.MIN_MOON_SEP
TURN_ON_NOTICE = config.TURN_ON_NOTICE
TURN_ON_TOO_EMAIL = config.TURN_ON_TOO_EMAIL
EMAIL_FROM = config.EMAIL_FROM
EMAIL_PASSWORD = config.EMAIL_PASSWORD
TOO_CONFIG = config.TOO_CONFIG
SLACK_TOKEN = config.SLACK_TOKEN
SLACK_CHANNEL = config.SLACK_CHANNEL
GCN_ID = config.GCN_ID
GCN_SECRET = config.GCN_SECRET
OUTPUT_CSV = config.OUTPUT_CSV
OUTPUT_ASCII = config.OUTPUT_ASCII
ASCII_MAX_EVENTS = config.ASCII_MAX_EVENTS
DISPLAY_TOPICS = config.DISPLAY_TOPICS
TOO_USER_GROUP = config.TOO_USER_GROUP
SLACK_APP_TOKEN = config.SLACK_APP_TOKEN

############################## Global Flags and Variables ############################
# Global flags and variables
running = True
last_heartbeat = datetime.now()
heartbeat_lock = Lock()
last_connection_status = True  # True = connected, False = disconnected
socket_handler = None
os.environ["NUMEXPR_MAX_THREADS"] = "4"
reconnect_attempts = 0
max_reconnect_attempts = 5
consumer_lock = Lock()  # For thread-safe consumer replacement

############################## Slack ToO Integration ############################

class SlackToOIntegration:
    """
    Handles ToO (Target of Opportunity) integration with Slack.
    
    This class manages:
    - User authorization checking
    - ToO button addition to messages
    - Modal form creation and handling
    - Form submission processing
    """
    
    def __init__(self, slack_client: WebClient, user_group: str):
        """
        Initialize ToO integration.
        
        Args:
            slack_client: Slack WebClient instance
            user_group: Name of authorized user group (e.g., 'too-operators')
        """
        self.slack_client = slack_client
        self.user_group = user_group
        logger.info(f"ToO Integration initialized with user group: {user_group}")
    
    def is_user_authorized(self, user_id: str) -> bool:
        """Check if user is authorized to submit ToO requests."""
        try:
            response = self.slack_client.usergroups_list()
            
            # Find the ToO user group
            too_group = None
            for group in response['usergroups']:
                if group['handle'] == self.user_group:
                    too_group = group
                    break
            
            if not too_group:
                logger.warning(f"ToO user group '{self.user_group}' not found")
                return False
            
            # Get members of the ToO group
            members_response = self.slack_client.usergroups_users_list(
                usergroup=too_group['id']
            )
            
            authorized_users = members_response['users']
            is_authorized = user_id in authorized_users
            
            logger.info(f"User {user_id} authorization check: {is_authorized}")
            return is_authorized
            
        except SlackApiError as e:
            logger.error(f"Error checking user authorization: {e}")
            return False
    
    def get_user_email(self, user_id: str) -> Optional[str]:
        """Get user's email address from Slack profile."""
        try:
            user_info = self.slack_client.users_info(user=user_id)
            email = user_info['user']['profile'].get('email')
            
            if email:
                logger.info(f"Retrieved email for user {user_id}")
                return email
            else:
                logger.warning(f"No email found for user {user_id}")
                return None
                
        except SlackApiError as e:
            logger.error(f"Error getting user email: {e}")
            return None
    
    def get_user_display_name(self, user_id: str) -> str:
        """Get user's display name from Slack profile."""
        try:
            user_info = self.slack_client.users_info(user=user_id)
            display_name = (
                user_info['user']['profile'].get('display_name') or 
                user_info['user']['profile'].get('real_name') or 
                user_info['user']['name']
            )
            return display_name
            
        except SlackApiError as e:
            logger.error(f"Error getting user display name: {e}")
            return "Unknown User"
    
    def add_too_button_to_message(self, blocks: List[Dict[str, Any]], notice_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Add ToO request button to existing GRB alert message."""
        
        try:
            # Create simple button data with essential fields
            button_data = {
                "Name": str(notice_data.get('Name', '')),
                "RA": str(notice_data.get('RA', '')),
                "DEC": str(notice_data.get('DEC', '')),
                "Facility": str(notice_data.get('Facility', '')),
                "Trigger_num": str(notice_data.get('Trigger_num', ''))
            }
            
            # Remove empty values
            button_data = {k: v for k, v in button_data.items() if v and v != 'nan'}
            
            # Test JSON serialization
            json_string = json.dumps(button_data, ensure_ascii=True)
            
            # Check Slack button value length limit
            if len(json_string) > 2000:
                logger.warning(f"Button data too long ({len(json_string)} chars), using minimal version")
                minimal_data = {
                    "Name": str(notice_data.get('Name', '')),
                    "RA": str(notice_data.get('RA', '')),
                    "DEC": str(notice_data.get('DEC', ''))
                }
                json_string = json.dumps(minimal_data, ensure_ascii=True)
            
            # Create ToO button block
            too_button_block = {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📧 Submit ToO Request",
                            "emoji": True
                        },
                        "style": "primary",
                        "action_id": "submit_too_request",
                        "value": json_string
                    }
                ]
            }
            
            # Add divider and button
            enhanced_blocks = blocks.copy()
            enhanced_blocks.append({"type": "divider"})
            enhanced_blocks.append(too_button_block)
            
            logger.info("ToO button added successfully")
            return enhanced_blocks
            
        except Exception as e:
            logger.error(f"Failed to add ToO button: {e}")
            # Return original blocks without button if there's any error
            return blocks
    
    def create_too_modal(self, trigger_id: str, user_email: str, notice_data: Dict[str, Any]) -> bool:
        """Create and open ToO request modal form with pre-populated data."""
        
        # DEBUG: Log what we received
        logger.debug(f"🔍 DEBUG - Modal received notice_data: {notice_data}")
        
        # Extract values (notice_data should now be clean strings/numbers)
        target_name = str(notice_data.get('Name', ''))
        ra_value = str(notice_data.get('RA', ''))
        dec_value = str(notice_data.get('DEC', ''))
        
        # Debug logging to check what data we're using
        logger.debug(f"🔍 DEBUG - Extracted values: Target='{target_name}', RA='{ra_value}', DEC='{dec_value}'")
        
        # Create modal view with pre-populated fields
        modal_view = {
            "type": "modal",
            "callback_id": "too_request_modal",
            "title": {
                "type": "plain_text",
                "text": "ToO Request Form"
            },
            "submit": {
                "type": "plain_text",
                "text": "Submit Request"
            },
            "close": {
                "type": "plain_text",
                "text": "Cancel"
            },
            "blocks": [
                # Header
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Target of Opportunity Request*\n*Target:* {target_name or 'Not specified'}"
                    }
                },
                {"type": "divider"},
                
                # Requester email
                {
                    "type": "input",
                    "block_id": "requester_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "requester_input",
                        "initial_value": user_email,
                        "placeholder": {
                            "type": "plain_text",
                            "text": "your.email@example.com"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Requester Email *"
                    }
                },
                
                # Target name - NOW WITH INITIAL VALUE
                {
                    "type": "input",
                    "block_id": "target_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "target_input",
                        "initial_value": target_name,  # ✅ Pre-populated from notice
                        "placeholder": {
                            "type": "plain_text",
                            "text": "GRB240101A"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Target Name *"
                    }
                },
                
                # Right Ascension (RA) - NOW WITH INITIAL VALUE
                {
                    "type": "input",
                    "block_id": "ra_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "ra_input",
                        "initial_value": ra_value,  # ✅ Pre-populated from notice
                        "placeholder": {
                            "type": "plain_text",
                            "text": "150.1234 (degrees)"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Right Ascension (RA) *"
                    }
                },
                
                # Declination (DEC) - NOW WITH INITIAL VALUE
                {
                    "type": "input",
                    "block_id": "dec_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "dec_input",
                        "initial_value": dec_value,  # ✅ Pre-populated from notice
                        "placeholder": {
                            "type": "plain_text",
                            "text": "-25.5678 (degrees)"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Declination (DEC) *"
                    }
                },
                
                {"type": "divider"},
                
                # Exposure time
                {
                    "type": "input",
                    "block_id": "exposure_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "exposure_input",
                        "initial_value": "100",
                        "placeholder": {
                            "type": "plain_text",
                            "text": "100"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Single Exposure Time (seconds) *"
                    }
                },
                
                # Image count
                {
                    "type": "input",
                    "block_id": "count_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "count_input",
                        "initial_value": "3",
                        "placeholder": {
                            "type": "plain_text",
                            "text": "3"
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Number of Images *"
                    }
                },
                
                # Observation mode
                {
                    "type": "input",
                    "block_id": "obsmode_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "obsmode_input",
                        "initial_option": {
                            "text": {"type": "plain_text", "text": "Spec"},
                            "value": "Spec"
                        },
                        "options": [
                            {"text": {"type": "plain_text", "text": "Deep"}, "value": "Deep"},
                            {"text": {"type": "plain_text", "text": "Spec"}, "value": "Spec"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Observation Mode *"
                    }
                },
                
                # Filters selection
                {
                    "type": "input",
                    "block_id": "filters_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "filters_input",
                        "initial_options": [
                            {"text": {"type": "plain_text", "text": "g"}, "value": "g"},    
                            {"text": {"type": "plain_text", "text": "r"}, "value": "r"},
                            {"text": {"type": "plain_text", "text": "i"}, "value": "i"}
                        ],
                        "options": [
                            {"text": {"type": "plain_text", "text": "u"}, "value": "u"},
                            {"text": {"type": "plain_text", "text": "g"}, "value": "g"},
                            {"text": {"type": "plain_text", "text": "r"}, "value": "r"},
                            {"text": {"type": "plain_text", "text": "i"}, "value": "i"},
                            {"text": {"type": "plain_text", "text": "z"}, "value": "z"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Filters *"
                    }
                },
                
                {"type": "divider"},
                
                # Priority
                {
                    "type": "input",
                    "block_id": "priority_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "priority_input",
                        "initial_option": {
                            "text": {"type": "plain_text", "text": "High"},
                            "value": "High"
                        },
                        "options": [
                            {"text": {"type": "plain_text", "text": "Low"}, "value": "Low"},
                            {"text": {"type": "plain_text", "text": "Medium"}, "value": "Medium"},
                            {"text": {"type": "plain_text", "text": "High"}, "value": "High"},
                            {"text": {"type": "plain_text", "text": "Urgent"}, "value": "Urgent"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Priority *"
                    }
                },
                
                # Binning
                {
                    "type": "input",
                    "block_id": "binning_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "binning_input",
                        "initial_option": {
                            "text": {"type": "plain_text", "text": "1"},
                            "value": "1"
                        },
                        "options": [
                            {"text": {"type": "plain_text", "text": "1"}, "value": "1"},
                            {"text": {"type": "plain_text", "text": "2"}, "value": "2"},
                            {"text": {"type": "plain_text", "text": "3"}, "value": "3"},
                            {"text": {"type": "plain_text", "text": "4"}, "value": "4"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Binning *"
                    }
                },
                
                # Gain
                {
                    "type": "input",
                    "block_id": "gain_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "gain_input",
                        "initial_option": {
                            "text": {"type": "plain_text", "text": "High"},
                            "value": "High"
                        },
                        "options": [
                            {"text": {"type": "plain_text", "text": "Low"}, "value": "Low"},
                            {"text": {"type": "plain_text", "text": "Medium"}, "value": "Medium"},
                            {"text": {"type": "plain_text", "text": "High"}, "value": "High"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Gain *"
                    }
                },
                
                # Abort current observation
                {
                    "type": "input",
                    "block_id": "abort_block",
                    "element": {
                        "type": "radio_buttons",
                        "action_id": "abort_input",
                        "initial_option": {
                            "text": {"type": "plain_text", "text": "No"},
                            "value": "No"
                        },
                        "options": [
                            {"text": {"type": "plain_text", "text": "Yes"}, "value": "Yes"},
                            {"text": {"type": "plain_text", "text": "No"}, "value": "No"}
                        ]
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Abort Current Observation *"
                    }
                },
                
                {"type": "divider"},
                
                # Comments - PRE-POPULATED WITH TARGET NAME
                {
                    "type": "input",
                    "block_id": "comments_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "comments_input",
                        "multiline": True,
                        "initial_value": f"Submitted via Slack for {target_name}" if target_name else "Submitted via Slack",
                        "placeholder": {
                            "type": "plain_text",
                            "text": "Additional comments or special instructions..."
                        }
                    },
                    "label": {
                        "type": "plain_text",
                        "text": "Comments"
                    },
                    "optional": True
                }
            ],
            "private_metadata": json.dumps(notice_data)
        }
        
        try:
            response = self.slack_client.views_open(
                trigger_id=trigger_id,
                view=modal_view
            )
            logger.info(f"ToO modal opened successfully with pre-populated data")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error opening ToO modal: {e}")
            return False
    
    def handle_unauthorized_access(self, channel_id: str, user_id: str, thread_ts: Optional[str] = None) -> bool:
        """Handle unauthorized user attempting to submit ToO request."""
        try:
            error_message = (
                "❌ *Access Denied*\n\n"
                f"You must be a member of @{self.user_group} to submit ToO requests.\n"
                "Please contact the system administrator to request access."
            )
            
            self.slack_client.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text=error_message,
                thread_ts=thread_ts
            )
            
            logger.warning(f"Unauthorized ToO request attempt by user {user_id}")
            return True
            
        except SlackApiError as e:
            logger.error(f"Error sending unauthorized access message: {e}")
            return False
    
    def extract_form_data(self, form_values: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format form data from Slack modal submission."""
        try:
            # Extract all form fields
            form_data = {
                'requester': form_values['requester_block']['requester_input']['value'],
                'target': form_values['target_block']['target_input']['value'],
                'ra': form_values['ra_block']['ra_input']['value'],
                'dec': form_values['dec_block']['dec_input']['value'],
                'exposure': form_values['exposure_block']['exposure_input']['value'],
                'imageCount': form_values['count_block']['count_input']['value'],
                'obsmode': form_values['obsmode_block']['obsmode_input']['selected_option']['value'],
                'priority': form_values['priority_block']['priority_input']['selected_option']['value'],
                'binning': form_values['binning_block']['binning_input']['selected_option']['value'],
                'gain': form_values['gain_block']['gain_input']['selected_option']['value'],
                'abortObservation': form_values['abort_block']['abort_input']['selected_option']['value'],
                'comments': form_values['comments_block']['comments_input']['value']
            }
            
            # Extract filters if selected
            filters_options = form_values.get('filters_block', {}).get('filters_input', {}).get('selected_options', [])
            form_data['selectedFilters'] = [opt['value'] for opt in filters_options]
            
            # Add computed fields
            form_data['totalExposureTime'] = int(form_data['exposure']) * int(form_data['imageCount'])
            form_data['is_ToO'] = form_data['abortObservation'] == 'Yes'
            
            return form_data
            
        except Exception as e:
            logger.error(f"Error extracting form data: {e}")
            return {}
    
    def convert_slack_form_to_email_data(self, form_data: Dict[str, Any], 
                                    user_name: str, user_email: str,
                                    notice_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced version of convert_slack_form_to_email_data method for SlackToOIntegration class.
        
        This function converts Slack modal form data to the exact format expected by 
        GCNToOEmailer.send_too_email() method.
        
        Args:
            form_data: Data extracted from Slack modal submission
            user_name: Display name of user who submitted form
            user_email: Email address of user who submitted form
            notice_data: Original GCN notice data
            
        Returns:
            Dictionary formatted for GCNToOEmailer.send_too_email()
        """
        try:
            # Get current timestamp for submission
            submission_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            
            # Create email data structure expected by GCNToOEmailer
            email_data = {
                # User and submission info
                'requester': user_email,
                'submitter_name': user_name,
                'submission_time': submission_time,
                'submitted_via': 'Slack Bot',
                
                # Target information (required fields)
                'target': form_data['target'],
                'ra': form_data['ra'],
                'dec': form_data['dec'],
                
                # Observation parameters
                'exposure': int(form_data['exposure']) * int(form_data['imageCount']),  # Total exposure
                'singleExposure': int(form_data['exposure']),  # Single exposure time
                'imageCount': int(form_data['imageCount']),
                'obsmode': form_data['obsmode'],
                'selectedFilters': form_data['selectedFilters'],
                'priority': form_data['priority'],
                'binning': form_data['binning'],
                'gain': form_data['gain'],
                'abortObservation': form_data['abortObservation'],
                
                # Additional fields for tcspy compatibility
                'selectedTelNumber': 1,  # Default to 1 telescope
                'radius': '0',  # Default radius
                'obsStartTime': 'ASAP',  # Immediate start
                'is_ToO': True,  # Always true for ToO requests
                
                # Include original notice info if available
                'original_facility': notice_data.get('Facility', 'Unknown'),
                'original_trigger': notice_data.get('Trigger_num', 'Unknown'),
                'gcn_notice_type': notice_data.get('Notice_type', 'Unknown')
            }
            
            # Handle comments with Slack-specific information
            base_comments = form_data.get('comments', '')
            slack_info = f"Submitted via Slack Bot by {user_name} ({user_email}) at {submission_time}"
            
            if base_comments:
                email_data['comments'] = f"{base_comments}\n\n--- Submission Info ---\n{slack_info}"
            else:
                email_data['comments'] = slack_info
                
            # Validate required fields
            required_fields = ['target', 'ra', 'dec', 'requester']
            missing_fields = [field for field in required_fields if not email_data.get(field)]
            
            if missing_fields:
                logger.error(f"Missing required fields in email data: {missing_fields}")
                return {}
                
            logger.info(f"Successfully converted Slack form data to email format for target: {email_data['target']}")
            return email_data
            
        except Exception as e:
            logger.error(f"Error converting Slack form data to email format: {e}")
            return {}

############################## Initialize Clients ############################
# Initialize Slack client
slack_client = WebClient(token=SLACK_TOKEN)

# Initialize Slack Bolt app and ToO integration
app = None
too_integration = None
socket_handler = None

if SLACK_APP_TOKEN:
    try:
        app = App(token=SLACK_TOKEN)
        too_integration = SlackToOIntegration(slack_client, TOO_USER_GROUP)
        logger.info("Slack Bolt app and ToO integration initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Slack Bolt app: {e}")
        app = None
        too_integration = None
else:
    logger.warning("SLACK_APP_TOKEN not configured - ToO integration disabled")

# Initialize GCN consumer
consumer = Consumer(
    client_id=GCN_ID,
    client_secret=GCN_SECRET
)

# Initialize notice handler
notice_handler = GCNNoticeHandler(
    output_csv=OUTPUT_CSV,
    output_ascii=OUTPUT_ASCII,
    ascii_max_events=ASCII_MAX_EVENTS
)

############################## Initialize argument parser ############################
# Add argument parser
parser = argparse.ArgumentParser(description='GCN Alert Monitor')
parser.add_argument('--send', action='store_true', help='Actually send test messages to Slack')
args = parser.parse_args()

# Set TEST_SEND_TO_SLACK based on args
TEST_SEND_TO_SLACK = args.send

############################## Signal Handling ############################
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Received shutdown signal")
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

############################## Facility and Topic Management ############################
# Subscribe to all topics including heartbeat
all_topics = DISPLAY_TOPICS + ['gcn.heartbeat']
consumer.subscribe(all_topics)

############################## Message Formatting ##############################
def _filter_notice_text(text, topic):
    """
    Filter out unnecessary information from notice text based on topic.
    
    Args:
        text (str): Original notice text, may be bytes or string
        topic (str): Topic of the notice
    
    Returns:
        tuple: (formatted_text, lc_url) - formatted text and LC URL
    """
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    
    # Split text into lines
    lines = text.splitlines()
    
    # Common patterns to exclude for all facilities
    exclude_patterns = [
        r"RECORD_NUM",
        r"SUN_POSTN",
        r"SUN_DIST",
        r"MOON_POSTN", 
        r"MOON_DIST",
        r"MOON_ILLUM",
        r"GAL_COORDS",
        r"ECL_COORDS",
        r"AMP[0-3]",
        r"WAVEFORM",
        r"TAM\[0-3\]",
        r"PKT_SER_NUM",
        r"PKT_HOP_CNT",
        r"PKT_SOD",
        r"RETRACTION",
        r"SC_LON_LAT",
        r"LC_URL",
        r"LOC_URL",
        r"SKYMAP_\w+_URL",
        r"GRB_PHI",
        r"GRB_THETA",
        r"SOLN",
        r"MERIT_PARAMS",
        r"BKG_INTEN",
        r"BKG_TIME",
        r"BKG_DUR",
        r"LOC_ALGORITHM",
        r"E_RANGE",
        r"POS_MAP_URL",
        r"DATA_INTERVAL",
        r"AMPLIFIER",
        r"COMMENTS:",
        r"TRIGGER_INDEX",
        r"TITLE",
        r"SRC_ERROR50",
        r"REVISION"
    ]
    
    # Store sections for structured output
    sections = {
        "basic": [],
        "location": [],
        "timing": [],
        "analysis": []
    }
    
    current_section = "basic"
    combined_date_time = None
    
    # Extract notice date and trigger time for standardization
    notice_date = None
    trigger_time = None
    notice_date_line = None
    trigger_time_line = None
    lc_url = None
    
    # First pass to extract key information for processing
    for line in lines:
        if "NOTICE_DATE:" in line:
            match = re.search(r"NOTICE_DATE:\s*(.+)", line)
            if match:
                notice_date = match.group(1)
                notice_date_line = line
        
        # Check for various time fields from different facilities
        # Format with curly braces: TIME: NNNNN SOD {HH:MM:SS.ss} UT
        time_with_braces = re.search(r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_TIME:.*?{([\d:\.]+)}\s*UT", line)
        if time_with_braces:
            trigger_time = time_with_braces.group(1)
            trigger_time_line = line
            continue  # Skip to next iteration if found
        
        # Format without braces: TIME: HH:MM:SS.ss UT
        time_without_braces = re.search(r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_TIME:\s*([\d:\.]+)\s*UT", line)
        if time_without_braces:
            trigger_time = time_without_braces.group(1)
            trigger_time_line = line
            continue  # Skip to next iteration if found
        
        # Extract LC_URL if available
        if "LC_URL:" in line:
            match = re.search(r"LC_URL:\s*([^\s]+)", line)
            if match:
                lc_url = match.group(1)

    # Process each line
    for line in lines:
        # Skip lines matching exclude patterns
        if any(re.search(pattern, line) for pattern in exclude_patterns):
            continue
        
        # Skip (current) and (1950) coordinate lines
        if "current" in line or "1950" in line:
            continue
            
        # For coordinates, only keep J2000 values
        if any(coord_type in line for coord_type in ["GRB_RA", "GRB_DEC", "SRC_RA", "SRC_DEC", "POINT_RA", "POINT_DEC"]):
            if "J2000" not in line:
                continue
        
        # Handle NOTICE_DATE with standardized format - improved parsing
        if line == notice_date_line and notice_date:
            standardized_notice_date = _standardize_time_format(notice_date)
            sections["basic"].append(f"NOTICE_DATE:      {standardized_notice_date}")
            continue
        
        # Handle date/time combination
        if any(date_type in line for date_type in ["GRB_DATE:", "TRIGGER_DATE:", "DISCOVERY_DATE:", "EVENT_DATE:", "IMG_START_DATE:"]):
            date_match = re.search(r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_DATE:.*?(\d{2})/(\d{2})/(\d{2})", line)
            if date_match:
                combined_date_time = f'{date_match.group(1)}/{date_match.group(2)}/{date_match.group(3)}'
            continue
        
        # Handle trigger time standardization and add time difference
        if line == trigger_time_line and trigger_time and notice_date:
            # Create full datetime string
            if combined_date_time:  # We have a date from GRB_DATE
                full_trigger_time = f"{combined_date_time} {trigger_time}"
            else:
                # Use current date if no GRB_DATE found
                today = datetime.now().strftime("%y/%m/%d")
                full_trigger_time = f"{today} {trigger_time}"
            
            # Add standardized trigger time
            standardized_trigger = _standardize_time_format(full_trigger_time)
            
            # Handle different time field names
            if "GRB_TIME:" in line:
                sections["timing"].append(f"GRB_TIME:      {standardized_trigger}")
            elif "TRIGGER_TIME:" in line:
                sections["timing"].append(f"TRIGGER_TIME:      {standardized_trigger}")
            elif "DISCOVERY_TIME:" in line:
                sections["timing"].append(f"DISCOVERY_TIME:      {standardized_trigger}")
            elif "EVENT_TIME:" in line:
                sections["timing"].append(f"EVENT_TIME:      {standardized_trigger}")
            elif "IMG_START_TIME:" in line:
                sections["timing"].append(f"IMG_START_TIME:      {standardized_trigger}")
            else:
                # Extract the time field name for other formats
                time_field_match = re.search(r"(\w+_TIME):", line)
                if time_field_match:
                    time_field = time_field_match.group(1)
                    sections["timing"].append(f"{time_field}:      {standardized_trigger}")
                else:
                    sections["timing"].append(f"TIME:      {standardized_trigger}")
            
            # Add time difference with corresponding field name
            time_diff = _calculate_time_diff(notice_date, full_trigger_time)
            if "GRB_TIME:" in line:
                sections["timing"].append(f"GRB_TIME_DIFF:      {time_diff}")
            elif "TRIGGER_TIME:" in line:
                sections["timing"].append(f"TRIGGER_TIME_DIFF:      {time_diff}")
            elif "DISCOVERY_TIME:" in line:
                sections["timing"].append(f"DISCOVERY_TIME_DIFF:      {time_diff}")
            elif "EVENT_TIME:" in line:
                sections["timing"].append(f"EVENT_TIME_DIFF:      {time_diff}")
            elif "IMG_START_TIME:" in line:
                sections["timing"].append(f"IMG_START_TIME_DIFF:      {time_diff}")
            else:
                # Extract the time field name for other formats
                time_field_match = re.search(r"(\w+_TIME):", line)
                if time_field_match:
                    time_field = time_field_match.group(1)
                    sections["timing"].append(f"{time_field}_DIFF:      {time_diff}")
                else:
                    sections["timing"].append(f"TIME_DIFF:      {time_diff}")
            
            combined_date_time = None  # Reset for next date
            continue
        
        # Determine line section
        if any(x in line for x in ["NOTICE_DATE:", "TRIGGER_NUM:", "EVENT_NUM:", "RUN_NUM:", "STREAM:", "ID:"]):
            current_section = "basic"
        elif any(x in line for x in ["GRB_RA:", "GRB_DEC:", "SRC_RA:", "SRC_DEC:", "POINT_RA:", "POINT_DEC:", "RA:", "DEC:", "GRB_ERROR:", "SRC_ERROR", "RA_DEC_ERROR"]):
            current_section = "location" 
        elif any(x in line for x in ["TRIGGER_DATE:", "TRIGGER_TIME:", "DISCOVERY_DATE:", "DISCOVERY_TIME:", "IMG_START_DATE:", "IMG_START_TIME:", "GRB_DATE:", "GRB_TIME:"]):
            current_section = "timing"
        elif any(x in line for x in ["ENERGY:", "SIGNALNESS:", "FAR:", "COINCIDENCE:", "SIGNIFICANCE:", "DELTA_T:", "COINC_PAIR:", "GRB_INTEN:", "GRB_SIGNIF:", "GRB_MAG:", "RATE_SIGNIF:", "IMAGE_SIGNIF:", "CHARGE:", "IMAGE_SNR:", "SNR:", "TRIGGER_DUR:", "LC_URL:"]):
            current_section = "analysis"
        
        # Store line in appropriate section
        if line.strip():
            sections[current_section].append(line)
    
    # Build formatted text with sections
    formatted_text = ""
    
    # Basic info section
    if sections["basic"]:
        formatted_text += "*[BASIC INFO]*\n> " + "\n> ".join(sections["basic"]) + "\n\n"
    
    # Location section
    if sections["location"]:
        formatted_text += "*[LOCATION]*\n> " + "\n> ".join(sections["location"]) + "\n\n"
    
    # Timing section
    if sections["timing"]:
        formatted_text += "*[TIMING]*\n> " + "\n> ".join(sections["timing"]) + "\n\n"
    
    # Analysis section
    if sections["analysis"]:
        formatted_text += "*[ANALYSIS]*\n> " + "\n> ".join(sections["analysis"]) + "\n\n"
    
    # Bold entire lines with key field names
    logger.debug(f"Processing line formatting for notice text")
    
    lines = formatted_text.split('\n')
    key_fields = [
        # Location fields
        'GRB_RA:', 'SRC_RA:', 'POINT_RA:', 'RA:',
        'GRB_DEC:', 'SRC_DEC:', 'POINT_DEC:', 'DEC:',
        # 'GRB_ERROR:', 'SRC_ERROR:', 'SRC_ERROR90:', 'SRC_ERROR50:', 'RA_DEC_ERROR:',
        
        # Timing fields
        'TRIGGER_TIME:', 'GRB_TIME:', 'GRB_DATETIME:', 'DISCOVERY_TIME:', 'IMG_START_TIME:',
        'TRIGGER_DATE:', 'DISCOVERY_DATE:', 'IMG_START_DATE:',
        'GRB_TIME_DIFF:', 'TRIGGER_TIME_DIFF:', 'EVENT_TIME_DIFF:',
        
        # Analysis fields
        'ENERGY:', 'SIGNALNESS:', 'FAR:', 'COINC_PAIR:', 'PVALUE:',
        'SIGNIFICANCE:', 'GRB_INTEN:', 'GRB_MAG:', 'GRB_SIGNIF:', 'DATA_SIGNIF:', 'IMAGE_SIGNIF:',
        'IMAGE_SNR:', 'SNR:', 'CHARGE:', 'NET_COUNT_RATE:',
        'DELTA_T:', 'SIGMA_T:', 'SIGNAL_TRACKNESS:',
        'TRIGGER_DUR:',
        
        # Basic info fields
        'NOTICE_DATE:', 'TRIGGER_NUM:', 'EVENT_NUM:', 'ID:'
    ]

    for i, line in enumerate(lines):
        # Skip section headers which are already bold
        if '*[BASIC INFO]*' in line or '*[LOCATION]*' in line or '*[TIMING]*' in line or \
           '*[ANALYSIS]*' in line:
            continue
        
        # Handle both '> ' and '>' blockquote formats
        if (line.startswith('> ') or line.startswith('>')) and any(key in line for key in key_fields):
            # Standardize the line format before adding bold
            if line.startswith('> '):
                line_content = line[2:]  # Remove '> '
            else:
                line_content = line[1:]  # Remove '>'
                
            # Make the entire line bold and add back blockquote format
            lines[i] = '> *' + line_content + '*'
            logger.debug(f"Made line bold: {lines[i]}")
    
    formatted_text = '\n'.join(lines)

    # Special terms in comments should still be bolded
    formatted_text = re.sub(r"(Long GRB|long GRB)", r"*\1*", formatted_text)
    formatted_text = re.sub(r"(Short GRB|short GRB)", r"*\1*", formatted_text)
    formatted_text = re.sub(r"(likely a GRB)", r"*\1*", formatted_text)
    
    # Store LC_URL for later use (return along with formatted text)
    return formatted_text.strip(), lc_url

def _format_json_notice(json_data, facility):
    """
    Format JSON notice with only necessary fields, organizing into appropriate sections.
    
    Args:
        json_data (dict): JSON data
        facility (str): Facility name
    
    Returns:
        formatted_text (str): Formatted notice text
    """
    # Initialize sections (removed comments section)
    sections = {
        "basic": [],
        "location": [],
        "timing": [],
        "analysis": []
    }
    
    # Track notice date and trigger time for time difference calculation
    notice_date = datetime.now().strftime("%a %d %b %y %H:%M:%S")
    trigger_time = None
    
    # Basic Info section
    sections["basic"].append(f"NOTICE_DATE:      {_standardize_time_format(notice_date)}")
    
    # Handle Einstein Probe specific fields
    if 'EINSTEIN_PROBE' in facility:
        # Add ID field to basic info if available
        if 'id' in json_data:
            if isinstance(json_data['id'], list) and json_data['id']:
                sections["basic"].append(f"ID:      {json_data['id'][0]}")
            else:
                sections["basic"].append(f"ID:      {json_data['id']}")
        
        # Location section
        if 'ra' in json_data:
            sections["location"].append(f"RA:      {json_data['ra']}")
        if 'dec' in json_data:
            sections["location"].append(f"DEC:      {json_data['dec']}")
        if 'ra_dec_error' in json_data:
            sections["location"].append(f"RA_DEC_ERROR:      {json_data['ra_dec_error']}")
        
        # Timing section
        if 'trigger_time' in json_data:
            trigger_time = json_data['trigger_time']
            time_str = _standardize_time_format(trigger_time)
            sections["timing"].append(f"TRIGGER_TIME:      {time_str}")
            
            # Add time difference
            time_diff = _calculate_time_diff(notice_date, trigger_time)
            sections["timing"].append(f"TRIGGER_TIME_DIFF:      {time_diff}")
            
        # Analysis section
        if 'image_energy_range' in json_data:
            sections["analysis"].append(f"IMAGE_ENERGY_RANGE:      {json_data['image_energy_range']}")
        if 'net_count_rate' in json_data:
            sections["analysis"].append(f"NET_COUNT_RATE:      {json_data['net_count_rate']}")
        if 'image_snr' in json_data:
            sections["analysis"].append(f"IMAGE_SNR:      {json_data['image_snr']}")
    
    # For other JSON formats, extract key fields in a similar manner
    else:
        # Generic extraction of important fields
        remaining_fields = {}
        for key, value in json_data.items():
            # Exclude schema fields
            if key not in ['schema', '$schema', 'id', 'instrument', 'trigger_time']:
                remaining_fields[key] = value
                
        if remaining_fields:
            sections["basic"].append("OTHER_FIELDS:")
            for key, value in remaining_fields.items():
                if isinstance(value, (list, dict)):
                    sections["basic"].append(f"{key.upper()}: {json.dumps(value)}")
                else:
                    sections["basic"].append(f"{key.upper()}: {value}")
    
    # Build formatted text with sections
    formatted_text = ""
    
    # Basic info section
    if sections["basic"]:
        formatted_text += "*[BASIC INFO]*\n> " + "\n> ".join(sections["basic"]) + "\n\n"
    
    # Location section
    if sections["location"]:
        formatted_text += "*[LOCATION]*\n> " + "\n> ".join(sections["location"]) + "\n\n"
    
    # Timing section
    if sections["timing"]:
        formatted_text += "*[TIMING]*\n> " + "\n> ".join(sections["timing"]) + "\n\n"
    
    # Analysis section
    if sections["analysis"]:
        formatted_text += "*[ANALYSIS]*\n> " + "\n> ".join(sections["analysis"])
        
    # Bold entire lines with key field names
    logger.debug(f"Processing line formatting for JSON notice")
    
    lines = formatted_text.split('\n')
    json_key_fields = [
        # Location fields
        'RA:', 'DEC:', 'RA_DEC_ERROR:',
        
        # Energy/analysis fields
        'IMAGE_ENERGY_RANGE:', 'NET_COUNT_RATE:', 'IMAGE_SNR:',
        'ENERGY:', 'SIGNALNESS:', 'FAR:',
        
        # ID fields
        'ID:', 'TRIGGER_NUM:', 'EVENT_NUM:',
        
        # Time fields
        'TRIGGER_TIME:', 'DISCOVERY_TIME:', 'TRIGGER_TIME_DIFF:'
    ]

    for i, line in enumerate(lines):
        # Skip section headers which are already bold
        if '*[BASIC INFO]*' in line or '*[LOCATION]*' in line or '*[TIMING]*' in line or \
           '*[ANALYSIS]*' in line:
            continue
        
        # Handle both '> ' and '>' blockquote formats
        if (line.startswith('> ') or line.startswith('>')) and any(key in line for key in json_key_fields):
            # Standardize the line format before adding bold
            if line.startswith('> '):
                line_content = line[2:]  # Remove '> '
            else:
                line_content = line[1:]  # Remove '>'
                
            # Make the entire line bold and add back blockquote format
            lines[i] = '> *' + line_content + '*'
            logger.debug(f"Made line bold: {lines[i]}")
    
    formatted_text = '\n'.join(lines)
    
    return formatted_text.strip()

def _get_facility_emoji(facility):
    """
    Get appropriate emoji for each facility type to improve visual distinction.
    
    Args:
        facility (str): Facility name
        
    Returns:
        str: Emoji representation for the facility
    """
    facility_upper = facility.upper()
    
    # Swift telescopes - Lightning bolt for Swift
    if 'SWIFT' in facility_upper:
        if 'BAT' in facility_upper:
            return '💥'  # Lightning for Burst Alert Telescope
        elif 'XRT' in facility_upper:
            return '💥'  # Lightning for X-Ray Telescope  
        elif 'UVOT' in facility_upper:
            return '💥'  # Lightning for UV/Optical Telescope
        else:
            return '💥'  # Generic Swift
    
    # Fermi telescopes - Galaxy/cosmic rays
    elif 'FERMI' in facility_upper:
        if 'GBM' in facility_upper:
            return '⚛️'  # Galaxy for Gamma-ray Burst Monitor
        elif 'LAT' in facility_upper:
            return '⚛️'  # Galaxy for Large Area Telescope
        else:
            return '⚛️'  # Generic Fermi
    
    # Einstein Probe - Telescope
    elif 'EINSTEIN' in facility_upper:
        return '👴'  # Telescope for Einstein Probe
    
    # IceCube neutrino detectors - Ice/snow
    elif 'ICECUBE' in facility_upper:
        if 'CASCADE' in facility_upper:
            return '❄️'  # Snowflake for CASCADE events
        elif 'GOLD' in facility_upper:
            return '🧊'  # Ice cube for GOLD tracks
        elif 'BRONZE' in facility_upper:
            return '🧊'  # Ice cube for BRONZE tracks
        else:
            return '🧊'  # Generic IceCube
    
    # AMON - Network/cooperation symbol
    elif 'AMON' in facility_upper:
        return '🔗'  # Link for multi-messenger network
    
    # HAWC - Mountain for high altitude
    elif 'HAWC' in facility_upper:
        return '🏔️'  # Mountain for High-Altitude Water Cherenkov
    
    # CALET - Satellite
    elif 'CALET' in facility_upper:
        return '🛰️'  # Satellite for CALorimetric Electron Telescope
    
    # Default fallback
    else:
        return '📡'  # Generic antenna/detector symbol

def format_message_for_slack(
    topic: str, 
    value: Union[str, bytes], 
    csv_status: Optional[bool] = None, 
    ascii_status: Optional[bool] = None, 
    test_mode: bool = False, 
    custom_facility: Optional[str] = None,
    notice_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Format message based on topic type and content into section-based format
    with unnecessary information removed.
    
    Args:
        topic: The topic of the message
        value: The value of the message
        csv_status: The status of saving the message to CSV
        ascii_status: The status of saving the message to ASCII text file (None for new events)
        test_mode: Whether the function is being called in test mode
        custom_facility: Override the facility name detection with a custom name
        notice_data: Parsed notice data (optional, used for GRB name display)
    
    Returns:
        tuple: (formatted_message, lc_url, notice_url) - formatted Slack message, light curve URL, notice URL
    """
    try:
        # Use custom facility name if provided, otherwise extract from topic
        facility = custom_facility if custom_facility else _get_facility_name(topic)
        
        # Skip test notices
        if '_TEST' in topic.upper() and not test_mode:
            logger.info(f"Skipping test notice from {facility}")
            return None, None, None
        
        # Initialize variables
        lc_url = None
        
        # Format message based on topic type
        # JSON format
        if any(json_topic in topic for json_topic in ['gcn.notices']):
            try:
                json_data = json.loads(value)
                formatted_text = _format_json_notice(json_data, facility)
                lc_url = None
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from {facility} notice")
                formatted_text = value  # Fallback to raw text
                lc_url = None
        
        # Classic text format
        else:
            try:
                formatted_text, lc_url = _filter_notice_text(value, topic)
            except Exception as e:
                logger.warning(f"Failed to parse classic text from {facility} notice: {e}")
                formatted_text = value  # Fallback to raw text
                lc_url = None
            
            # If lc_url is None, construct LC_URL only for Swift notices
            if lc_url is None and notice_data and 'Trigger_num' in notice_data and notice_data['Trigger_num']:
                logger.info(f"Constructing Swift LC_URL for {facility} notice")
                try:
                    trigger_num = str(notice_data['Trigger_num'])
                    facility = notice_data.get('Facility', '')
                    
                    if 'SwiftBAT' in facility:
                        lc_url = f"https://gcn.gsfc.nasa.gov/notices_s/sw0{trigger_num}000msb.gif"
                        logger.info(f"Created Swift BAT LC_URL: {lc_url}")
                    elif 'SwiftXRT' in facility:
                        lc_url = f"https://gcn.gsfc.nasa.gov/notices_s/sw0{trigger_num}000msx.gif"
                        logger.info(f"Created Swift XRT LC_URL: {lc_url}")
                except Exception as e:
                    logger.warning(f"Error constructing Swift LC_URL: {e}")
        
        # Ensure formatted_text is a string before using regex
        if isinstance(formatted_text, bytes):
            formatted_text = formatted_text.decode('utf-8')
        
        # Get notice URL
        notice_url = _get_notice_url(topic, value)
        
        # Add probable GRB name if TURN_ON_NOTICE is true and we have notice data
        if TURN_ON_NOTICE and notice_data and 'Name' in notice_data and notice_data['Name']:
            # Convert formatted_text to string if it's not already
            formatted_text_str = str(formatted_text)
            
            # Pattern to match the [BASIC INFO] section
            basic_info_pattern = r"(\*\[BASIC INFO\]\*\n(?:>.*\n)*?)"
            match = re.search(basic_info_pattern, formatted_text_str)
            
            if match:
                # Extract the parts
                prefix = match.group(1)  # [BASIC INFO] section
                
                # Create GRB_NAME line with proper formatting
                grb_name_line = f"> *GRB_NAME(Probably):      {notice_data['Name']}*\n"
                
                # Replace the matched section with our modified version
                formatted_text = formatted_text_str.replace(
                    prefix,
                    prefix + grb_name_line
                )
                
                logger.info(f"Added probable GRB name to message: {notice_data['Name']}")
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{_get_facility_emoji(facility)} {facility}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": formatted_text
                }
            }
        ]
        
        # Add status information if provided
        status_lines = []
        if csv_status is not None:
            csv_status_icon = "✅" if csv_status else "❌"
            status_lines.append(f"{csv_status_icon} CSV Database: {'Parsed' if csv_status and test_mode else 'Saved' if csv_status else 'Failed'}")
        
        # Show ASCII status for all events
        if ascii_status is not None:
            ascii_status_icon = "✅" if ascii_status else "❌"
            status_lines.append(f"{ascii_status_icon} ASCII Database: {'Parsed' if ascii_status and test_mode else 'Saved' if ascii_status else 'Failed'}")
        
        if status_lines:
            blocks.append({
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": " | ".join(status_lines)
                    }
                ]
            })
        
        return {"blocks": blocks}, lc_url, notice_url
        
    except Exception as e:
        logger.error(f"Error formatting message: {str(e)}", exc_info=True)
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"New GCN Alert: {facility}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error formatting message:* {str(e)}\n\n```{value[:1000]}```"
                    }
                }
            ]
        }, None, None

############################## Calculating ##############################
def _standardize_time_format(time_str, format=None):
    """
    Standardize time format to show both UTC and KST.
    
    Args:
        time_str (str): Time string to standardize
        format (str, optional): Format of the input time string
        
    Returns:
        str: Standardized time string in format 'yy-mm-dd hh:mm:ss (UTC) / yy-mm-dd hh:mm:ss (KST)'
    """
    try:
        # Different date formats in notices
        formats = [
            '%y/%m/%d %H:%M:%S.%f',  # GRB_DATE + GRB_TIME format
            '%y/%m/%d %H:%M:%S',      # Standard format without microseconds
            '%Y-%m-%dT%H:%M:%S.%fZ',  # JSON ISO format
            '%a %d %b %y %H:%M:%S',   # NOTICE_DATE format
            '%a %d %b %y %H:%M:%S.%f' # NOTICE_DATE format with microseconds
        ]
        
        # If a specific format is provided, try that first
        if format:
            formats.insert(0, format)
        
        # Try each format until one works
        dt = None
        for fmt in formats:
            try:
                # Special handling for ISO format with milliseconds
                if fmt == '%Y-%m-%dT%H:%M:%S.%fZ' and 'T' in time_str and time_str.endswith('Z'):
                    # Check if it has milliseconds (3 digits) vs microseconds (6 digits)
                    if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$', time_str):
                        # Convert 3-digit milliseconds to 6-digit microseconds
                        time_str_padded = time_str[:-1] + '000Z'  # .201Z -> .201000Z
                        dt = datetime.strptime(time_str_padded, fmt)
                    else:
                        dt = datetime.strptime(time_str, fmt)
                else:
                    dt = datetime.strptime(time_str, fmt)
                break
            except (ValueError, TypeError):
                continue
        
        # Additional fallback for ISO format parsing
        if dt is None and 'T' in time_str and time_str.endswith('Z'):
            try:
                # Try parsing with fromisoformat after removing Z and adding timezone info
                iso_time = time_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(iso_time)
                logger.debug(f"Successfully parsed time using fromisoformat: {dt}")
            except Exception as e:
                logger.debug(f"fromisoformat also failed: {e}")
        
        # Special handling for NOTICE_DATE format (e.g., "Thu 27 Mar 25 22:22:04 UT")
        if dt is None:
            match = re.search(r"(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT", time_str)
            if match:
                _, day, month_name, year, hour, minute, second = match.groups()
                try:
                    month_number = datetime.strptime(month_name, '%b').month
                    
                    # Handle with or without microseconds
                    if '.' in second:
                        datetime_str = f"20{year}-{month_number:02d}-{int(day):02d} {hour}:{minute}:{second}"
                        dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        datetime_str = f"20{year}-{month_number:02d}-{int(day):02d} {hour}:{minute}:{second}"
                        dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse special NOTICE_DATE format: {e}")
        
        if not dt:
            logger.warning(f"Could not parse time string: '{time_str}'")
            return time_str  # Return original if parsing fails
        
        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Convert to KST
        kst = pytz.timezone('Asia/Seoul')
        dt_kst = dt.astimezone(kst)
        
        # Format in the requested style
        return f"{dt.strftime('%y-%m-%d %H:%M:%S')} (UTC) / {dt_kst.strftime('%y-%m-%d %H:%M:%S')} (KST)"
    except Exception as e:
        logger.error(f"Error standardizing time format: {e}")
        return time_str

def _calculate_time_diff(notice_time_str, trigger_time_str):
    """
    Calculate time difference between notice time and trigger time.
    
    Args:
        notice_time_str (str): Notice time string
        trigger_time_str (str): Trigger time string
        
    Returns:
        str: Time difference in a readable format
    """
    try:
        # Parse both times more robustly
        notice_dt = None
        trigger_dt = None
        
        # Try to parse notice time - first with standard formats
        formats = [
            '%y/%m/%d %H:%M:%S.%f',
            '%y/%m/%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%a %d %b %y %H:%M:%S',
            '%a %d %b %y %H:%M:%S.%f'
        ]
        
        for fmt in formats:
            try:
                notice_dt = datetime.strptime(notice_time_str, fmt)
                if notice_dt.tzinfo is None:
                    notice_dt = notice_dt.replace(tzinfo=timezone.utc)
                break
            except (ValueError, TypeError):
                continue
        
        # Special handling for NOTICE_DATE format if standard formats fail
        if notice_dt is None:
            match = re.search(r"(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT", notice_time_str)
            if match:
                _, day, month_name, year, hour, minute, second = match.groups()
                try:
                    month_number = datetime.strptime(month_name, '%b').month
                    
                    # Handle with or without microseconds
                    if '.' in second:
                        datetime_str = f"20{year}-{month_number:02d}-{int(day):02d} {hour}:{minute}:{second}"
                        notice_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        datetime_str = f"20{year}-{month_number:02d}-{int(day):02d} {hour}:{minute}:{second}"
                        notice_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    
                    if notice_dt.tzinfo is None:
                        notice_dt = notice_dt.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError) as e:
                    logger.error(f"Could not parse special NOTICE_DATE format: {e}")
        
        # Try to parse trigger time with enhanced ISO format handling
        for fmt in formats:
            try:
                # Special handling for ISO format with milliseconds
                if fmt == '%Y-%m-%dT%H:%M:%S.%fZ' and 'T' in trigger_time_str and trigger_time_str.endswith('Z'):
                    # Check if it has milliseconds (3 digits) vs microseconds (6 digits)
                    if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$', trigger_time_str):
                        # Convert 3-digit milliseconds to 6-digit microseconds
                        trigger_time_padded = trigger_time_str[:-1] + '000Z'  # .201Z -> .201000Z
                        trigger_dt = datetime.strptime(trigger_time_padded, fmt)
                    else:
                        trigger_dt = datetime.strptime(trigger_time_str, fmt)
                else:
                    trigger_dt = datetime.strptime(trigger_time_str, fmt)
                    
                if trigger_dt.tzinfo is None:
                    trigger_dt = trigger_dt.replace(tzinfo=timezone.utc)
                break
            except (ValueError, TypeError):
                continue
        
        # Additional fallback for ISO format parsing
        if trigger_dt is None and 'T' in trigger_time_str and trigger_time_str.endswith('Z'):
            try:
                # Try parsing with fromisoformat after removing Z and adding timezone info
                iso_time = trigger_time_str.replace('Z', '+00:00')
                trigger_dt = datetime.fromisoformat(iso_time)
                logger.info(f"Successfully parsed trigger time using fromisoformat: {trigger_dt}")
            except Exception as e:
                logger.warning(f"fromisoformat also failed for trigger time: {e}")
        
        if notice_dt and trigger_dt:
            # Calculate difference in seconds
            diff_seconds = (notice_dt - trigger_dt).total_seconds()
            
            # Format difference
            hours, remainder = divmod(abs(diff_seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            # Determine if notice came before or after trigger
            if diff_seconds >= 0:
                return f"+{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} (Notice after Trigger)"
            else:
                return f"-{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} (Notice before Trigger)"
        
        logger.warning(f"Could not parse both times: notice='{notice_time_str}', trigger='{trigger_time_str}'")
        return "Time difference could not be calculated"
    except Exception as e:
        logger.error(f"Error calculating time difference: {e}")
        return "Time difference calculation error"

def _get_notice_url(topic, text):
    """
    Generate facility-specific notice URL.
    
    Args:
        topic (str): The topic of the notice
        text (str): The text content of the notice
        
    Returns:
        str or None: URL for the notice if applicable
    """
    try:
        # Ensure text is a string
        if isinstance(text, bytes):
            text = text.decode('utf-8', errors='ignore')
        
        # Extract facility from topic
        facility = _get_facility_name(topic)
        logger.info(f"Getting notice URL for facility: {facility} from topic: {topic}")
        
        # ICECUBE patterns (BRONZE, GOLD)
        if 'ICECUBE_ASTROTRACK_BRONZE' in topic or 'ICECUBE_ASTROTRACK_GOLD' in topic:
            run_match = re.search(r"RUN_NUM:\s*(\d+)", text)
            event_match = re.search(r"EVENT_NUM:\s*(\d+)", text)
            
            if run_match and event_match:
                run_num = run_match.group(1)
                event_num = event_match.group(1)
                url = f"https://gcn.gsfc.nasa.gov/notices_amon_g_b/{run_num}_{event_num}.amon"
                logger.info(f"Generated IceCube track URL: {url}")
                return url
            else:
                logger.warning(f"Could not extract RUN_NUM or EVENT_NUM for IceCube track notice")
        
        # HAWC_BURST_MONITOR pattern
        elif 'HAWC_BURST_MONITOR' in topic:
            run_match = re.search(r"RUN_NUM:\s*(\d+)", text)
            event_match = re.search(r"EVENT_NUM:\s*(\d+)", text)
            
            if run_match and event_match:
                run_num = run_match.group(1)
                event_num = event_match.group(1)
                url = f"https://gcn.gsfc.nasa.gov/notices_amon_hawc/{run_num}_{event_num}.amon"
                logger.info(f"Generated HAWC URL: {url}")
                return url
        
        # AMON_NU_EM_COINC pattern
        elif 'AMON_NU_EM_COINC' in topic:
            run_match = re.search(r"RUN_NUM:\s*(\d+)", text)
            event_match = re.search(r"EVENT_NUM:\s*(\d+)", text)
            
            if run_match and event_match:
                run_num = run_match.group(1)
                event_num = event_match.group(1)
                url = f"https://gcn.gsfc.nasa.gov/notices_amon_nu_em/{run_num}_{event_num}.amon"
                logger.info(f"Generated AMON URL: {url}")
                return url
        
        # ICECUBE_CASCADE pattern
        elif 'ICECUBE_CASCADE' in topic:
            run_match = re.search(r"RUN_NUM:\s*(\d+)", text)
            event_match = re.search(r"EVENT_NUM:\s*(\d+)", text)
            
            if run_match and event_match:
                run_num = run_match.group(1)
                event_num = event_match.group(1)
                url = f"https://gcn.gsfc.nasa.gov/notices_amon_icecube_cascade/{run_num}_{event_num}.amon"
                logger.info(f"Generated IceCube cascade URL: {url}")
                return url
        
        # FERMI patterns - use more flexible matching
        elif ('FERMI' in topic.upper() or 'FERMI' in facility.upper()) and ('GBM' in topic.upper() or 'LAT' in topic.upper() or 'GBM' in facility.upper() or 'LAT' in facility.upper()):
            # Try multiple patterns for TRIGGER_NUM
            trigger_patterns = [
                r"TRIGGER_NUM:\s*(\d+)",
                r"TRIGGER_NUM.*?(\d+)",
                r"TRIGGER.*?(\d{9})",  # For 9-digit IDs
                r"trigger.*?#?(\d+)"
            ]
            
            for pattern in trigger_patterns:
                trigger_match = re.search(pattern, text, re.IGNORECASE)
                if trigger_match:
                    trigger_num = trigger_match.group(1)
                    url = f"https://gcn.gsfc.nasa.gov/other/{trigger_num}.fermi"
                    logger.info(f"Generated Fermi URL: {url}")
                    return url
            
            logger.warning(f"Could not extract TRIGGER_NUM for Fermi notice: {topic}")
        
        # SWIFT patterns - use more flexible matching
        elif ('SWIFT' in topic.upper() or 'SWIFT' in facility.upper()) and any(x in topic.upper() or x in facility.upper() for x in ['BAT', 'UVOT', 'XRT']):
            # Try multiple patterns for TRIGGER_NUM
            trigger_patterns = [
                r"TRIGGER_NUM:\s*(\d+)",
                r"TRIGGER_NUM.*?(\d+)",
                r"trigger.*?#?(\d+)"
            ]
            
            for pattern in trigger_patterns:
                trigger_match = re.search(pattern, text, re.IGNORECASE)
                if trigger_match:
                    trigger_num = trigger_match.group(1)
                    url = f"https://gcn.gsfc.nasa.gov/other/{trigger_num}.swift"
                    logger.info(f"Generated Swift URL: {url}")
                    return url
            
            logger.warning(f"Could not extract TRIGGER_NUM for Swift notice: {topic}")
        
        logger.info(f"No URL pattern matched for topic: {topic}")
        return None
    except Exception as e:
        logger.error(f"Error getting notice URL: {e}")
        return None

def _get_facility_name(topic):
    """
    Extract facility name from topic string
    
    Args:
        topic (str): Topic string
    
    Returns:
        str: Facility name
    """
    try:
        if 'gcn.classic.text.' in topic:
            # Split by dots and get the part after 'text.'
            parts = topic.split('text.')[1].split('_')
            
            # Custom rules for different facilities
            if parts[0] == 'FERMI' or parts[0] == 'SWIFT':
                # Return FACILITY_INSTRUMENT (e.g., FERMI_GBM, SWIFT_BAT)
                return f"{parts[0]}-{parts[1]}"
            elif parts[0] == 'ICECUBE':
                # Return the full name after text. (e.g., ICECUBE_ASTROTRACK_BRONZE)
                return topic.split('text.')[1]
            elif parts[0] in ['AMON', 'HAWC']:
                # Return just the first part
                return parts[0]
            else:
                # Default: return first part
                return parts[0]
            
        elif 'gcn.notices.' in topic:
            # Split by dots and get the part after 'notices.'
            parts = topic.split('notices.')[1].split('.')
            return parts[0].upper()  # Return first part capitalized
        
        return topic
    except Exception as e:
        logger.error(f"Error extracting facility name: {e}")
        return topic

############################## Connection Monitoring ##############################
def update_heartbeat() -> None:
    """Update the last heartbeat time"""
    global last_heartbeat
    with heartbeat_lock:
        last_heartbeat = datetime.now()
        logger.debug(f"Updated last heartbeat time to {last_heartbeat}")

def check_connection() -> None:
    """Monitor connection status and authentication with improved reconnection logic."""
    global last_connection_status, last_heartbeat, consumer, running, all_topics
    global reconnect_attempts, max_reconnect_attempts
    
    while running:
        try:
            with heartbeat_lock:
                time_since_last_heartbeat = datetime.now() - last_heartbeat
                currently_connected = time_since_last_heartbeat.total_seconds() < CONNECTION_TIMEOUT
                
                # If connection status has changed
                if currently_connected != last_connection_status:
                    try:
                        if not currently_connected:
                            logger.warning("Connection lost. Last heartbeat: %s", last_heartbeat)
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
                                                    f"*Time Since Last Heartbeat:* {time_since_last_heartbeat.total_seconds():.1f} seconds"
                                        }
                                    }
                                ]
                            }
                            
                            # Try to send slack notification about connection loss
                            try:
                                slack_client.chat_postMessage(
                                    channel=SLACK_CHANNEL,
                                    blocks=message["blocks"]
                                )
                            except Exception as e:
                                logger.error(f"Error sending connection alert to Slack: {e}")
                                
                        else:
                            # Connection restored - reset reconnect attempts
                            reconnect_attempts = 0
                            logger.info("Connection restored. Last heartbeat: %s", last_heartbeat)
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
                            
                            # Try to send slack notification about connection restoration
                            try:
                                slack_client.chat_postMessage(
                                    channel=SLACK_CHANNEL,
                                    blocks=message["blocks"]
                                )
                            except Exception as e:
                                logger.error(f"Error sending connection restoration alert to Slack: {e}")
                        
                        # Update the connection status regardless of Slack notification success
                        last_connection_status = currently_connected
                        
                    except Exception as e:
                        logger.error(f"Error sending connection status to Slack: {e}")
                
                # Try to reconnect if needed with exponential backoff
                if not currently_connected:
                    if reconnect_attempts >= max_reconnect_attempts:
                        logger.error(f"Max reconnection attempts ({max_reconnect_attempts}) reached. "
                                   f"Waiting longer before retry...")
                        time.sleep(300)  # Wait 5 minutes before resetting attempts
                        reconnect_attempts = 0
                        continue
                    
                    # Calculate backoff delay
                    base_delay = min(2 ** reconnect_attempts, 60)  # Cap at 60 seconds
                    jitter = random.uniform(0, 1)
                    delay = base_delay + jitter
                    
                    logger.info(f"Attempting to reconnect to GCN (attempt {reconnect_attempts + 1}/{max_reconnect_attempts}) "
                              f"in {delay:.1f} seconds...")
                    
                    time.sleep(delay)
                    
                    success = attempt_reconnection()
                    if success:
                        logger.info("Reconnection successful")
                        reconnect_attempts = 0
                        # Update heartbeat to reflect successful reconnection
                        last_heartbeat = datetime.now()
                    else:
                        reconnect_attempts += 1
                        logger.warning(f"Reconnection attempt {reconnect_attempts} failed")
            
        except Exception as e:
            logger.error("Error in connection monitoring: %s", e, exc_info=True)
        
        # Dynamic check interval based on connection status
        if currently_connected:
            time.sleep(60)  # Check every minute when connected
        else:
            time.sleep(30)  # Check more frequently when disconnected

def attempt_reconnection() -> bool:
    """
    Attempt to reconnect to GCN with proper error handling.
    
    Returns:
        bool: True if reconnection successful, False otherwise
    """
    global consumer, all_topics
    
    try:
        # Thread-safe consumer replacement
        with consumer_lock:
            old_consumer = consumer
            
            try:
                # Close existing consumer with timeout
                if old_consumer:
                    logger.debug("Closing existing consumer connection")
                    old_consumer.close()
                    logger.debug("Successfully closed existing consumer")
            except Exception as close_err:
                logger.warning(f"Error closing existing consumer: {close_err}")
            
            # Create new consumer
            logger.debug("Creating new consumer instance")
            new_consumer = Consumer(
                client_id=GCN_ID,
                client_secret=GCN_SECRET
            )
            
            # Subscribe to topics
            logger.debug(f"Subscribing to topics: {all_topics}")
            new_consumer.subscribe(all_topics)
            
            # Test the connection with a short consume call
            logger.debug("Testing new connection")
            test_msgs = new_consumer.consume(timeout=2.0)
            
            # If we get here without exception, connection is working
            logger.debug("Connection test passed")
            consumer = new_consumer
            
            return True
            
    except Exception as e:
        logger.error(f"Reconnection attempt failed: {e}")
        # Clean up failed consumer if it was created
        try:
            if 'new_consumer' in locals():
                new_consumer.close()
        except Exception as cleanup_err:
            logger.debug(f"Error cleaning up failed consumer: {cleanup_err}")
        
        return False

def trigger_reconnection():
    """
    Trigger immediate reconnection attempt from main loop.
    Call this function when main loop detects connection errors.
    """
    global last_heartbeat
    
    logger.info("Main loop triggered reconnection")
    
    # Set last_heartbeat to old time to trigger reconnection logic
    with heartbeat_lock:
        last_heartbeat = datetime.now() - timedelta(seconds=CONNECTION_TIMEOUT + 1)
############################## PROCESS ##############################
def _compare_event_data(old_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare old and new event data to identify differences.
    Updated to work with new 4-case visibility system.
    
    Args:
        old_data (Dict[str, Any]): Previous event data from ASCII file
        new_data (Dict[str, Any]): New event data from notice
        
    Returns:
        Dict[str, Any]: Dictionary containing the differences
    """
    differences = {}
    
    try:
        # Check coordinate changes
        coord_fields = ['RA', 'DEC', 'Error']
        coord_changed = False
        new_coords = {}
        
        for field in coord_fields:
            old_val = old_data.get(field, '')
            new_val = new_data.get(field, '')
            
            # Convert to float for comparison if both are numeric
            try:
                old_float = float(old_val) if old_val and old_val != '' else None
                new_float = float(new_val) if new_val and new_val != '' else None
                
                # Only record change if both values exist and are different
                if old_float is not None and new_float is not None:
                    # Use a small tolerance for floating point comparison
                    if abs(old_float - new_float) > 0.001:
                        coord_changed = True
                        new_coords[field] = new_float
                elif old_float != new_float and new_float is not None:
                    coord_changed = True
                    new_coords[field] = new_float
            except (ValueError, TypeError):
                # Non-numeric values, compare as strings
                if old_val != new_val and new_val:
                    coord_changed = True
                    new_coords[field] = new_val
        
        if coord_changed:
            differences['coordinates'] = new_coords
            logger.info(f"Coordinate changes detected: {new_coords}")
        
        # Check facility/instrument changes (normalized comparison)
        old_facility = old_data.get('Facility', '')
        new_facility = new_data.get('Facility', '')
        
        # Use normalized comparison but record actual facility names
        old_normalized = notice_handler._normalize_facility_name(old_facility)
        new_normalized = notice_handler._normalize_facility_name(new_facility)
        
        # If the normalized facilities are the same but actual facilities differ,
        # this is an instrument update (e.g., SwiftBAT -> SwiftXRT)
        if old_normalized == new_normalized and old_facility != new_facility:
            differences['facility_change'] = {
                'from': old_facility,
                'to': new_facility
            }
            logger.info(f"Facility instrument change: {old_facility} -> {new_facility}")
        
        # Check visibility changes (if visibility info is provided)
        if 'visibility_info' in new_data:
            visibility_info = new_data['visibility_info']
            # Always include visibility info for thread updates
            differences['visibility'] = visibility_info
            logger.info(f"Visibility status: {visibility_info.get('status', 'unknown')}")
        
        logger.info(f"Found {len(differences)} types of differences between old and new data")
        return differences
        
    except Exception as e:
        logger.error(f"Error comparing event data: {e}")
        return {}

def _format_thread_message(differences: Dict[str, Any], notice_data: Dict[str, Any]) -> str:
    """
    Format a thread message showing only the differences.
    Updated to work with new 4-case visibility system.
    
    Args:
        differences (Dict[str, Any]): Dictionary of differences
        notice_data (Dict[str, Any]): New notice data
        
    Returns:
        str: Formatted thread message
    """
    try:
        facility = notice_data.get('Facility', 'Unknown')
        sections = []
        
        # Header
        sections.append(f"🔄 *UPDATE: {facility}*")
        sections.append("> *New Information:*")
        
        # Facility/instrument change
        if 'facility_change' in differences:
            change = differences['facility_change']
            sections.append(f"> - 🔬 *Instrument:* {change['from']} → *{change['to']}*")
        
        # Coordinate changes
        if 'coordinates' in differences:
            coords = differences['coordinates']
            coord_parts = []
            
            if 'RA' in coords:
                coord_parts.append(f"RA={coords['RA']}")
            if 'DEC' in coords:
                coord_parts.append(f"DEC={coords['DEC']}")
            if 'Error' in coords:
                coord_parts.append(f"±{coords['Error']}°")
            
            if coord_parts:
                coord_str = ", ".join(coord_parts)
                sections.append(f"> - 📍 *Coordinates:* {coord_str}")
        
        # Visibility changes - now works with 4-case system
        if 'visibility' in differences:
            vis_info = differences['visibility']
            status = vis_info.get('status', '')
            
            if status == 'observable_now':
                end_time = vis_info.get('observable_end')
                end_time_str = end_time.strftime('%H:%M') if end_time else "Unknown"
                remaining = vis_info.get('remaining_hours', 0)
                sections.append(f"> - 🌃 *Visibility:* 🟢 Currently Observable until {end_time_str} CLT ({remaining:.1f}h remaining)")
                
            elif status == 'observable_later':
                hours_until = vis_info.get('hours_until_observable', 0)
                start_time = vis_info.get('observable_start')
                start_time_str = start_time.strftime('%H:%M') if start_time else "Unknown"
                sections.append(f"> - 🌃 *Visibility:* 🟠 Observable in {hours_until:.1f} hours (from {start_time_str} CLT)")
                
            elif status == 'observable_tomorrow':
                sections.append(f"> - 🌃 *Visibility:* 🔵 Observable Tomorrow Night")
                
            else:  # not_observable
                reason = vis_info.get('reason', 'Unknown limitation')
                sections.append(f"> - 🌃 *Visibility:* 🔴 Not Observable ({reason})")
        
        # If no differences found, show a generic update message
        if len(sections) <= 2:
            sections.append("> - ℹ️ *Status:* Updated information received")
        
        return "\n".join(sections)
        
    except Exception as e:
        logger.error(f"Error formatting thread message: {e}")
        return f"🔄 *UPDATE: {notice_data.get('Facility', 'Unknown')}*\n> - ℹ️ *Status:* Updated information received"

def _evaluate_too_criteria(notice_data: Dict[str, Any], visibility_info: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Evaluate whether a ToO request should be sent based on specific criteria.
    Updated to work with new 4-case visibility system.
    
    Args:
        notice_data: Parsed notice data
        visibility_info: Visibility analysis information
        
    Returns:
        Tuple[bool, str]: (should_send, reason)
    """
    if not notice_data:
        return False, "No notice data available"
    
    facility = notice_data.get('Facility', '')
    target_name = notice_data.get('Name', 'Unknown Target')
    
    # Criteria 1: IceCube neutrino events (always high priority)
    icecube_facilities = ['AMON', 'IceCubeCASCADE', 'HAWC', 'IceCubeBRONZE', 'IceCubeGOLD']
    is_neutrino_event = any(ice_fac in facility for ice_fac in icecube_facilities)
    
    if is_neutrino_event:
        logger.info(f"ToO Criteria Met - Neutrino Event: {facility}")
        
        # Return specific facility name as reason
        if 'AMON' in facility:
            return True, "AMON Neutrino-EM Coincidence"
        elif 'CASCADE' in facility:
            return True, "IceCube CASCADE Event"
        elif 'HAWC' in facility:
            return True, "HAWC Burst Monitor"
        elif 'GOLD' in facility:
            return True, "IceCube GOLD Track"
        elif 'BRONZE' in facility:
            return True, "IceCube BRONZE Track"
        else:
            return True, facility  # Fallback to full facility name
    
    # Criteria 2: Currently observable targets with good conditions
    if visibility_info:
        status = visibility_info.get('status')
        
        if status == 'observable_now':
            remaining_hours = visibility_info.get('remaining_hours', 0)
            current_altitude = visibility_info.get('current_altitude', 0)
            
            # Only send ToO if we have sufficient time and good altitude
            if remaining_hours >= 1.0 and current_altitude >= 35:
                logger.info(f"ToO Criteria Met - Currently Observable: {target_name}")
                return True, "Currently Observable with Good Conditions"
            else:
                logger.debug(f"Target observable but limited conditions (alt={current_altitude:.1f}°, {remaining_hours:.1f}h remaining)")
                return False, f"Observable but limited conditions (alt={current_altitude:.1f}°, {remaining_hours:.1f}h remaining)"
        
        elif status == 'observable_later':
            # For targets observable later tonight, only send ToO if:
            # 1. Will be observable within 2 hours
            # 2. Will have decent altitude when observable
            hours_until = visibility_info.get('hours_until_observable', 999)
            observable_hours = visibility_info.get('observable_hours', 0)
            
            if hours_until <= 2.0 and observable_hours >= 2.0:
                logger.info(f"ToO Criteria Met - Observable Soon: {target_name}")
                return True, f"Observable in {hours_until:.1f} hours"
            else:
                logger.debug(f"Target observable later but not urgent (in {hours_until:.1f}h, for {observable_hours:.1f}h)")
                return False, f"Observable later but not urgent (in {hours_until:.1f}h)"
        
        elif status == 'observable_tomorrow':
            # Generally don't send immediate ToO for tomorrow targets
            # unless it's a very special case (handled above for neutrinos)
            logger.debug(f"Target observable tomorrow - no immediate ToO needed")
            return False, "Observable tomorrow - plan for next night"
        
        else:  # not_observable
            logger.debug(f"Target not observable - no ToO possible")
            return False, f"Not observable from Chile"
    
    # Default: No criteria met
    logger.debug(f"No ToO criteria met for {facility} event")
    return False, f"No immediate ToO criteria met for {facility} event"

def _send_too_email_if_criteria_met(notice_data: Dict[str, Any], visibility_info: Optional[Dict[str, Any]]) -> None:
    """
    Send ToO email if specific criteria are met.
    Updated to work with new 4-case visibility system.
    
    Args:
        notice_data: Parsed notice data
        visibility_info: Visibility analysis information
    """
    if not TURN_ON_TOO_EMAIL:
        return
        
    # Evaluate ToO criteria
    should_send, reason = _evaluate_too_criteria(notice_data, visibility_info)
    
    if not should_send:
        logger.debug(f"ToO not sent: {reason}")
        return
        
    try:        
        # Initialize emailer
        emailer = GCNToOEmailer(
            email_from=EMAIL_FROM,
            email_to=["7dt.observation.alert@gmail.com"],
            email_password=EMAIL_PASSWORD,
            min_altitude=MIN_ALTITUDE,
            min_moon_sep=MIN_MOON_SEP
        )
        
        # Use base ToO config and add the specific reason
        custom_too_config = TOO_CONFIG.copy()
        custom_too_config['additional_comments'] = f"ToO Reason: {reason}"
        
        # Set priority and urgency based on reason and visibility status
        neutrino_reasons = ['AMON', 'IceCube CASCADE', 'HAWC', 'IceCube GOLD', 'IceCube BRONZE']
        is_neutrino = any(neut_type in reason for neut_type in neutrino_reasons)
        
        if is_neutrino:
            # High priority for all neutrino events
            custom_too_config.update({
                'priority': 'URGENT',
                'abortObservation': 'Yes'
            })
        elif visibility_info and visibility_info.get('status') == 'observable_now':
            # High priority for currently observable targets
            custom_too_config.update({
                'priority': 'URGENT',
                'abortObservation': 'Yes'
            })
        else:
            # Normal priority for other cases
            custom_too_config.update({
                'priority': 'NORMAL',
                'abortObservation': 'No'
            })
        
        # Send ToO request
        email_sent = emailer.process_notice(notice_data, custom_too_config, visibility_info)
        
        if email_sent:
            logger.info(f"ToO email sent for {notice_data.get('Name', 'target')} - Reason: {reason}")
        else:
            logger.warning(f"ToO email failed to send for {notice_data.get('Name', 'target')} - Reason: {reason}")
            
    except Exception as e:
        logger.error(f"Error sending ToO email: {e}")

def setup_slack_handlers():
    """Setup Slack event handlers for ToO integration"""
    if not app or not too_integration:
        return
    
    @app.view("too_request_modal")
    def handle_too_modal(ack, body, client):
        """
        Handle modal submissions - Phase 3: Send actual emails using GCNToOEmailer
        
        This function processes Slack ToO form submissions and sends actual emails
        to the observation team using the existing GCNToOEmailer infrastructure.
        """
        ack()
        
        try:
            view = body['view']
            user_id = body['user']['id']
            thread_ts = body.get('message', {}).get('ts')  # For threaded responses
            
            # Extract form data using the class method
            form_data = too_integration.extract_form_data(view['state']['values'])
            
            if not form_data:
                raise ValueError("Failed to extract form data from modal submission")
            
            # Parse original notice data from modal metadata
            try:
                notice_data = json.loads(view['private_metadata'])
            except (json.JSONDecodeError, KeyError):
                logger.warning("No notice data found in modal metadata, using empty dict")
                notice_data = {}
            
            # Get user information
            user_name = too_integration.get_user_display_name(user_id)
            user_email = too_integration.get_user_email(user_id)
            
            if not user_email:
                raise ValueError("Could not retrieve user email address")
            
            # Convert Slack form data to GCNToOEmailer format
            email_data = too_integration.convert_slack_form_to_email_data(
                form_data, user_name, user_email, notice_data
            )
            
            if not email_data:
                raise ValueError("Failed to convert form data to email format")
            
            # Log the ToO request submission
            logger.info(f"Processing ToO request from {user_name} ({user_email}) for target: {email_data.get('target', 'Unknown')}")
            
            # Check if email sending is enabled
            if not TURN_ON_TOO_EMAIL:
                # Send Slack confirmation without email
                client.chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text="⚠️ *ToO Request Logged* (Email disabled in config)\n"
                        f"Target: {form_data['target']}\n"
                        f"Requester: {user_name}\n"
                        f"*Note: Email sending is currently disabled*",
                    thread_ts=thread_ts
                )
                logger.info(f"ToO request logged but email disabled for {form_data['target']}")
                return
            
            # Initialize GCN ToO Emailer with current configuration
            emailer = GCNToOEmailer(
                email_from=EMAIL_FROM,
                email_to=["7dt.observation.alert@gmail.com"],  # Primary observation team email
                email_password=EMAIL_PASSWORD,
                min_altitude=MIN_ALTITUDE,
                min_moon_sep=MIN_MOON_SEP
            )
            
            # Attempt to get visibility information for this target
            visibility_info = None
            try:
                if visibility_available and email_data.get('ra') and email_data.get('dec'):
                    # Try to get visibility info using existing visibility system
                    # This integrates with the visibility plotting functionality
                    from visibility_analysis import get_visibility_info
                    visibility_info = get_visibility_info(
                        ra=float(email_data['ra']),
                        dec=float(email_data['dec']),
                        target_name=email_data['target']
                    )
                    logger.info(f"Retrieved visibility info for {email_data['target']}")
            except Exception as vis_error:
                logger.warning(f"Could not get visibility info: {vis_error}")
                # Continue without visibility info - email will still be sent
            
            # Create custom ToO configuration for this specific request
            custom_too_config = {
                'singleExposure': int(email_data['singleExposure']),
                'imageCount': int(email_data['imageCount']),
                'obsmode': email_data['obsmode'],
                'selectedFilters': email_data['selectedFilters'],
                'selectedTelNumber': email_data.get('selectedTelNumber', 1),
                'abortObservation': email_data['abortObservation'],
                'priority': email_data['priority'],
                'gain': email_data['gain'],
                'radius': email_data.get('radius', '0'),
                'binning': email_data['binning'],
                'obsStartTime': email_data.get('obsStartTime', 'ASAP'),
                'additional_comments': f"Submitted via Slack by {user_name} ({user_email})"
            }
            
            # Send ToO email using the existing emailer infrastructure
            email_success = emailer.send_too_email(
                notice_data=email_data,  # email_data is already in correct format
                visibility_info=visibility_info,
                too_config=custom_too_config
            )
            
            # Handle email sending results
            if email_success:
                # Success: Send confirmation to Slack with details
                success_message = (
                    f"✅ *ToO Request Sent Successfully*\n\n"
                    f"**Target:** {form_data['target']}\n"
                    f"**Coordinates:** RA {form_data['ra']}°, Dec {form_data['dec']}°\n"
                    f"**Exposure:** {form_data['exposure']}s × {form_data['imageCount']} images "
                    f"({form_data['totalExposureTime']}s total)\n"
                    f"**Filters:** {', '.join(form_data['selectedFilters'])}\n"
                    f"**Priority:** {form_data['priority']}\n"
                    f"**Submitted by:** {user_name}\n\n"
                    f"🔗 The observation team has been notified via email."
                )
                
                # Add visibility information if available
                if visibility_info:
                    status = visibility_info.get('status', 'unknown')
                    if status == 'observable_now':
                        success_message += f"\n🌟 **Status:** Observable now!"
                    elif status == 'observable_later':
                        hours_until = visibility_info.get('hours_until_observable', 'unknown')
                        success_message += f"\n⏰ **Status:** Observable in {hours_until} hours"
                    elif status == 'observable_tomorrow':
                        success_message += f"\n🌅 **Status:** Observable tomorrow night"
                    else:
                        success_message += f"\n❓ **Status:** {status}"
                
                client.chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text=success_message,
                    thread_ts=thread_ts
                )
                
                logger.info(f"ToO email sent successfully for {form_data['target']} by {user_name}")
                
            else:
                # Email failed: Send error message to Slack
                error_message = (
                    f"❌ *ToO Request Failed*\n\n"
                    f"**Target:** {form_data['target']}\n"
                    f"**Submitted by:** {user_name}\n\n"
                    f"⚠️ Failed to send email to observation team.\n"
                    f"Please try again or contact the system administrator.\n\n"
                    f"**Your request has been logged for manual processing.**"
                )
                
                client.chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text=error_message,
                    thread_ts=thread_ts
                )
                
                logger.error(f"ToO email failed to send for {form_data['target']} by {user_name}")
                
                # Log the form data for manual processing
                logger.error(f"MANUAL PROCESSING REQUIRED - ToO data: {json.dumps(form_data, indent=2)}")
        
        except ValueError as ve:
            # Handle validation errors
            error_msg = str(ve)
            logger.warning(f"ToO form validation error: {error_msg}")
            
            try:
                client.chat_postEphemeral(
                    channel=SLACK_CHANNEL,
                    user=user_id,
                    text=f"❌ **Validation Error:** {error_msg}\nPlease try submitting the form again."
                )
            except Exception as slack_error:
                logger.error(f"Failed to send validation error message to Slack: {slack_error}")
        
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Unexpected error in handle_too_modal: {e}", exc_info=True)
            
            try:
                client.chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text=(
                        f"❌ *System Error*\n\n"
                        f"An unexpected error occurred while processing the ToO request.\n"
                        f"**Error:** {str(e)}\n\n"
                        f"Please contact the system administrator or try again later.\n"
                        f"Your request details have been logged for manual review."
                    ),
                    thread_ts=thread_ts
                )
            except Exception as slack_error:
                logger.error(f"Failed to send error message to Slack: {slack_error}")

    @app.action("submit_too_request")
    def handle_submit_too_request(ack, body, client, logger):
        """
        Handle ToO request button clicks - opens the ToO request modal.
        
        This function is triggered when someone clicks the "Submit ToO Request" button
        on a GRB alert message. It validates user authorization and opens the ToO form modal.
        """
        # Acknowledge the button click immediately (must be within 3 seconds)
        ack()
        
        try:
            user_id = body['user']['id']
            trigger_id = body['trigger_id']
            
            # Log the button click
            logger.info(f"ToO request button clicked by user {user_id}")
            
            # Check if user is authorized to submit ToO requests
            if not too_integration.is_user_authorized(user_id):
                channel_id = body['channel']['id']
                thread_ts = body.get('message', {}).get('ts')
                too_integration.handle_unauthorized_access(channel_id, user_id, thread_ts)
                logger.warning(f"Unauthorized ToO request attempt by user {user_id}")
                return
            
            # Get user email
            user_email = too_integration.get_user_email(user_id)
            if not user_email:
                # Send ephemeral error message
                client.chat_postEphemeral(
                    channel=body['channel']['id'],
                    user=user_id,
                    text="❌ *Error: Could not retrieve your email address*\n\nPlease ensure your Slack profile has an email address configured.",
                    thread_ts=body.get('message', {}).get('ts')
                )
                logger.error(f"Could not retrieve email for authorized user {user_id}")
                return
            
            # Parse notice data from button value
            notice_data = {}
            try:
                action_value = body['actions'][0].get('value', '{}')
                notice_data = json.loads(action_value)
                logger.debug(f"Parsed notice data from button: {notice_data}")
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.warning(f"Could not parse notice data from button value: {e}")
                # Continue with empty notice_data - modal will still work
            
            # Create and open the ToO request modal
            modal_opened = too_integration.create_too_modal(trigger_id, user_email, notice_data)
            
            if modal_opened:
                logger.info(f"ToO modal opened successfully for user {user_id}")
            else:
                # Send ephemeral error message
                client.chat_postEphemeral(
                    channel=body['channel']['id'],
                    user=user_id,
                    text="❌ *Error: Could not open ToO request form*\n\nPlease try again or contact the system administrator.",
                    thread_ts=body.get('message', {}).get('ts')
                )
                logger.error(f"Failed to open ToO modal for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error handling ToO request button click: {e}")
            
            # Send ephemeral error message to user
            try:
                client.chat_postEphemeral(
                    channel=body['channel']['id'],
                    user=body['user']['id'],
                    text="❌ *Unexpected Error*\n\nSomething went wrong while processing your ToO request. Please try again or contact the system administrator.",
                    thread_ts=body.get('message', {}).get('ts')
                )
            except Exception as notification_error:
                logger.error(f"Failed to send error notification to user: {notification_error}")

def process_notice_and_send_message(topic, value, slack_client, slack_channel, is_test=False):
    """
    Process a GCN notice and send to Slack with visibility plot.
    Updated to work seamlessly with new 4-case visibility system.
    
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
        
        # 1. Parse the notice with handler
        logger.info(f"Parsing notice from {topic}")
        notice_data = notice_handler.parse_notice(value, topic)
        
        if not notice_data:
            logger.warning(f"Failed to parse notice from {topic}")
            return False, "Failed to parse notice"
        
        facility = notice_data.get('Facility', '')
        trigger_num = notice_data.get('Trigger_num', '')
        
        # 2. Check if this is an update to an existing event
        existing_event = None
        existing_thread_ts = None
        is_update = False
        
        if facility and trigger_num:
            try:
                existing_event = notice_handler._find_existing_event(facility, trigger_num, return_full_data=True)
                if existing_event:
                    existing_thread_ts = existing_event.get('thread_ts', '')
                    is_update = True
                    logger.info(f"Found existing event for {facility} trigger {trigger_num}, thread_ts: {existing_thread_ts}")
            except Exception as e:
                logger.error(f"Error checking existing event: {e}")
                existing_event = None
        
        # 3. Process visibility information if coordinates available
        plot_path = None
        visibility_info = None
        
        ra = notice_data.get('RA')
        dec = notice_data.get('DEC')
        
        if visibility_available and ra is not None and dec is not None:
            try:
                logger.info(f"Generating visibility analysis for {notice_data.get('Name', 'target')}")
                
                # New visibility plotter handles all 4 cases internally
                result = plotter.create_visibility_plot(
                    ra=ra,
                    dec=dec,
                    grb_name=notice_data.get('Name', ''),
                    test_mode=is_test,
                    minalt=MIN_ALTITUDE,
                    minmoonsep=MIN_MOON_SEP
                )
                
                if isinstance(result, tuple) and len(result) == 2:
                    plot_path, visibility_info = result
                    notice_data['visibility_info'] = visibility_info  # Store for comparison
                
                # Log the determined status
                if visibility_info:
                    status = visibility_info.get('status', 'unknown')
                    logger.info(f"Visibility status determined: {status}")
                    
                    if status == 'not_observable':
                        logger.info("No plot generated - target not observable")
                    elif status == 'observable_tomorrow':
                        logger.info("Generated tomorrow's sky plot with warning label")
                    else:
                        logger.info(f"Generated today's plot (show_current_time={status=='observable_now'})")
                
            except Exception as e:
                logger.error(f"Error creating visibility plot: {e}")
        
        # 4. Save to databases (skip if this is a test message)
        csv_status = False
        ascii_status = False
        
        if not is_test:
            # Save to CSV (always append new records)
            csv_status = notice_handler.save_to_csv(notice_data)
            
            # 4-1. Update existing event
            if is_update and existing_thread_ts:
                # For updates, save ASCII with existing thread_ts first
                ascii_status = notice_handler.save_to_ascii(notice_data, existing_thread_ts)
                
                # Compare data and generate thread message
                differences = _compare_event_data(existing_event, notice_data)
                
                if differences:  # Only send update if there are actual differences
                    thread_message = _format_thread_message(differences, notice_data)
                    
                    try:
                        # Send thread update message
                        thread_response = slack_client.chat_postMessage(
                            channel=slack_channel,
                            thread_ts=existing_thread_ts,
                            text=thread_message,
                            unfurl_links=False,
                            unfurl_media=False
                        )
                        logger.info(f"Sent thread update for {facility} trigger {trigger_num}")
                        
                        # Add visibility plot to thread if coordinates changed and plot available
                        if 'coordinates' in differences and plot_path and os.path.exists(plot_path):
                            try:
                                plot_title = f"Updated Visibility Plot: {notice_data.get('Name', 'Target')}"
                                if visibility_info and visibility_info.get('showing_tomorrow'):
                                    plot_title += " (Tomorrow's Sky)"
                                
                                slack_client.files_upload_v2(
                                    file_uploads=[{"file": plot_path}],
                                    channel=slack_channel,
                                    thread_ts=existing_thread_ts,
                                    title=plot_title
                                )
                                logger.info(f"Uploaded updated visibility plot to thread")
                            except Exception as plot_error:
                                logger.error(f"Error uploading plot to thread: {plot_error}")
                        
                        # Send ToO email if criteria are met
                        _send_too_email_if_criteria_met(notice_data, visibility_info)
                                
                    except Exception as e:
                        logger.error(f"Error sending thread update: {e}")
                
                # Clean up plot file if it was created
                if plot_path and os.path.exists(plot_path) and not plot_path.startswith('./test_plots'):
                    try:
                        os.remove(plot_path)
                    except Exception as e:
                        logger.warning(f"Error removing plot file: {e}")
                
                return True, "Thread update sent successfully"
            
            # 4-2. New event
            else:
                # First save to ASCII without thread_ts to get correct status
                try:
                    ascii_status = notice_handler.save_to_ascii(notice_data)
                    logger.info(f"Initial ASCII save completed with status: {ascii_status}")
                except Exception as e:
                    logger.error(f"Error with initial ASCII save: {e}")
                    ascii_status = False
                
                # Format the full message for Slack with correct ASCII status
                slack_message, lc_url, notice_url = format_message_for_slack(
                    topic=topic,
                    value=value,
                    csv_status=csv_status,
                    ascii_status=ascii_status,  # Now shows correct status
                    test_mode=is_test,
                    notice_data=notice_data
                )

                if slack_message is None:
                    return False, "Message formatting failed"
                
                # Add visibility blocks if available
                visibility_blocks = []
                if visibility_info:
                    visibility_text = plotter.format_visibility_message(visibility_info)
                    visibility_blocks = [
                        {"type": "divider"},
                        {
                            "type": "header",
                            "text": {"type": "plain_text", "text": "Visibility Information"}
                        },
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": visibility_text}
                        }
                    ]
                
                # Combine all blocks
                message_blocks = slack_message.get('blocks', []) + visibility_blocks

                # Add ToO button if GRB keywords are present
                grb_keywords = ['GRB', 'Fermi', 'Swift', 'IceCube', 'HAWC', 'AMON']
                if any(keyword.lower() in topic.lower() for keyword in grb_keywords):
                    if too_integration:
                        try:
                            enhanced_blocks = too_integration.add_too_button_to_message(message_blocks, notice_data)
                            message_blocks = enhanced_blocks
                            logger.info(f"Added ToO button to {notice_data.get('Name', 'target')} alert")
                        except Exception as e:
                            logger.warning(f"Failed to add ToO button: {e}")

                # Send the main message
                if not is_test or (is_test and TEST_SEND_TO_SLACK):
                    try:
                        response = slack_client.chat_postMessage(
                            channel=slack_channel,
                            blocks=message_blocks,
                            text=f"GCN Alert: {notice_data.get('Name', 'New Target')}",
                            unfurl_links=False,
                            unfurl_media=False
                        )
                        
                        new_thread_ts = response['ts']
                        logger.info(f"Sent new message for {facility} trigger {trigger_num}, thread_ts: {new_thread_ts}")
                        
                        # Update ASCII entry with thread_ts
                        try:
                            ascii_update_status = notice_handler.save_to_ascii(notice_data, new_thread_ts)
                            logger.info(f"Updated ASCII entry with thread_ts: {new_thread_ts}, status: {ascii_update_status}")
                        except Exception as e:
                            logger.error(f"Error updating ASCII entry with thread_ts: {e}")
                            # Note: Don't update ascii_status here as the message already shows correct initial status
                        
                        # Send consolidated URL message to thread
                        url_messages = []
                        
                        if lc_url:
                            url_messages.append(f"*Light Curve:* <{lc_url}|View Light Curve>")
                        if notice_url:
                            url_messages.append(f"*Original Notice:* <{notice_url}|View GCN Notice>")
                        
                        if url_messages:
                            try:
                                combined_url_message = "\n".join(url_messages)
                                if lc_url:
                                    combined_url_message += "\n_(Note: Light curve images may take a few minutes to generate)_"
                                
                                slack_client.chat_postMessage(
                                    channel=slack_channel,
                                    thread_ts=new_thread_ts,
                                    text=combined_url_message,
                                    unfurl_links=False
                                )
                                logger.info(f"Sent consolidated URL message to thread")
                            except Exception as e:
                                logger.error(f"Error sending URL message: {e}")
                        
                        # Add visibility plot as thread reply (if available)
                        if plot_path and os.path.exists(plot_path):
                            try:
                                plot_title = f"Visibility Plot: {notice_data.get('Name', 'Target')}"
                                if visibility_info and visibility_info.get('showing_tomorrow'):
                                    plot_title += " (Tomorrow's Sky)"
                                
                                slack_client.files_upload_v2(
                                    file_uploads=[{"file": plot_path}],
                                    channel=slack_channel,
                                    thread_ts=new_thread_ts,
                                    title=plot_title
                                )
                                logger.info(f"Uploaded visibility plot to thread")
                            except Exception as plot_error:
                                logger.error(f"Error uploading plot to thread: {plot_error}")
                        
                        # Send ToO email if criteria are met
                        _send_too_email_if_criteria_met(notice_data, visibility_info)
                        
                        # Clean up plot file if it was created (for successful message sending)
                        if plot_path and os.path.exists(plot_path) and not plot_path.startswith('./test_plots'):
                            try:
                                os.remove(plot_path)
                            except Exception as e:
                                logger.warning(f"Error removing plot file: {e}")
                        
                        return True, "New message sent successfully"
                                
                    except Exception as e:
                        logger.error(f"Error sending Slack message: {e}")
                        return False, f"Error sending message: {str(e)}"
                else:
                    logger.info("Test mode without --send flag: Skipping Slack message")
                    # Clean up plot file if it was created (even in test mode)
                    if plot_path and os.path.exists(plot_path) and not plot_path.startswith('./test_plots'):
                        try:
                            os.remove(plot_path)
                        except Exception as e:
                            logger.warning(f"Error removing plot file: {e}")
                    return True, "Test completed (no messages sent)"
        else:
            logger.info("Test mode: Skipping database save")
            return True, "Test mode - processing completed"
        
    except Exception as e:
        logger.error(f"Error processing notice: {e}", exc_info=True)
        return False, str(e)

############################## Main Loop ##############################
def main():
    """Enhanced main function with ToO integration support."""
    global running, TEST_SEND_TO_SLACK, consumer, socket_handler  # ADD socket_handler to global
    
    # Update TEST_SEND_TO_SLACK based on args
    TEST_SEND_TO_SLACK = args.send
    
    # Start connection monitoring thread
    monitor_thread = Thread(target=check_connection, daemon=True)
    monitor_thread.start()
    
    logger.info("Starting Enhanced GCN Slack Bot with ToO Integration... (Press Ctrl+C to stop)")
    
    # Setup Slack handlers for ToO integration
    setup_slack_handlers()
    
    # Start Socket Mode handler for interactive features
    if app and SLACK_APP_TOKEN:
        try:
            socket_handler = SocketModeHandler(app, SLACK_APP_TOKEN)
            socket_thread = threading.Thread(
                target=socket_handler.start,
                daemon=True,
                name="SlackSocketMode"
            )
            socket_thread.start()
            logger.info("Socket Mode handler started for ToO interactive features")
        except Exception as e:
            logger.error(f"Failed to start Socket Mode: {e}")
    else:
        logger.warning("Socket Mode not available - ToO buttons will not be interactive")
    
    # Try to authenticate with Slack
    try:
        test_response = slack_client.api_test()
        if not test_response["ok"]:
            logger.error(f"Slack authentication failed: {test_response.get('error', 'Unknown error')}")
            logger.warning("Continuing without Slack notifications")
    except Exception as e:
        logger.error(f"Error connecting to Slack: {e}")
        logger.warning("Continuing without Slack notifications")
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    try:
        while running:
            try:
                # Use consumer with lock for thread safety
                with consumer_lock:
                    current_consumer = consumer
                
                # Consume messages with timeout
                messages = current_consumer.consume(timeout=1)
                
                # Reset error counter on successful consume
                consecutive_errors = 0
                
                for message in messages:
                    # Check if we should stop
                    if not running: 
                        break
                        
                    # Check for errors
                    if message.error():
                        error_msg = str(message.error())
                        error_code = message.error().code() if hasattr(message.error(), 'code') else None
                        
                        logger.error("Consumer error: %s (code: %s)", error_msg, error_code)
                        
                        # Check if this is a connection-related error
                        connection_error_keywords = [
                            'transport', 'broker', 'connection', 'ssl', 'handshake', 
                            'timeout', 'disconnected', 'refused'
                        ]
                        
                        if any(keyword in error_msg.lower() for keyword in connection_error_keywords):
                            logger.warning("Detected connection error, triggering reconnection")
                            trigger_reconnection()
                        
                        # Send error notification to Slack
                        try:
                            slack_client.chat_postMessage(
                                channel=SLACK_CHANNEL,
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
                                            "text": f"*Error:* ```{error_msg}```\n*Code:* {error_code}"
                                        }
                                    }
                                ]
                            )
                        except SlackApiError as e:
                            logger.error("Error sending error notification to Slack: %s", e.response['error'])
                        continue
                    
                    # Get topic and value
                    topic = message.topic()
                    value = message.value()
                    
                    # Update heartbeat timestamp if it's a heartbeat message
                    if topic == 'gcn.heartbeat':
                        update_heartbeat()
                        continue
                    
                    # For topic logging
                    logger.info('topic=%s, offset=%d', topic, message.offset())
                    logger.debug("Message value: %s", value)
                    
                    # Process notice and send message
                    try:
                        success, response = process_notice_and_send_message(
                            topic, value, slack_client, SLACK_CHANNEL
                        )
                        
                        if success:
                            logger.info(f"Successfully processed notice from {topic}")
                        else:
                            logger.warning(f"Issue processing notice from {topic}: {response}")
                    except Exception as process_error:
                        logger.error(f"Error processing message from {topic}: {process_error}", exc_info=True)
                    
            except Exception as e:
                consecutive_errors += 1
                error_msg = str(e)
                
                logger.error(f"Main loop error ({consecutive_errors}/{max_consecutive_errors}): {error_msg}")
                
                # Check if this looks like a connection error
                connection_error_keywords = [
                    'transport', 'broker', 'connection', 'ssl', 'handshake', 
                    'timeout', 'disconnected', 'refused', 'kafka'
                ]
                
                if any(keyword in error_msg.lower() for keyword in connection_error_keywords):
                    logger.warning("Main loop detected connection error, triggering reconnection")
                    trigger_reconnection()
                
                # If too many consecutive errors, take a longer break
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({max_consecutive_errors}). "
                               f"Taking longer break...")
                    if running:
                        time.sleep(30)  # Longer wait after many errors
                    consecutive_errors = 0  # Reset counter
                elif running:
                    # Brief pause before retrying
                    time.sleep(min(consecutive_errors * 2, 10))  # Gradual backoff up to 10 seconds
                    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        running = False
    except Exception as e:
        logger.error(f"Critical error in main process: {e}", exc_info=True)
    finally:
        logger.info("Shutting down Enhanced GCN Alert Monitor...")
        running = False
        
        # Close Socket Mode handler
        if socket_handler:
            try:
                socket_handler.close()
                logger.info("Socket Mode handler closed")
            except Exception as e:
                logger.warning(f"Error closing Socket Mode handler: {e}")
        
        # Close consumer safely
        with consumer_lock:
            if consumer:
                try:
                    consumer.close()
                    logger.info("Consumer closed successfully")
                except Exception as e:
                    logger.warning(f"Error closing consumer: {e}")


if __name__ == "__main__":
    main()