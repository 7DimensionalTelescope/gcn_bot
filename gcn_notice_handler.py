#!/usr/bin/env python3
"""
GCN Notice Handler
================
Processes and manages GCN notices data.

Basic Information
----------------
Author:         YoungPyo Hong
Created:        2025-01-21
Version:        1.2.0
License:        MIT
Copyright:      (c) 2025 YoungPyo Hong

Technical Requirements
---------------------
Dependencies:
    - 'pandas'            : Data management and CSV file handling
    - 're'                : Regular expression operations
    - 'datetime'          : Time handling
    - 'json'              : JSON parsing
    - 'string'            : String constants (for ASCII_UPPERCASE)
    - 'logging'           : Error and activity logging

Key Features
-----------
Data Processing:
    - Swift/Fermi classic text format parsing
    - Einstein Probe JSON format parsing
    - IceCube classic text format parsing
    - Comprehensive error logging

Event Management:
    - Automated GRB name generation based on date sequence
    - Unique event identification using trigger numbers
    - Chronological sorting of events
    - CSV storage with append capability

Monitored Facilities:
    - Swift (BAT, XRT)     : Classic text format
    - Fermi (GBM, LAT)     : Classic text format
    - Einstein Probe       : JSON format
    - IceCube              : Classic text format

Data Storage:
    - Automated CSV file and ASCII file creation and management
    - Standardized data format
    - Chronological event ordering
    - Data validation and verification
    - Detailed activity logging

Strict Parsing Mode:
    - 'strict_parsing' : Enable strict parsing mode (default: False)
    - Lose entire data when parsing notices that are not in the expected format
    - Set False if you don't want to lose entire data and want to get partial data that matches the expected format

Usage
-----
Parameters:
    1. Default Parameters:
        - 'output_csv'     : CSV file to store the processed notices (default: 'gcn_notices.csv')
        - 'output_ascii'   : ASCII file to store the processed notices (default: 'grb_targets.ascii')
        - 'ascii_max_events' : Maximum number of events to store in the ASCII file (default: 10)
        - 'strict_parsing' : Enable strict parsing mode (default: False)

    2. Required Parameters:
        - 'message_text'   : The text content of the notice 
        - 'topic'          : The topic of the GCN notices (e.g. 'gcn.notices.einstein_probe.wxt.notice')

Usage Example:
    from gcn_notice_handler import GCNNoticeHandler

    # Initialize handler
    handler = GCNNoticeHandler(output_csv='gcn_notices.csv'
                               output_ascii='grb_targets.ascii',
                               ascii_max_events=10,
                               strict_parsing=False)

    # Process a notice
    notice_data = handler.parse_notice(message_text, topic)
    if notice_data:
        csv_success = handler.save_to_csv(notice_data)
        ascii_success = handler.save_to_ascii(notice_data)
        print("CSV:", csv_success)
        print("ASCII:", ascii_success)
    
    # Check logs for detailed operation information
    # Logs are saved in 'gcn_notice_handler.log'

CSV Format
---------
Columns:
    - 'name'            : Temporary unique GRB identifier (e.g., 'GRB 250119A')
    - 'facility'            : Source facility name
    - 'discovery_date'        : Event date in YYYY-MM-DD format
    - 'discovery_time'        : Event time in HH:MM:SS.sss format
    - 'ra'                  : Right Ascension in degrees
    - 'dec'                 : Declination in degrees
    - 'trigger_num'         : Facility-specific trigger number

ASCII Format
------------
Columns:
    - 'GCN_ID'              : Temporary unique identifier of GCN alert ('GCN_(Facility)_(Trigger_num)') (This will be repalced by gcn_circular_hander.py)
    - 'Name'                : Temporary unique GRB identifier (e.g., 'GRB 250119A') (This will be repalced by gcn_circular_hander.py)
    - 'RA'                  : Right Ascension in degrees
    - 'DEC'                 : Declination in degrees
    - 'Discovery_UTC'       : Event discovery time in YYYY-MM-DD HH:MM:SS.sss format
    - 'Facility'            : Source facility name
    - 'Trigger_num'         : Facility-specific trigger number

Logging
-------
The handler logs all operations to 'gcn_notice_handler.log', including:
    - Notice parsing attempts and results
    - Data saving operations
    - Error messages and stack traces
    - File operations
    - Data validation results

Error Handling
-------------
All operations are wrapped in try-except blocks with:
    - Detailed error logging
    - Graceful failure recovery
    - User-friendly error messages
    - Operation status tracking

Change Log
----------
1.2.0 / 2025-03-12
    - Enhanced date parsing with multiple formats
    - Added caching for GRB name generation
    - Improved logging with more context
    - Added robust error handling for CSV operations
    - Added CSV verification functionality
    - Standardized numeric formatting to 2 decimal places
    - Removed microseconds from datetime values

1.1.0 / 2025-03-06
    - Deal with IceCube classic text format properly

1.0.4 / 2025-02-13
    - Added 'strict_parsing' option to control parsing behavior
    - Fix the bug in the '_parse_notice_einstein_probe' function ('ra_dec_error' is missing)

1.0.3 / 2025-02-07
    - Explited '_parse_notice_fermi_and_swift' function to deal with different date/time formats

1.0.2 / 2025-02-05
    - Added double quotes to 'Name' column in ASCII format
    - Added more error handling and logging

1.0.1 / 2025-02-04
    - Fix the bug in the 'save_to_csv' method ('save_to_ascii' existed in 'save_to_csv')
    - Updated documentation

1.0.0 / 2025-01-31
    - Removed time window functionality
    - Added comprehensive logging system
    - Renamed parsing functions for consistency
    - Enhanced error handling and reporting
    - Added operation status tracking
    - Added save status feedback for Slack integration

0.1.0 / 2025-01-21
    - Initial version release
    - Basic GCN notice parsing functionality
    - CSV storage implementation
    - Time window-based event matching
"""

import pandas as pd
import re
import os
import csv
import shutil
import glob
from datetime import datetime
from threading import Lock
from typing import Dict, Any, Optional, Union, List, Tuple
import json
from string import ascii_uppercase, ascii_lowercase
import logging
import time

# Set up enhanced logging
def setup_logger():
    """Configure a structured logger for the GCN Notice Handler."""
    logger = logging.getLogger(__name__)
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatter with more information
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler('gcn_notice_handler.log')
    file_handler.setFormatter(formatter)
    
    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    # Set level
    logger.setLevel(logging.INFO)
    
    return logger

# Initialize logger
logger = setup_logger()

class GCNNoticeHandler:
    """
    Parse and process GCN notices from monitored facilities.

    The GCNNoticeHandler class is used to parse and process GCN notices from monitored facilities.
    It provides methods for parsing notices, extracting relevant information, and storing the results
    in a CSV file.

    Args:
    ----
        - output_csv (str): The path to the output CSV file. (default: 'gcn_notices.csv')
        - output_ascii (str): The path to the output ASCII file. (default: 'grb_targets.ascii')
        - ascii_max_events (int): The maximum number of events to store in the ASCII file. (default: 10)
        - strict_parsing (bool): Enable strict parsing mode. (default: False)

    Attributes:
    ----------
        - monitored_facilities (dict): A dictionary of monitored facilities and their corresponding topics.
        - csv_columns (list): The columns in the output CSV file.
        - ascii_columns (list): The columns in the output ASCII file.
        - output_csv (str): The path to the output CSV file.
        - output_ascii (str): The path to the output ASCII file.
        - ascii_max_events (int): The maximum number of events to store in the ASCII file.
    """
    
    def __init__(self, output_csv='gcn_notices.csv', output_ascii='grb_targets.ascii',
                ascii_max_events=10, strict_parsing=False):
        """
        Initialize the GCN Notice Handler.
        
        Args:
            output_csv (str): The path to the output CSV file. (default: 'gcn_notices.csv')
            output_ascii (str): The path to the output ASCII file. (default: 'grb_targets.ascii')
            ascii_max_events (int): The maximum number of events to store in the ASCII file. (default: 10)
            strict_parsing (bool): Enable strict parsing mode. (default: False)
        """
        self.file_lock = Lock()
        self.strict_parsing = strict_parsing
        
        # For CSV verification
        self._last_verification = None
        
        # Monitored facilities
        self.monitored_facilities = {
            'SwiftBAT': ['SWIFT_BAT_GRB_POS_ACK'],
            'SwiftXRT': ['SWIFT_XRT_POSITION'],
            'SwiftUVOT': ['SWIFT_UVOT_POS'],
            
            'FermiGBM': ['FERMI_GBM_GND_POS', 'FERMI_GBM_FIN_POS', 'FERMI_GBM_FLT_POS'],
            'FermiLAT': ['FERMI_LAT_OFFLINE'],
            
            'AMON': ['AMON_NU_EM_COINC'],
            'IceCubeCASCADE': ['ICECUBE_CASCADE'],
            'HAWC': ['HAWC_BURST_MONITOR'],
            'IceCubeBRONZE': ['ICECUBE_ASTROTRACK_BRONZE'],
            'IceCubeGOLD': ['ICECUBE_ASTROTRACK_GOLD'],
            
            'CALET': ['CALET_GBM_FLT_LC'],
            
            'EinsteinProbe': ['einstein_probe'] # JSON
        }
        
        # CSV file
        self.output_csv = output_csv
        self.csv_columns = [
            'GCN_ID',        # GCN_SwiftXRT_123456
            'Name',          # GRB 250119A
            'RA',            # 250.05 (deg)
            'DEC',           # 25.0   (deg)
            'Error',         # 0.3    (deg)
            'Discovery_UTC', # 2025-01-19 00:00:00 UTC
            'Facility',      # SwiftXRT
            'Trigger_num',   # 123456
            'Notice_date'    # 2025-01-19 00:00:10 UTC
        ]
        
        # Ascii file
        self.output_ascii = output_ascii
        self.ascii_max_events = ascii_max_events
        self.ascii_columns = [
            'GCN_ID',
            'Name', 
            'RA',
            'DEC',
            'Error',
            'Discovery_UTC',
            'Primary_Facility',      # Notice = First detector
            'Best_Facility',         # Initially same as Primary
            'All_Facilities',        # Start with just this facility
            'Trigger_num',           # From notice
            'Notice_date',           # When notice was processed
            'Last_Update',           # Same as Notice_date initially
            'Redshift',              # Usually empty from notices
            'Host_info',             # Usually empty from notices
            'thread_ts'              # Slack thread timestamp
        ]
    PATTERNS = {
        'fermi': {
            'ra': r"GRB_RA:.*?(\d+\.\d+)d.*?\(J2000\)",
            'dec': r"GRB_DEC:.*?([-+]?\d+\.\d+)d.*?\(J2000\)",
            'error': r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            'date': r"GRB_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            'time': r"GRB_TIME:.*?{([\d:\.]+)}\s*UT",
            'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
            'notice_date': r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT"  # Add this line
        },
        'swift': {
            'ra': r"(?:GRB_RA|POINT_RA):.*?(\d+\.\d+)d?.*\(J2000\)",
            'dec': r"(?:GRB_DEC|POINT_DEC):.*?([-+]?\d+\.\d+)d?.*\(J2000\)",
            'error': r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            'date': r"(?:GRB_DATE|IMG_START_DATE):.*?(\d{2})/(\d{2})/(\d{2})",
            'time': r"(?:GRB_TIME|IMG_START_TIME):.*?{([\d:\.]+)}\s*UT",
            'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
            'notice_date': r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT"  # Add this line
        },
        'amon': {
            'ra': r"SRC_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
            'dec': r"SRC_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
            'error': r"SRC_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            'date': r"DISCOVERY_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            'time': r"DISCOVERY_TIME:.*?{([\d:\.]+)}\s*UT",
            'trigger_num': r"EVENT_NUM:\s*(\d+)",
            'notice_date': r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT"  # Add this line
        },
        'calet': {
            'ra': r"POINT_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
            'dec': r"POINT_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
            'error': None,
            'date': r"TRIGGER_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            'time': r"TRIGGER_TIME:.*?{([\d:\.]+)}\s*UT",
            'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
            'notice_date': r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT"  # Add this line
        }
    }
    
    def _normalize_facility_name(self, facility: str) -> str:
        """
        Normalize facility names for consistent comparison across different instruments.
        
        This method standardizes facility names to group related instruments together.
        For example, all Swift instruments (SwiftBAT, SwiftXRT, SwiftUVOT) are normalized to 'Swift'.
        
        Args:
            facility (str): The original facility name
            
        Returns:
            str: The normalized facility name
        """
        if not facility:
            return ""
        
        facility = facility.strip()
        
        # Swift family - all Swift instruments are considered the same mission
        swift_names = ['Swift', 'SwiftBAT', 'SwiftXRT', 'SwiftUVOT', 'Swift-BAT', 'Swift-XRT', 'Swift-UVOT']
        for name in swift_names:
            if name.lower() in facility.lower():
                return 'Swift'
        
        # Fermi family - all Fermi instruments are considered the same mission
        if any(x in facility for x in ['Fermi', 'GBM', 'LAT']):
            return 'Fermi'
        
        # GECAM instruments
        if 'GECAM' in facility:
            return 'GECAM'
        
        # SVOM instruments
        if 'SVOM' in facility:
            return 'SVOM'
        
        # Einstein Probe instruments
        if 'Einstein' in facility or 'EP' in facility:
            return 'Einstein Probe'
        
        # IceCube variations
        if 'IceCube' in facility or 'ICECUBE' in facility:
            return 'IceCube'
        
        # For other facilities, return as-is
        return facility

    def _get_facility(self, topic: str) -> Optional[str]:
        """
        Determine the facility from the topic.
        
        Args:
            topic (str): The topic of the GCN notice.
            
        Returns:
            str or None: The facility name if found, None otherwise
        """
        for facility, topics in self.monitored_facilities.items():
            if any(t in topic for t in topics):
                return facility
        return None
    
    def _find_existing_event(self, facility: str, trigger_num: str, return_full_data: bool = False) -> Optional[Union[str, Dict[str, Any]]]:
        """
        Unified method to find existing event from ASCII file.
        
        Args:
            facility (str): The facility name
            trigger_num (str): The trigger number
            return_full_data (bool): If True, return full event data; if False, return only GRB name
            
        Returns:
            Optional[Union[str, Dict[str, Any]]]: 
                - If return_full_data=False: GRB name (str) or None
                - If return_full_data=True: Full event data (dict) or None
        """
        if not facility or not trigger_num:
            return None
            
        try:
            # Load ASCII file directly without caching
            if not os.path.exists(self.output_ascii):
                logger.info(f"ASCII file does not exist: {self.output_ascii}")
                return None
                
            df = pd.read_csv(self.output_ascii, sep=r'\s+', 
                            quotechar='"', quoting=csv.QUOTE_NONNUMERIC, 
                            dtype=str, na_filter=False)
            
            if df.empty:
                logger.info("ASCII file is empty")
                return None
            
            # Normalize the search facility name
            normalized_facility = self._normalize_facility_name(facility)
            
            # Search for matching event
            for _, row in df.iterrows():
                row_trigger = str(row.get('Trigger_num', '')).strip()
                
                # Check trigger number first (exact match)
                if row_trigger != str(trigger_num).strip():
                    continue
                    
                # Check facility match - look in All_Facilities column
                all_facilities = str(row.get('All_Facilities', '')).strip()
                if not all_facilities:
                    continue
                    
                # Split facilities and normalize each one
                facilities_list = [f.strip() for f in all_facilities.split(',')]
                normalized_facilities = [self._normalize_facility_name(f) for f in facilities_list]
                
                # Check if our normalized facility matches any in the list
                if normalized_facility in normalized_facilities:
                    if return_full_data:
                        logger.info(f"Found existing event for {facility} trigger {trigger_num}, thread_ts: {row.get('thread_ts', '')}")
                        return row.to_dict()
                    else:
                        grb_name = row.get('Name', '').strip().strip('"')
                        logger.info(f"Found existing event: {grb_name} for {facility} trigger {trigger_num}")
                        return grb_name
            
            logger.info(f"No existing event found for {facility} trigger {trigger_num}")
            return None
            
        except Exception as e:
            logger.error(f"Error finding existing event: {e}")
            return None
        
    def _generate_grb_name(self, trigger_date: datetime, facility: str, df: pd.DataFrame) -> str:
        """
        Generate a consistent name with different prefixes based on facility.
        - GRB YYMMDD[A-Z] for most events
        - EP YYMMDD[a-z] for Einstein Probe events  
        - IceCube-YYMMDD[A-Z] for IceCube events
        
        Simplified version without caching - relies on file operations only.
        """
        # Determine the prefix and alphabet based on the facility
        is_einstein_probe = "EinsteinProbe" in facility
        is_icecube = any(ice_fac in facility for ice_fac in ["IceCube", "AMON"])
        
        if is_icecube:
            prefix, name_format, alphabet = "IceCube", "dash", ascii_uppercase
        elif is_einstein_probe:
            prefix, name_format, alphabet = "EP", "space", ascii_lowercase
        else:
            prefix, name_format, alphabet = "GRB", "space", ascii_uppercase

        date_key = trigger_date.strftime('%y%m%d')
        used_letters = set()

        # Find used letters for this prefix and date from the existing DataFrame
        if not df.empty and 'Name' in df.columns:
            name_pattern = re.compile(rf"^{prefix}[-\s]{date_key}([A-Za-z])$")
            for name in df['Name']:
                match = name_pattern.match(str(name).strip().strip('"'))
                if match:
                    used_letters.add(match.group(1))

        # Find the next available letter
        next_letter = next((letter for letter in alphabet if letter not in used_letters), alphabet[-1])
        
        # Format the new name
        separator = "-" if name_format == "dash" else " "
        new_name = f"{prefix}{separator}{date_key}{next_letter}"
        
        logger.info(f"Generated new name: {new_name}")
        return new_name

    @staticmethod
    def _normalize_error_to_deg(value, unit):
        """
        Convert error values to degrees.

        This function supports conversion from arcmin, arcsec, and deg units 
        to a standardized degree representation.

        Args:
            value (float or str): Error value to be converted
            unit (str): Original unit (arcmin, arcsec, or deg)
        
        Returns:
            float: Error value converted to degrees
        """
        try:
            # Convert input to float
            value = float(value)
            
            # Conversion factors
            if unit.lower() == 'arcmin':
                # 1 degree = 60 arcmin
                return value / 60.0
            elif unit.lower() == 'arcsec':
                # 1 degree = 3600 arcsec
                return value / 3600.0
            elif unit.lower() == 'deg':
                return value
            
            logger.warning(f"Unknown unit {unit}, returning original value")
            return value
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting to degrees: {e}")
            return value

    def _create_notice_data(self, ra, dec, error, trigger_date, facility, notice_date, trigger_num='', **kwargs):
        """Create standardized notice data dictionary with formatted numbers."""
        try:
            # Format numeric values to 2 decimal places
            if ra is not None:
                ra = round(float(ra), 2)
            if dec is not None:
                dec = round(float(dec), 2)
            if error is not None:
                error = round(float(error), 2)
                    
            # Remove microseconds from datetimes
            if trigger_date is not None:
                trigger_date = trigger_date.replace(microsecond=0)
            if notice_date is not None:
                notice_date = notice_date.replace(microsecond=0)
            
            notice_data = {
                'GCN_ID': f"GCN_{facility}_{trigger_num}",
                'Name': '',
                'RA': ra if ra is not None else '',
                'DEC': dec if dec is not None else '',
                'Error': error if error is not None else '',
                'Discovery_UTC': trigger_date if trigger_date else '',
                'Facility': facility if facility else '',
                'Trigger_num': str(trigger_num) if trigger_num else '',
                'Notice_date': notice_date
            }
            
            # Add IceCube metadata as an extra field (not for DB storage)
            if 'icecube_info' in kwargs:
                notice_data['icecube_info'] = kwargs['icecube_info']
            
            return notice_data
        except Exception as e:
            logger.error(f"Error creating notice data: {e}")
            raise

    def _parse_text_notice(self, text: str, facility: str, patterns: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Core parsing function for all notice types.
        
        Args:
            text (str): Formatted text of the notice.
            facility (str): Facility of the notice.
            patterns (dict): Dictionary of regular expressions to match.
        
        Returns:
            notice_data (dict): Parsed notice data.
        """
        # Initialize data dictionary
        parsed_data = {
            'ra': None,
            'dec': None,
            'error': None,
            'trigger_date': None,
            'trigger_num': None,
            'notice_date': datetime.now()
        }
        
        try:
            # Try to match each patterns
            matches = {key: re.search(pattern, text, re.DOTALL) if pattern else None 
                    for key, pattern in patterns.items()}
            
            # In strict parsing mode, check if all patterns matched
            if self.strict_parsing:
                missing_patterns = [key for key, match in matches.items() if not match]
                if missing_patterns:
                    logger.error(f"Missing patterns in strict parsing mode: {facility}: "
                                 f"{', '.join(missing_patterns)}")
                    return None
                else:
                    logger.info(f"All patterns matched in strict parsing mode: {facility}")
            
            # In non-strict parsing mode, at least one pattern must match
            else:
                matched_patterns = [key for key, match in matches.items() if match]
                if not matched_patterns:
                    logger.error(f"No patterns matched in non-strict parsing mode: {facility}")
                    return None
                else:
                    logger.info(f"Patterns matched in non-strict parsing mode: {facility}: "
                                 f"{', '.join(matched_patterns)}")
            
            logger.debug(f"Matches: {matches}")
            
            # Parse matched patterns
            # Parse 'RA/DEC'
            if matches.get('ra'):
                try:
                    parsed_data['ra'] = float(matches['ra'].group(1))
                    logger.debug(f"Successfully parsed 'RA' value: {parsed_data['ra']}")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse 'RA' value: {e}")
            else:
                logger.warning(f"Could not find 'RA' value in notice: {facility}")
            
            if matches.get('dec'):
                try:
                    parsed_data['dec'] = float(matches['dec'].group(1))
                    logger.debug(f"Successfully parsed 'Dec' value: {parsed_data['dec']}")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse 'Dec' value: {e}")
            else:
                logger.warning(f"Could not find 'Dec' value in notice: {facility}")
            
            # Parse 'trigger_date'
            if matches.get('date') and matches.get('time'):
                logger.debug(f"Trying to parse 'trigger_date': {matches['date']} {matches['time']}")
                try:
                    yy, mm, dd = matches['date'].groups()
                    time_str = matches['time'].group(1).strip()
                    # Add handling for different time formats
                    if '.' in time_str:
                        datetime_str = f"20{yy}-{mm}-{dd} {time_str}"
                        parsed_data['trigger_date'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        datetime_str = f"20{yy}-{mm}-{dd} {time_str}"
                        parsed_data['trigger_date'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    logger.debug(f"Successfully parsed 'trigger_date': {parsed_data['trigger_date']}")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse 'trigger_date': {e}")
            else:
                logger.warning(f"Could not find 'trigger_date' value in notice: {facility}")
            
            # Parse 'error'
            if matches.get('error'):
                try:
                    error_value = matches['error'].group(1)
                    error_unit = matches['error'].group(2)
                    parsed_data['error'] = self._normalize_error_to_deg(error_value, error_unit)
                    logger.debug(f"Successfully parsed 'error' value: {parsed_data['error']}")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse 'error' value: {e}")
            else:
                logger.warning(f"Could not find 'error' value in notice: {facility}")
            
            # Parse 'notice_date'
            if matches.get('notice_date'):
                try:
                    _, day, month_name, year, hour, minute, second = matches['notice_date'].groups()
                    month_number = datetime.strptime(month_name, '%b').month
                    # Add handling for different time formats
                    if '.' in second:
                        datetime_str = f"20{year}-{month_number}-{day} {hour}:{minute}:{second}"
                        parsed_data['notice_date'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
                    else:
                        datetime_str = f"20{year}-{month_number}-{day} {hour}:{minute}:{second}"
                        parsed_data['notice_date'] = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    logger.debug(f"Successfully parsed notice date: {parsed_data['notice_date']}")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse notice date: {e}")
            else:
                logger.warning(f"Could not find 'notice_date' value in notice: {facility}")
            
            # Parse 'trigger_num'
            if matches.get('trigger_num'):
                try:
                    parsed_data['trigger_num'] = matches['trigger_num'].group(1)
                    logger.debug(f"Successfully parsed trigger number: {parsed_data['trigger_num']}")
                except AttributeError as e:
                    logger.warning(f"Could not parse trigger number: {e}")
            else:
                logger.warning(f"Could not find 'trigger_num' value in notice: {facility}")
            
            # Format parsed values before '_create_notice_data'
            if parsed_data['ra'] is not None:
                parsed_data['ra'] = round(float(parsed_data['ra']), 2)
            if parsed_data['dec'] is not None:
                parsed_data['dec'] = round(float(parsed_data['dec']), 2)
            if parsed_data['error'] is not None:
                parsed_data['error'] = round(float(parsed_data['error']), 2)
            if parsed_data['trigger_date'] is not None:
                parsed_data['trigger_date'] = parsed_data['trigger_date'].replace(microsecond=0)
            if parsed_data['notice_date'] is not None:
                parsed_data['notice_date'] = parsed_data['notice_date'].replace(microsecond=0)
            
            # Log parsing results
            found_fields = [key for key, value in parsed_data.items() if value is not None]
            missing_fields = [key for key, value in parsed_data.items() if value is None]
            
            if found_fields:
                mode_str = "strict" if self.strict_parsing else "flexible"
                logger.info(f"Successfully parsed fields from {facility} ({mode_str} mode): "
                        f"{', '.join(found_fields)}")
                if missing_fields:
                    logger.warning(f"Missing fields from {facility}: {', '.join(missing_fields)}")
                return self._create_notice_data(
                    ra=parsed_data['ra'],
                    dec=parsed_data['dec'],
                    error=parsed_data['error'],
                    trigger_date=parsed_data['trigger_date'],
                    facility=facility,
                    notice_date=parsed_data['notice_date'],
                    trigger_num=parsed_data['trigger_num']
                )
            else:
                logger.error(f"No valid information found in {facility} notice")
                return None
        
        except Exception as e:
            logger.error(f"Error parsing {facility} notice: {str(e)} - Core parsing function")
            return None

    def _parse_notice_einstein_probe(self, text, facility):
        """Parse Einstein Probe format notices (JSON format)."""
        parsed_data = {
            'ra': None,
            'dec': None,
            'error': 0.0,
            'trigger_date': None,
            'notice_date': datetime.now().replace(microsecond=0), # Set to current time since JSON format doesn't have a notice date
            'trigger_num': 'UNKNOWN'
        }
        
        try:
            data = json.loads(text)
            
            # Try to parse RA
            if 'ra' in data:
                try:
                    parsed_data['ra'] = float(data['ra'])
                    logger.debug(f"Successfully parsed RA from {facility}: {parsed_data['ra']}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse RA from {facility}: {e}")
            
            # Try to parse Dec
            if 'dec' in data:
                try:
                    parsed_data['dec'] = float(data['dec'])
                    logger.debug(f"Successfully parsed Dec from {facility}: {parsed_data['dec']}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse Dec from {facility}: {e}")
            
            # Try to parse error
            if 'ra_dec_error' in data:
                try:
                    parsed_data['error'] = float(data['ra_dec_error'])
                    logger.debug(f"Successfully parsed error from {facility}: {parsed_data['error']}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse error from {facility}: {e}")
            
            # Try to parse trigger date
            if 'trigger_time' in data:
                try:
                    trigger_date = datetime.fromisoformat(data['trigger_time'].replace('Z', '+00:00'))
                    parsed_data['trigger_date'] = trigger_date.replace(tzinfo=None)
                    logger.debug(f"Successfully parsed trigger date from {facility}: {parsed_data['trigger_date']}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse trigger time from {facility}: {e}")
            
            # Try to parse trigger number
            if 'id' in data and isinstance(data['id'], list) and data['id']:
                try:
                    parsed_data['trigger_num'] = str(data['id'][0])
                    logger.debug(f"Successfully parsed trigger number from {facility}: {parsed_data['trigger_num']}")
                except (IndexError, TypeError) as e:
                    logger.warning(f"Could not parse trigger number from {facility}: {e}")
            
            # Format numeric values to 2 decimal places
            if parsed_data['ra'] is not None:
                parsed_data['ra'] = round(float(parsed_data['ra']), 2)
            if parsed_data['dec'] is not None:
                parsed_data['dec'] = round(float(parsed_data['dec']), 2)
            if parsed_data['error'] is not None:
                parsed_data['error'] = round(float(parsed_data['error']), 2)
            
            # Remove microseconds from datetimes
            if parsed_data['trigger_date'] is not None:
                parsed_data['trigger_date'] = parsed_data['trigger_date'].replace(microsecond=0)
            
            # Log results
            found_fields = [k for k, v in parsed_data.items() if v is not None]
            missing_fields = [k for k, v in parsed_data.items() if v is None]
            
            if found_fields:
                logger.info(f"Successfully parsed fields from {facility}: {', '.join(found_fields)}")
                if missing_fields:
                    logger.warning(f"Missing fields from {facility}: {', '.join(missing_fields)}")
                return self._create_notice_data(
                    ra=parsed_data['ra'],
                    dec=parsed_data['dec'],
                    error=parsed_data['error'],
                    trigger_date=parsed_data['trigger_date'],
                    facility=facility,
                    notice_date=parsed_data['notice_date'],
                    trigger_num=parsed_data['trigger_num']
                )
            else:
                logger.error(f"No valid information found in {facility} notice")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {facility} notice: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing {facility} notice: {e}")
            return None

    def _create_backup_with_limit(self, filepath: str, max_backups: int = 5) -> str:
        """
        Create a backup of the file and manage backup count to keep only the most recent ones.
        
        Args:
            filepath (str): Path to the file to backup
            max_backups (int): Maximum number of backup files to keep (default: 5)
        
        Returns:
            str: Path of the created backup file, or empty string if backup failed
        """
        if not os.path.exists(filepath):
            logger.debug(f"File {filepath} does not exist, skipping backup")
            return ""
        
        try:
            # Create new backup with timestamp
            backup_path = f"{filepath}.backup.{int(time.time())}"
            shutil.copy2(filepath, backup_path)
            logger.debug(f"Created backup: {backup_path}")
            
            # Clean up old backups - keep only the most recent ones
            self._cleanup_old_backups(filepath, max_backups)
            
            return backup_path
            
        except Exception as e:
            logger.error(f"Failed to create backup for {filepath}: {e}")
            return ""

    def _cleanup_old_backups(self, filepath: str, max_backups: int = 5) -> None:
        """
        Remove old backup files, keeping only the most recent ones.
        
        Args:
            filepath (str): Original file path (backups will be filepath.backup.*)
            max_backups (int): Maximum number of backup files to keep
        """
        try:
            # Find all backup files for this filepath
            backup_pattern = f"{filepath}.backup.*"
            backup_files = glob.glob(backup_pattern)
            
            if len(backup_files) <= max_backups:
                logger.debug(f"Only {len(backup_files)} backup files, no cleanup needed")
                return
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            # Identify files to remove
            files_to_remove = backup_files[max_backups:]
            
            # Remove old backup files
            removed_count = 0
            for old_backup in files_to_remove:
                try:
                    os.remove(old_backup)
                    removed_count += 1
                    logger.debug(f"Removed old backup: {old_backup}")
                except Exception as e:
                    logger.warning(f"Failed to remove backup file {old_backup}: {e}")
            
            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old backup files, keeping {max_backups} most recent")
                
        except Exception as e:
            logger.error(f"Error during backup cleanup: {e}")

#---------------------------------------Main Function----------------------------------------
    def parse_notice(self, formatted_text: Union[str, bytes], topic: str) -> Optional[Dict[str, Any]]:
        facility = self._get_facility(topic)
        if not facility:
            return None

        if isinstance(formatted_text, bytes):
            formatted_text = formatted_text.decode('utf-8', 'ignore')

        if 'EinsteinProbe' in facility:
            return self._parse_notice_einstein_probe(formatted_text, facility)
        
        # Simplified routing logic
        parser_key = None
        if 'Swift' in facility: parser_key = 'swift'
        elif 'Fermi' in facility: parser_key = 'fermi'
        elif any(f in facility for f in ['AMON', 'IceCube', 'HAWC']): parser_key = 'amon'
        elif 'CALET' in facility: parser_key = 'calet'

        if parser_key:
            return self._parse_text_notice(formatted_text, facility, self.PATTERNS[parser_key])
        
        logger.warning(f"No parser available for facility: {facility}")
        return None

    def save_to_csv(self, notice_data: Dict[str, Any]) -> bool:
        """
        Saves notice data to a CSV file using pandas for robustness and efficiency.
        """
        try:
            with self.file_lock:
                new_row_df = pd.DataFrame([notice_data]).reindex(columns=self.csv_columns)
                file_exists = os.path.exists(self.output_csv)
                new_row_df.to_csv(
                    self.output_csv, mode='a', header=not file_exists, index=False,
                    quoting=csv.QUOTE_MINIMAL
                )
                logger.info(f"Successfully saved entry for {notice_data.get('Name', 'N/A')} to {self.output_csv}")
                return True
        except Exception as e:
            logger.error(f"Failed to save to CSV file '{self.output_csv}': {e}", exc_info=True)
            return False

    def save_to_ascii(self, notice_data: Dict[str, Any], thread_ts: Optional[str] = None) -> bool:
        """
        Saves or updates notice data in a space-delimited ASCII file.
        """
        try:
            with self.file_lock:
                # --- 1. Load Existing Data ---
                try:
                    df = pd.read_csv(self.output_ascii, sep=r'\s+', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL, dtype=str, na_filter=False)
                    missing_cols = set(self.ascii_columns) - set(df.columns)
                    for col in missing_cols: df[col] = ''
                    df = df[self.ascii_columns].fillna('')
                except (pd.errors.EmptyDataError, FileNotFoundError):
                    df = pd.DataFrame(columns=self.ascii_columns)
                except Exception as load_error:
                    logger.warning(f"Failed to load '{self.output_ascii}', will recreate: {load_error}")
                    self._create_backup_with_limit(self.output_ascii, max_backups=5)
                    df = pd.DataFrame(columns=self.ascii_columns)

                # --- 2. Prepare Data and Find Existing Entry ---
                facility = str(notice_data.get('Facility', '')).strip()
                trigger_num = str(notice_data.get('Trigger_num', '')).strip()
                
                existing_idx = None
                if facility and trigger_num and not df.empty:
                    # Find by trigger number and normalized facility
                    normalized_facility = self._normalize_facility_name(facility)
                    
                    for idx, row in df.iterrows():
                        row_trigger = str(row.get('Trigger_num', '')).strip()
                        if row_trigger != trigger_num:
                            continue
                            
                        # Check if this facility family is already in All_Facilities
                        all_facilities = str(row.get('All_Facilities', '')).strip()
                        if all_facilities:
                            facilities_list = [f.strip() for f in all_facilities.split(',')]
                            normalized_facilities = [self._normalize_facility_name(f) for f in facilities_list]
                            
                            if normalized_facility in normalized_facilities:
                                existing_idx = idx
                                break

                # --- 3. Update or Append Logic ---
                if existing_idx is not None:
                    # UPDATE existing event
                    name = df.at[existing_idx, 'Name']
                    notice_data['Name'] = name
                    
                    # Update All_Facilities to include this specific facility
                    existing_facilities = set(str(df.at[existing_idx, 'All_Facilities']).split(','))
                    existing_facilities = {f.strip() for f in existing_facilities if f.strip()}
                    existing_facilities.add(facility)
                    
                    # Update fields
                    for col, value in notice_data.items():
                        if col in df.columns and str(value).strip():
                            df.at[existing_idx, col] = value
                    
                    # Update All_Facilities
                    df.at[existing_idx, 'All_Facilities'] = ','.join(sorted(existing_facilities))
                    df.at[existing_idx, 'Last_Update'] = notice_data.get('Notice_date', '')
                    
                    # Update thread_ts if provided
                    if thread_ts:
                        df.at[existing_idx, 'thread_ts'] = thread_ts
                        logger.info(f"Updated thread_ts for existing entry: {thread_ts}")
                        
                    logger.info(f"Updated existing entry for {facility} trigger {trigger_num}.")
                else:
                    # APPEND new event
                    if 'Name' not in notice_data or not notice_data['Name']:
                        name = self._generate_grb_name(notice_data['Discovery_UTC'], facility, df)
                        notice_data['Name'] = name
                    else:
                        name = notice_data['Name']
                        logger.info(f"Using pre-assigned name: {name}")
                    
                    row_data = {col: notice_data.get(col, '') for col in self.ascii_columns}
                    row_data.update({
                        'Primary_Facility': facility, 
                        'Best_Facility': facility,
                        'All_Facilities': facility, 
                        'Last_Update': notice_data.get('Notice_date', ''),
                        'thread_ts': thread_ts if thread_ts else ''
                    })
                    new_row_df = pd.DataFrame([row_data])
                    df = pd.concat([new_row_df, df], ignore_index=True)
                    logger.info(f"Added new entry for {name} with thread_ts: {thread_ts if thread_ts else 'empty'}")

                # --- 4. Trim DataFrame to Max Events ---
                if len(df) > self.ascii_max_events:
                    df['_sort_key'] = pd.to_datetime(df['Notice_date'], errors='coerce')
                    df = df.sort_values('_sort_key', ascending=False, na_position='last').head(self.ascii_max_events)
                    df = df.drop(columns=['_sort_key'])
                
                # --- 5. Backup and Save ---
                self._create_backup_with_limit(self.output_ascii, max_backups=5)
                
                # Ensure thread_ts column is preserved in output
                df.to_csv(
                    self.output_ascii, sep=' ', header=True, index=False,
                    quoting=csv.QUOTE_NONNUMERIC, quotechar='"',
                    columns=self.ascii_columns  # Explicitly specify column order
                )
                
                logger.info(f"ASCII file '{self.output_ascii}' saved successfully with {len(df)} entries.")
                return True

        except Exception as e:
            logger.error(f"A critical error occurred in save_to_ascii: {e}", exc_info=True)
            return False

#---------------------------------------Test Code----------------------------------------
if __name__ == "__main__":
    
    ######################## Setup for test ########################
    
    # Set output CSV file and time window
    csv_test_file = '/home/hongyp007/projects/GCN/gcn_bot/test_code/gcn_notices_test.csv'
    ascii_test_file = '/home/hongyp007/projects/GCN/gcn_bot/test_code/gcn_notices_test_ascii.ascii'
    
    # Test notices for different facilities
    test_cases = [
        # Einstein Probe (JSON format)
        {
            "topic": "gcn.notices.einstein_probe.wxt.notice",
            "notice": """{
                "trigger_time": "2025-01-13T01:20:44.949Z",
                "id": ["01709130131"],
                "ra": 94.224,
                "dec": 56.893,
                "ra_dec_error": 0.05094559
            }"""
        },
        
        # Swift BAT (Classic text format)
        {
            "topic": "gcn.classic.text.SWIFT_BAT_GRB_POS_ACK",
            "notice": """TITLE:           GCN/SWIFT NOTICE
NOTICE_DATE:     Fri 13 Jan 25 01:45:10 UT
NOTICE_TYPE:     Swift-BAT GRB Position ACK
TRIGGER_NUM:     1287821
GRB_RA:          16.0900d {+01h 04m 21.59s} (J2000)
GRB_DEC:         -12.1645d {-12d 09' 52.2"} (J2000)
GRB_ERROR:       4.4 [arcmin radius]
GRB_DATE:        25/01/13
GRB_TIME:        19927.17 SOD {05:32:07.17} UT"""
        },
        
        # Fermi GBM (Classic text format)
        {
            "topic": "gcn.classic.text.FERMI_GBM_FIN_POS",
            "notice": """TITLE:           GCN/FERMI NOTICE
NOTICE_DATE:     Fri 13 Jan 25 02:13:25 UT
NOTICE_TYPE:     Fermi-GBM Final Position
TRIGGER_NUM:     760683844
GRB_RA:          191.400d {+12h 45m 36s} (J2000)
GRB_DEC:         -11.820d {-11d 49' 11"} (J2000)
GRB_ERROR:       6.77 [deg radius]
GRB_DATE:        25/01/13
GRB_TIME:        19927.17 SOD {05:32:07.17} UT"""
        },
        
        # IceCube (Classic text format)
        {
            "topic": "gcn.classic.text.AMON_NU_EM_COINC",
            "notice": """TITLE:           GCN/AMON NOTICE
NOTICE_DATE:     Fri 13 Jan 25 03:20:15 UT
NOTICE_TYPE:     AMON_NU_EM_COINC
EVENT_NUM:       123456789
DISCOVERY_DATE:  25/01/13
DISCOVERY_TIME:  03:15:22.45 UT
SRC_RA:         120.500d {+08h 02m 00s} (J2000)
SRC_DEC:        -23.500d {-23d 30' 00"} (J2000)
ERROR_RADIUS:   0.5 [deg]"""
        }
    ]
    
    ######################## Process and save notices ########################
    
    # Initialize handler
    handler = GCNNoticeHandler(
        output_csv=csv_test_file,
        output_ascii=ascii_test_file,
        ascii_max_events=10,
        strict_parsing=False
    )
    # Configure logging only for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/home/hongyp007/projects/GCN/gcn_bot/test_code/gcn_notice_handler.log', mode='w'),
            logging.StreamHandler()
        ]
    )
    test_logger = logging.getLogger(__name__)
    
    # Parse and save notices
    for test_case in test_cases:
        test_logger.info(f"\nTesting {test_case['topic']}...")
        
        # Parse notices
        result = handler.parse_notice(
            formatted_text=test_case['notice'],
            topic=test_case['topic']
        )
        # Save results if parsing is successful
        if result:
            test_logger.info("Parsing successful!")
            test_logger.info(f"Parsed data: {result}")            
            
            # Save to CSV and ASCII
            csv_success = handler.save_to_csv(result)
            ascii_success = handler.save_to_ascii(result)
            
            test_logger.info(f"CSV save: {'Success' if csv_success else 'Failed'}")
            test_logger.info(f"ASCII save: {'Success' if ascii_success else 'Failed'}")
        else:
            test_logger.error("Parsing failed!")
    
    # Test GRB name generation
    test_logger.info("\nTesting GRB name generation...")

    # Generate a name and test
    test_date = datetime.now()
    name1 = handler._generate_grb_name(test_date)
    test_logger.info(f"Generated name: {name1}")

    # Generate another name for same date - should increment letter
    name2 = handler._generate_grb_name(test_date)
    test_logger.info(f"Generated second name for same date: {name2}")

    # Test different facility types
    test_logger.info("\nTesting different facility types...")

    # Einstein Probe
    ep_name = handler._generate_grb_name(test_date, facility="EinsteinProbe")
    test_logger.info(f"Einstein Probe name: {ep_name}")

    # IceCube
    icecube_name = handler._generate_grb_name(test_date, facility="IceCubeGOLD")
    test_logger.info(f"IceCube name: {icecube_name}")

    # Regular GRB
    grb_name = handler._generate_grb_name(test_date, facility="SwiftXRT")
    test_logger.info(f"Swift GRB name: {grb_name}")

    test_logger.info("\nName generation test complete!")
    
    test_logger.info("\nTest complete! Check the output files for results.")