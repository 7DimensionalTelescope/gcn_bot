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
        - verify_interval (int): How often to verify CSV integrity (in days). (default: 1)

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
                ascii_max_events=10, strict_parsing=False, verify_interval=1):
        """
        Initialize the GCN Notice Handler.
        
        Args:
            output_csv (str): The path to the output CSV file. (default: 'gcn_notices.csv')
            output_ascii (str): The path to the output ASCII file. (default: 'grb_targets.ascii')
            ascii_max_events (int): The maximum number of events to store in the ASCII file. (default: 10)
            strict_parsing (bool): Enable strict parsing mode. (default: False)
            verify_interval (int): How often to verify CSV integrity (in days). (default: 1)
        """
        self.file_lock = Lock()
        self.strict_parsing = strict_parsing
        self.verify_interval = verify_interval
        
        # Unified caching system
        # GRB name sequence tracking (date -> letter)
        self._grb_name_cache = {}  # Format: {'PREFIX_YYMMDD': 'latest_letter'}
        
        # Name consistency cache (facility, trigger_num) -> name
        self._name_cache = {}  # Format: {(facility, trigger_num): name}
        
        # ASCII entry cache to minimize file I/O
        self._ascii_entry_cache = {}  # Format: {(facility, trigger_num): row_data}
        
        # Cache control
        self._cache_loaded = False  # Indicates if the cache has been loaded
        self._last_cache_refresh = datetime.now()  # For controlling refresh frequency
        self._cache_refresh_interval = 300  # 5 minutes
        
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
            'GCN_ID',        # GCN_SwiftXRT_123456
            'Name',          # GRB 250119A
            'RA',            # 250.05 (deg)
            'DEC',           # 25.0   (deg)
            'Error',         # 0.3    (deg)
            'Discovery_UTC', # 2025-01-19 00:00:00 UTC
            'Facility',      # SwiftXRT
            'Trigger_num',   # 123456
            'Notice_date',   # 2025-01-19 00:00:10 UTC
            'Redshift',      # 0.3
            'Host_info'      # "bright galaxy within the localization of GRB"
        ]
        
        # Check if it's time to verify CSV integrity
        self._check_verify_schedule()

    def _check_verify_schedule(self):
        """Check if it's time to verify CSV integrity based on schedule."""
        try:
            # Check if verification is due
            current_time = time.time()
            
            # Initialize verification time if needed
            if self._last_verification is None:
                # Check for existing verification record
                verify_file = f"{self.output_csv}.verify"
                if os.path.exists(verify_file):
                    try:
                        with open(verify_file, 'r') as f:
                            self._last_verification = float(f.read().strip())
                    except (ValueError, IOError) as e:
                        logger.warning(f"Error reading verification record: {e}")
                        self._last_verification = current_time
                else:
                    self._last_verification = current_time
            
            # Check if it's time to verify
            if current_time - self._last_verification > (self.verify_interval * 86400):  # days to seconds
                if os.path.exists(self.output_csv):
                    logger.info("Performing scheduled CSV integrity verification")
                    is_valid, issues, repairs = self._verify_csv_integrity(repair=True)
                    
                    if not is_valid:
                        logger.warning(f"CSV integrity issues found: {', '.join(issues)}")
                        if repairs:
                            logger.info(f"Repairs made: {', '.join(repairs)}")
                    else:
                        logger.info("CSV integrity verification passed")
                
                # Update verification time
                self._last_verification = current_time
                
                # Save verification timestamp
                try:
                    verify_file = f"{self.output_csv}.verify"
                    with open(verify_file, 'w') as f:
                        f.write(str(current_time))
                except IOError as e:
                    logger.error(f"Error saving verification timestamp: {e}")
        
        except Exception as e:
            logger.error(f"Error in verification schedule check: {e}")

    def _normalize_facility_name(self, facility: str) -> str:
        """
        Normalize facility names to group instruments from the same mission.
        
        Args:
            facility (str): Original facility name
            
        Returns:
            str: Normalized facility name (e.g., SwiftBAT, SwiftXRT -> Swift)
        """
        if not facility:
            return ""
            
        if "Swift" in facility:
            return "Swift"
        elif "Fermi" in facility:
            return "Fermi"
        elif "IceCube" in facility or "AMON" in facility:
            return "IceCube"
        elif "Einstein" in facility:
            return "EinsteinProbe"
        else:
            return facility

    def _load_caches(self, force=False):
        """
        Load all caches for names, sequence tracking, and ASCII entries.
        Ensures all facility-trigger_num combinations are properly mapped to existing GRB names.
        
        Args:
            force (bool): If True, forces reload of caches regardless of timing.
        """
        now = datetime.now()
        # Check if cache refresh needed
        if (self._cache_loaded and not force and 
            (now - self._last_cache_refresh).total_seconds() < self._cache_refresh_interval):
            return

        with self.file_lock:
            logger.debug("Loading caches from files")
            
            # Clear existing caches but preserve grb_name_cache sequence tracking
            if force:
                self._name_cache.clear()
                self._ascii_entry_cache.clear()
            
            # Load from CSV - used for both name mapping and sequence tracking
            if os.path.exists(self.output_csv):
                try:
                    # Use safer CSV reading method
                    df = self._safe_csv_read(self.output_csv)
                    
                    # Process for name mapping (facility, trigger_num) -> name
                    if all(col in df.columns for col in ['Name', 'Facility', 'Trigger_num']):
                        for _, row in df.iterrows():
                            if pd.notna(row['Facility']) and pd.notna(row['Trigger_num']) and pd.notna(row['Name']):
                                # Add to name cache - ensure trigger_num is a string
                                key = (row['Facility'], str(row['Trigger_num']))
                                self._name_cache[key] = row['Name']
                                logger.debug(f"Cached name mapping: {key} -> {row['Name']}")
                    
                    # Process for sequence tracking
                    for name in df['Name']:
                        if isinstance(name, str):
                            # Handle different name formats: GRB YYMMDD[A-Z], EP YYMMDD[a-z], IceCube-YYMMDD[A-Z]
                            if name.startswith('GRB '):
                                prefix = 'GRB'
                                parts = name.split()
                                date_part = parts[1][:6]  # 'YYMMDD'
                                letter_part = parts[1][6]  # The sequence letter
                                cache_key = f"{prefix}_{date_part}"
                                alphabet = ascii_uppercase
                            elif name.startswith('EP '):
                                prefix = 'EP'
                                parts = name.split()
                                date_part = parts[1][:6]  # 'YYMMDD'
                                letter_part = parts[1][6]  # The sequence letter
                                cache_key = f"{prefix}_{date_part}"
                                alphabet = ascii_lowercase
                            elif name.startswith('IceCube-'):
                                prefix = 'IceCube'
                                # Remove prefix and dash
                                remaining = name[8:]  # Remove 'IceCube-'
                                date_part = remaining[:6]  # 'YYMMDD'
                                letter_part = remaining[6]  # The sequence letter
                                cache_key = f"{prefix}_{date_part}"
                                alphabet = ascii_uppercase
                            else:
                                continue  # Skip if it doesn't match any expected format
                            
                            # Update cache with latest letter for each prefix_date
                            if cache_key in self._grb_name_cache:
                                current_letter = self._grb_name_cache[cache_key]
                                
                                # Get index of current and new letters
                                try:
                                    letter_idx = alphabet.index(letter_part) if letter_part in alphabet else -1
                                    current_idx = alphabet.index(current_letter) if current_letter in alphabet else -1
                                    
                                    if letter_idx > current_idx:
                                        self._grb_name_cache[cache_key] = letter_part
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"Error comparing letters for {cache_key}: {e}")
                            else:
                                self._grb_name_cache[cache_key] = letter_part
                    
                    logger.debug(f"Loaded {len(self._name_cache)} name mappings into cache from CSV")
                    logger.debug(f"Loaded {len(self._grb_name_cache)} date sequences into GRB name cache")
                except Exception as e:
                    logger.warning(f"Error loading caches from CSV: {e}")
            
            # Load entries from ASCII as well for completeness
            if os.path.exists(self.output_ascii):
                try:
                    df = pd.read_csv(self.output_ascii, sep=r'\s+', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    
                    # Process rows into cache
                    for _, row in df.iterrows():
                        if pd.notna(row['Facility']) and pd.notna(row['Trigger_num']):
                            key = (row['Facility'], str(row['Trigger_num']))
                            # Add to name cache as well as ASCII entry cache
                            self._ascii_entry_cache[key] = row.to_dict()
                            if 'Name' in row and pd.notna(row['Name']):
                                self._name_cache[key] = row['Name']
                    
                    logger.debug(f"Additionally loaded {len(self._ascii_entry_cache)} entries from ASCII entry cache")
                except Exception as e:
                    logger.warning(f"Error loading ASCII entry cache: {e}")
            
            self._cache_loaded = True
            self._last_cache_refresh = now

    def _event_exists(self, facility: str, trigger_num: str) -> Optional[str]:
        """
        Check if an event with the given facility and trigger number already exists.
        Facilities from the same mission (e.g., SwiftBAT, SwiftXRT) are considered the same.
        
        Args:
            facility (str): The facility name
            trigger_num (str): The trigger number
            
        Returns:
            Optional[str]: The existing GRB name if found, None otherwise
        """
        # Always force reload to ensure we have the latest data
        self._load_caches(force=True)
        
        if not facility or not trigger_num:
            return None
        
        # Normalize the facility name for comparison
        normalized_facility = self._normalize_facility_name(facility)
        
        # Check for any matches with normalized facility names
        for (existing_facility, existing_trigger), existing_name in self._name_cache.items():
            # Normalize the existing facility name
            normalized_existing = self._normalize_facility_name(existing_facility)
            
            # If normalized names and trigger numbers match, return the existing name
            if normalized_existing == normalized_facility and str(existing_trigger) == str(trigger_num):
                logger.info(f"Found existing event: {existing_name} for {facility} trigger {trigger_num} (matched with {existing_facility})")
                return existing_name
        
        # If not in name cache, try csv directly (safety check)
        try:
            if os.path.exists(self.output_csv):
                df = self._safe_csv_read(self.output_csv)
                if all(col in df.columns for col in ['Name', 'Facility', 'Trigger_num']):
                    # Get all matching rows with normalized facility names
                    matches = []
                    for idx, row in df.iterrows():
                        normalized_row_facility = self._normalize_facility_name(row['Facility'])
                        if (normalized_row_facility == normalized_facility and 
                            str(row['Trigger_num']) == str(trigger_num)):
                            matches.append(row['Name'])
                    
                    if matches:
                        existing_name = matches[0]  # Take the first match
                        # Update the cache
                        self._name_cache[(facility, str(trigger_num))] = existing_name
                        logger.info(f"Found existing event in CSV (not in cache): {existing_name}")
                        return existing_name
        except Exception as e:
            logger.warning(f"Error checking CSV directly for existing event: {e}")
        
        logger.info(f"No existing event found for {facility} trigger {trigger_num}")
        return None

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

    def _generate_grb_name(self, trigger_date: datetime, facility: Optional[str] = None, trigger_num: Optional[str] = None) -> str:
        """
        Generate a consistent name with different prefixes based on facility.
        - GRB YYMMDD[A-Z] for most events
        - EP YYMMDD[a-z] for Einstein Probe events
        - IceCube-YYMMDD[A-Z] for IceCube events
        """
        # First check if this is an existing event
        if facility and trigger_num:
            existing_name = self._event_exists(facility, str(trigger_num))
            if existing_name:
                logger.info(f"Using existing name {existing_name} for {facility} trigger {trigger_num}")
                return existing_name
        
        # Load caches if needed
        if not self._cache_loaded:
            self._load_caches()
        
        # Determine the prefix based on facility
        is_einstein_probe = facility == "EinsteinProbe"
        is_icecube = any(ice_fac in str(facility) for ice_fac in ["IceCube", "AMON"])
        
        # Set prefix and format based on facility type
        if is_icecube:
            prefix = "IceCube"
            name_format = "dash"  # IceCube-YYMMDD[A-Z]
            alphabet = ascii_uppercase
        elif is_einstein_probe:
            prefix = "EP"
            name_format = "space"  # EP YYMMDD[a-z]
            alphabet = ascii_lowercase
        else:
            prefix = "GRB"
            name_format = "space"  # GRB YYMMDD[A-Z]
            alphabet = ascii_uppercase
        
        logger.info(f"Generating new {prefix} name for date: {trigger_date.strftime('%Y-%m-%d')}")
        
        try:
            # Get date in YYMMDD format
            date_key = trigger_date.strftime('%y%m%d')
            
            # Create a composite key that includes the prefix to separate naming sequences
            sequence_key = f"{prefix}_{date_key}"
            
            # Check if prefix_date exists in cache
            if sequence_key in self._grb_name_cache:
                letter = self._grb_name_cache[sequence_key]
                
                # Get next letter
                next_idx = alphabet.index(letter) + 1
                if next_idx < len(alphabet):
                    next_letter = alphabet[next_idx]
                else:
                    next_letter = alphabet[-1]
                
                # Update cache
                self._grb_name_cache[sequence_key] = next_letter
                
                # Format the name based on facility type
                if name_format == "dash":
                    new_name = f"{prefix}-{date_key}{next_letter}"
                else:
                    new_name = f"{prefix} {date_key}{next_letter}"
                
            else:
                # New date, start with first letter of appropriate alphabet
                first_letter = alphabet[0]
                self._grb_name_cache[sequence_key] = first_letter
                
                # Format the name based on facility type
                if name_format == "dash":
                    new_name = f"{prefix}-{date_key}{first_letter}"
                else:
                    new_name = f"{prefix} {date_key}{first_letter}"
            
            # Update name cache if we have facility and trigger_num
            if facility and trigger_num:
                self._name_cache[(facility, str(trigger_num))] = new_name
                
            return new_name
                
        except Exception as e:
            logger.error(f"Error generating {prefix} name from cache: {e}")
            
            # Fallback to file-based generation if cache fails
            try:
                with self.file_lock:
                    try:
                        with open(self.output_csv, 'r') as f:
                            # Get non-empty lines
                            lines = [line.strip() for line in f.readlines() if line.strip()]
                            if not lines:
                                first_letter = alphabet[0]
                                # Format based on facility
                                if name_format == "dash":
                                    return f"{prefix}-{trigger_date.strftime('%y%m%d')}{first_letter}"
                                else:
                                    return f"{prefix} {trigger_date.strftime('%y%m%d')}{first_letter}"
                            
                            # Start from last non-empty line
                            for line in reversed(lines):
                                try:
                                    grb_name_idx = self.csv_columns.index('Name')
                                    discovery_col_idx = self.csv_columns.index('Discovery_UTC')
                                    
                                    fields = line.split(',')
                                    
                                    # Parse date with multiple format support
                                    date_str = fields[discovery_col_idx].strip('"').strip()
                                    
                                    # Try multiple date formats
                                    date_formats = ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', 
                                                '%Y-%m-%d %H:%M:%S.%f    ', '%Y-%m-%d %H:%M:%S       ']
                                    
                                    line_date = None
                                    for date_format in date_formats:
                                        try:
                                            logger.debug(f"Attempting to parse '{date_str}' with format '{date_format}'")
                                            line_date = datetime.strptime(date_str, date_format)
                                            logger.debug(f"Successfully parsed date: {line_date}")
                                            break
                                        except ValueError as e:
                                            logger.debug(f"Format '{date_format}' failed: {e}")
                                            continue
                                    
                                    if not line_date:
                                        logger.warning(f"Could not parse date: {date_str}")
                                        continue
                                    
                                    # If different date, start with appropriate first letter
                                    if line_date.date() != trigger_date.date():
                                        continue
                                    
                                    grb_name = fields[grb_name_idx].strip('"')
                                    
                                    # Only process lines that match our prefix
                                    if not grb_name.startswith(f"{prefix} "):
                                        continue
                                        
                                    # Extract the sequence letter
                                    sequence_letter = grb_name[-1]
                                    
                                    # Determine the appropriate alphabet for this name
                                    idx_alphabet = ascii_lowercase if prefix == "EP" else ascii_uppercase
                                    
                                    # If not final letter, use next letter
                                    if sequence_letter != idx_alphabet[-1]:
                                        try:
                                            next_sequence = idx_alphabet.index(sequence_letter) + 1
                                            if next_sequence < 26:
                                                new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{idx_alphabet[next_sequence]}"
                                                
                                                # Update name cache if we have facility and trigger_num
                                                if facility and trigger_num:
                                                    self._name_cache[(facility, str(trigger_num))] = new_name
                                                    
                                                return new_name
                                            else:
                                                new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{idx_alphabet[-1]}"
                                                
                                                # Update name cache if we have facility and trigger_num
                                                if facility and trigger_num:
                                                    self._name_cache[(facility, str(trigger_num))] = new_name
                                                    
                                                return new_name
                                        except ValueError:
                                            # If we can't find the letter in the expected alphabet, start fresh
                                            new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{idx_alphabet[0]}"
                                            
                                            # Update name cache if we have facility and trigger_num
                                            if facility and trigger_num:
                                                self._name_cache[(facility, str(trigger_num))] = new_name
                                                
                                            return new_name
                                    
                                    # If final letter, continue to next line
                                    continue
                                    
                                except (IndexError, ValueError) as e:
                                    logger.warning(f"Error processing line: {e}")
                                    continue
                            
                            # If no matching entries are found
                            if name_format == "dash":
                                new_name = f"{prefix}-{trigger_date.strftime('%y%m%d')}{next_letter}"
                            else:
                                new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{next_letter}"
                            
                            # Update name cache if we have facility and trigger_num
                            if facility and trigger_num:
                                self._name_cache[(facility, str(trigger_num))] = new_name
                                
                            return new_name
                            
                    except (FileNotFoundError, pd.errors.EmptyDataError):
                        first_letter = alphabet[0]
                        if name_format == "dash":
                            new_name = f"{prefix}-{trigger_date.strftime('%y%m%d')}{first_letter}"
                        else:
                            new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{first_letter}"
                        
                # Update name cache if we have facility and trigger_num
                if facility and trigger_num:
                    self._name_cache[(facility, str(trigger_num))] = new_name
                    
                return new_name
                    
            except Exception as nested_e:
                logger.error(f"Error in fallback name generation: {nested_e}")
                last_letter = alphabet[-1]
                
                if name_format == "dash":
                    new_name = f"{prefix}-{trigger_date.strftime('%y%m%d')}{last_letter}"
                else:
                    new_name = f"{prefix} {trigger_date.strftime('%y%m%d')}{last_letter}"
                
                # Update name cache if we have facility and trigger_num
                if facility and trigger_num:
                    self._name_cache[(facility, str(trigger_num))] = new_name
                    
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
            
            # Check for event_name_override for CASCADE events
            if 'event_name_override' in kwargs and kwargs['event_name_override']:
                name = kwargs['event_name_override']
            else:
                name = self._generate_grb_name(trigger_date, facility, trigger_num) if trigger_date else ''
            
            notice_data = {
                'GCN_ID': f"GCN_{facility}_{trigger_num}",
                'Name': name,
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

    def _parse_notice(self, text, facility, patterns):
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
            matches = {key: re.search(pattern, text, re.DOTALL) for key, pattern in patterns.items()}
            
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

    def _parse_notice_fermi(self, text, facility):
        """Parse Fermi format notices."""
        try:
            patterns = {
                'notice_date': r"NOTICE_DATE:\s*(\w{3})\s*(\d{2})\s*(\w{3})\s*(\d{2})\s*(\d{2}):(\d{2}):(\d{2})\s*UT", # DD/MM/YY HH:MM:SS UT
                'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
                'date': r"GRB_DATE:.*?(\d{2})/(\d{2})/(\d{2})",  # YY/MM/DD
                'time': r"GRB_TIME:.*?{([\d:\.]+)}\s*UT", # HH:MM:SS
                'ra': r"GRB_RA:.*?(\d+\.\d+)d.*?\(J2000\)",
                'dec': r"GRB_DEC:.*?([-+]?\d+\.\d+)d.*?\(J2000\)",
                'error': r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]"
            }
            logger.debug(f"Starting to parse {facility} notice - Fermi format")
            return self._parse_notice(text, facility, patterns)
        
        except Exception as e:
            logger.error(f"Error parsing {facility} notice: {str(e)} - Fermi format")
            return None

    def _parse_notice_swift(self, text, facility):
        """
        Parse Swift format notices for BAT, XRT, and UVOT.
        """
        try:
            # Define patterns with multiple possible field names
            patterns = {
                'notice_date': r"NOTICE_DATE:\s*(\w{3})\s*(\d{2})\s*(\w{3})\s*(\d{2})\s*(\d{2}):(\d{2}):(\d{2})\s*UT",
                'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
                # Try both GRB_DATE and IMG_START_DATE patterns
                'date': r"(?:GRB_DATE|IMG_START_DATE):.*?(\d{2})/(\d{2})/(\d{2})",  # YY/MM/DD
                # Try both GRB_TIME and IMG_START_TIME patterns
                'time': r"(?:GRB_TIME|IMG_START_TIME):\s*(?:\d+.\d+)\s*(?:SOD)?\s*{([^}]+)}",
                # Handle both decimal degree formats
                'ra': r"GRB_RA:.*?(\d+\.\d+)d?\s*{[^}]+}\s*\(J2000\)",
                'dec': r"GRB_DEC:.*?([-+]?\d+\.\d+)d?\s*{[^}]+}\s*\(J2000\)",
                'error': r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]"
            }
            
            logger.info(f"Starting to parse {facility} notice - Swift format")
            return self._parse_notice(text, facility, patterns)
            
        except Exception as e:
            logger.error(f"Error parsing {facility} notice: {str(e)} - Swift format")
            return None

    def _parse_notice_amon(self, text, facility):
        """
        Parse AMON-style notices (AMON_NU_EM_COINC, ICECUBE_CASCADE, 
        ICECUBE_ASTROTRACK_GOLD/BRONZE, HAWC_BURST_MONITOR).
        """
        try:
            # Common patterns across all AMON notice types
            patterns = {
                'notice_date': r"NOTICE_DATE:\s*(\w{3})\s*(\d{2})\s*(\w{3})\s*(\d{2})\s*(\d{2}):(\d{2}):(\d{2})\s*UT",
                'trigger_num': r"EVENT_NUM:\s*(\d+)",
                'ra': r"SRC_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
                'dec': r"SRC_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
                'error': r"SRC_ERROR:.*?([\d.]+)\s*\[(\w+)"
            }
            
            # Add specific date/time patterns
            if facility in ['AMON', 'IceCubeCASCADE', 'IceCubeBRONZE', 'IceCubeGOLD']:
                patterns.update({
                    'date': r"DISCOVERY_DATE:.*?(\d{2})/(\d{2})/(\d{2})",  # YY/MM/DD
                    'time': r"DISCOVERY_TIME:.*?{([\d:\.]+)}\s*UT" # HH:MM:SS
                })
                
                # Add IceCube-specific fields but don't store in notice_data
                if facility in ['IceCubeBRONZE', 'IceCubeGOLD']:
                    patterns.update({
                        'energy': r"ENERGY:\s*([\d.]+e[+-]?\d+)\s*\[TeV\]",
                        'signalness': r"SIGNALNESS:\s*([\d.]+e[+-]?\d+)\s*\[dn\]",
                        'far': r"FAR:\s*([\d.]+)\s*\[yr\^-1\]"
                    })
                elif facility == 'IceCubeCASCADE':
                    patterns.update({
                        'energy': r"ENERGY:\s*([\d.]+)\s*\[TeV\]",
                        'signalness': r"SIGNALNESS:\s*([\d.]+e[+-]?\d+)\s*\[dn\]",
                        'far': r"FAR:\s*([\d.]+)\s*\[yr\^-1\]",
                        'event_name': r"EVENT_NAME:\s*(IceCubeCascade-\w+)"
                    })
            elif facility == 'AMON':
                patterns.update({
                    'date': r"DISCOVERY_DATE:.*?(\d{2})/(\d{2})/(\d{2})",  # YY/MM/DD
                    'time': r"DISCOVERY_TIME:.*?{([\d:\.]+)}\s*UT", # HH:MM:SS
                    'coinc_pair': r"COINC_PAIR:\s*\d+\s+(\S+)",
                    'delta_t': r"DELTA_T:\s*([\d.]+)"
                })
                
            logger.info(f"Starting to parse {facility} notice - AMON format")
            parsed_data = self._parse_notice(text, facility, patterns)
            
            # Extract IceCube-specific fields for use in memory (but not DB storage)
            icecube_info = {}
            for field in ['energy', 'signalness', 'far', 'coinc_pair', 'delta_t', 'event_name']:
                pattern = patterns.get(field)
                if pattern:
                    match = re.search(pattern, text, re.DOTALL)
                    if match:
                        icecube_info[field] = match.group(1)
                        logger.debug(f"Extracted {field}: {icecube_info[field]}")
            
            # For CASCADE events, use the event_name as Name if provided
            if 'event_name' in icecube_info and parsed_data:
                parsed_data['event_name_override'] = icecube_info['event_name']
                
            # Store IceCube-specific info in parsed_data's 'extra_info' field
            # This won't be saved to the database but can be used for display and ToO
            if parsed_data and icecube_info:
                parsed_data['icecube_info'] = icecube_info
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing {facility} notice: {str(e)} - AMON format")
            return None

    def _parse_notice_calet(self, text, facility):
        """Parse CALET format notices."""
        try:
            patterns = {
                'notice_date': r"NOTICE_DATE:\s*(\w{3})\s*(\d{2})\s*(\w{3})\s*(\d{2})\s*(\d{2}):(\d{2}):(\d{2})\s*UT",
                'trigger_num': r"TRIGGER_NUM:\s*(\d+)",
                'date': r"TRIGGER_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
                'time': r"TRIGGER_TIME:.*?{([\d:\.]+)}\s*UT",
                'ra': r"POINT_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
                'dec': r"POINT_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
            }
            
            logger.info(f"Starting to parse {facility} notice - CALET format")
            parsed_data = self._parse_notice(text, facility, patterns)
            
            # Set error to 0.0 for CALET format since it doesn't have an error
            if parsed_data:
                parsed_data['Error'] = 0.0
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Error parsing {facility} notice: {str(e)} - CALET format")
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

    def _safe_csv_read(self, filepath):
        """Safely read a CSV file, handling various error conditions."""
        try:
            # Try standard pandas read
            return pd.read_csv(filepath)
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV file {filepath} is empty")
            return pd.DataFrame(columns=self.csv_columns)
        except pd.errors.ParserError:
            # If parser error, try a more robust approach
            logger.warning(f"Parser error in CSV file {filepath}, attempting line-by-line read")
            
            valid_rows = []
            expected_fields = len(self.csv_columns)
            
            with open(filepath, 'r') as f:
                try:
                    header = next(f).strip().split(',')
                    for i, line in enumerate(f, 1):
                        try:
                            fields = line.strip().split(',')
                            if len(fields) == expected_fields:
                                valid_rows.append(fields)
                            else:
                                logger.warning(f"Line {i} has {len(fields)} fields, expected {expected_fields}")
                        except Exception as e:
                            logger.warning(f"Error processing line {i}: {e}")
                except StopIteration:
                    # File is empty or only has header
                    return pd.DataFrame(columns=self.csv_columns)
            
            # Create DataFrame from valid rows
            if valid_rows:
                return pd.DataFrame(valid_rows, columns=header)
            else:
                return pd.DataFrame(columns=self.csv_columns)
        except Exception as e:
            logger.error(f"Unexpected error reading CSV file {filepath}: {e}")
            return pd.DataFrame(columns=self.csv_columns)

    def _verify_csv_integrity(self, repair=False):
        """
        Verify the integrity of the CSV file and optionally repair issues.
        
        Args:
            repair (bool): If True, attempt to repair issues found
            
        Returns:
            tuple: (is_valid, issues_found, repairs_made)
        """
        issues_found = []
        repairs_made = []
        
        try:
            if not os.path.exists(self.output_csv):
                issues_found.append("CSV file does not exist")
                
                if repair:
                    # Create new file with header
                    with open(self.output_csv, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                        writer.writeheader()
                    repairs_made.append("Created new CSV file with header")
                    
                return False, issues_found, repairs_made
                
            # Check file is not empty
            if os.path.getsize(self.output_csv) == 0:
                issues_found.append("CSV file is empty")
                if repair:
                    with open(self.output_csv, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                        writer.writeheader()
                    repairs_made.append("Created empty CSV with header")
                return False, issues_found, repairs_made
                
            # Check header row
            with open(self.output_csv, 'r') as f:
                header = f.readline().strip().split(',')
                
            # Verify all expected columns are present
            missing_columns = [col for col in self.csv_columns if col not in header]
            if missing_columns:
                issues_found.append(f"Missing columns: {', '.join(missing_columns)}")
                
                if repair and len(missing_columns) < len(self.csv_columns) / 2:
                    # Only attempt repair if most columns are present
                    temp_file = f"{self.output_csv}.temp"
                    
                    # Read existing file
                    try:
                        df = self._safe_csv_read(self.output_csv)
                        
                        # Add missing columns
                        for col in missing_columns:
                            df[col] = ''
                            
                        # Save with correct columns
                        df.to_csv(temp_file, index=False, columns=self.csv_columns)
                        
                        # Replace original file
                        shutil.move(temp_file, self.output_csv)
                        repairs_made.append(f"Added missing columns: {', '.join(missing_columns)}")
                    except Exception as e:
                        logger.error(f"Failed to repair missing columns: {e}")
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                
            # Check data consistency
            df = self._safe_csv_read(self.output_csv)
            
            # Check for missing values in critical columns
            for col in ['Name', 'RA', 'DEC', 'Discovery_UTC']:
                if col in df.columns and pd.isna(df[col]).any():
                    count = pd.isna(df[col]).sum()
                    issues_found.append(f"Missing values in {col} column: {count} rows")
                    
            # Check for duplicate GRB names
            if 'Name' in df.columns:
                duplicates = df['Name'].duplicated()
                duplicate_count = duplicates.sum()
                if duplicate_count > 0:
                    duplicate_names = df.loc[duplicates, 'Name'].tolist()
                    issues_found.append(f"Found {duplicate_count} duplicate GRB names: {', '.join(duplicate_names[:5])}")
                    
                    if repair:
                        # Remove duplicates by keeping first occurrence
                        df = df.drop_duplicates(subset=['Name'], keep='first')
                        df.to_csv(self.output_csv, index=False)
                        repairs_made.append(f"Removed {duplicate_count} duplicate GRB name entries")
                    
            # Check date format consistency
            if 'Discovery_UTC' in df.columns:
                date_format_issues = 0
                for i, date_str in enumerate(df['Discovery_UTC']):
                    if isinstance(date_str, str):
                        try:
                            # Try to parse with various formats
                            formats = ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f    ', '%Y-%m-%d %H:%M:%S       ']
                            for fmt in formats:
                                try:
                                    datetime.strptime(date_str.strip(), fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                date_format_issues += 1
                        except (ValueError, AttributeError):
                            date_format_issues += 1
                            
                if date_format_issues > 0:
                    issues_found.append(f"Found {date_format_issues} date format issues in Discovery_UTC")
                    
                    if repair:
                        # Try to fix date formats
                        for i, date_str in enumerate(df['Discovery_UTC']):
                            if isinstance(date_str, str):
                                try:
                                    # Try different formats
                                    for fmt in formats:
                                        try:
                                            dt = datetime.strptime(date_str.strip(), fmt)
                                            df.at[i, 'Discovery_UTC'] = dt.strftime('%Y-%m-%d %H:%M:%S')
                                            break
                                        except ValueError:
                                            continue
                                except Exception:
                                    pass
                                    
                        df.to_csv(self.output_csv, index=False)
                        repairs_made.append(f"Attempted to fix {date_format_issues} date format issues")
                
            return len(issues_found) == 0, issues_found, repairs_made
            
        except Exception as e:
            logger.error(f"Error verifying CSV integrity: {e}")
            issues_found.append(f"Verification error: {str(e)}")
            return False, issues_found, repairs_made

#---------------------------------------Main Function----------------------------------------
    def parse_notice(self, formatted_text: Union[str, bytes], topic: str) -> Optional[Dict[str, Any]]:
        """
        Parse notice and extract relevant information.
        
        Args:
            formatted_text (str): Formatted text of the notice.
            topic (str): Topic of the notice.
        
        Returns:
            notice_data (dict): Parsed notice data.
        """
        facility = self._get_facility(topic)
        if not facility:
            logger.warning(f"Facility not found in topic: {topic}")
            return None

        try:
            if isinstance(formatted_text, bytes):
                formatted_text = formatted_text.decode('utf-8')
            else:
                formatted_text = formatted_text

            # Route to appropriate parser based on facility
            if 'Swift' in facility:
                return self._parse_notice_swift(formatted_text, facility)
            elif 'Fermi' in facility:
                return self._parse_notice_fermi(formatted_text, facility)
            elif any(fac in facility for fac in ['AMON', 'IceCubeCASCADE', 'HAWC', 'IceCubeBRONZE', 'IceCubeGOLD', 'IceCube']):
                return self._parse_notice_amon(formatted_text, facility)
            elif 'CALET' in facility:
                return self._parse_notice_calet(formatted_text, facility)
            elif 'EinsteinProbe' in facility: # JSON
                return self._parse_notice_einstein_probe(formatted_text, facility)
            else:
                logger.warning(f"No parser available for facility: {facility}")
                return None

        except Exception as e:
            logger.error(f"Error parsing notice: {str(e)}")
            return None

    def save_to_csv(self, notice_data: Dict[str, Any]) -> bool:
        """
        Save notice data to CSV file.
        Always appends as a new row, but name consistency is maintained
        through the _generate_grb_name function.
        
        Args:
            notice_data (dict): Notice data to be saved.
        
        Returns:
            Bool: True if notice data is saved successfully, False otherwise.
        """
        try:
            with self.file_lock:
                # Format numeric values again just to be sure
                formatted_data = notice_data.copy()
                
                # Format numeric fields to 2 decimal places
                for field in ['RA', 'DEC', 'Error']:
                    if field in formatted_data and formatted_data[field] not in ('', None):
                        formatted_data[field] = round(float(formatted_data[field]), 2)
                
                # Format date fields to remove microseconds
                for field in ['Discovery_UTC', 'Notice_date']:
                    if field in formatted_data and formatted_data[field] not in ('', None):
                        if isinstance(formatted_data[field], datetime):
                            formatted_data[field] = formatted_data[field].replace(microsecond=0)
                
                # Create a row with None for missing columns
                row_data = {col: formatted_data.get(col, None) for col in self.csv_columns}
                
                # Check if file exists
                file_exists = os.path.exists(self.output_csv)
                
                # Create new file with header if it doesn't exist
                if not file_exists:
                    with open(self.output_csv, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                        writer.writeheader()
                        writer.writerow(row_data)
                        logger.info(f"Created new CSV file with header: {self.output_csv}")
                    return True
                
                # Check first line for header (up to 1024 characters)
                with open(self.output_csv, 'r') as f:
                    first_line = f.read(1024).strip().split("\n")[0]
                
                # Check if header is present
                has_header = all(col in first_line for col in self.csv_columns)
                
                if not has_header:
                    logger.warning(f"CSV file missing header at top line: {self.output_csv}")
                
                # Check if last character is a newline
                with open(self.output_csv, 'rb+') as f:
                    f.seek(-1, os.SEEK_END)  # Move to the last character
                    last_char = f.read(1).decode('utf-8', errors='ignore')

                    if last_char not in ('\n', '\r'):
                        f.write(b'\n')  # Add a newline if missing
                
                # Append new data
                with open(self.output_csv, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.csv_columns)
                    writer.writerow(row_data)
                    logger.info(f"Added new row to CSV for {formatted_data.get('Name', 'Unknown')}")
                
                return True

        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False

    def save_to_ascii(self, notice_data: Dict[str, Any]) -> bool:
        """
        Save/update latest events to ASCII file.
        If an entry with the same Facility and Trigger_num already exists,
        it will be updated with the new data instead of adding a new row.
        
        Args:
            notice_data (dict): Notice data to be saved.
        
        Returns:
            bool: True if notice data is saved successfully, False otherwise.
        """
        try:
            with self.file_lock:
                # Format numeric values again just to be sure
                formatted_data = notice_data.copy()
                
                # Format numeric fields to 2 decimal places
                for field in ['RA', 'DEC', 'Error']:
                    if field in formatted_data and formatted_data[field] not in ('', None):
                        formatted_data[field] = round(float(formatted_data[field]), 2)
                
                # Format date fields to remove microseconds
                for field in ['Discovery_UTC', 'Notice_date']:
                    if field in formatted_data and formatted_data[field] not in ('', None):
                        if isinstance(formatted_data[field], datetime):
                            formatted_data[field] = formatted_data[field].replace(microsecond=0)
                
                # Get facility and trigger_num for matching
                facility = str(formatted_data.get('Facility'))
                trigger_num = str(formatted_data.get('Trigger_num', ''))  # Ensure trigger_num is a string
                
                # Normalize facility name for matching
                normalized_facility = self._normalize_facility_name(facility)
                logger.info(f"Saving/updating ASCII entry for facility={facility} (normalized={normalized_facility}), trigger_num={trigger_num}")
                
                try:
                    # Try to load existing ASCII file
                    df = pd.read_csv(self.output_ascii, sep=r'\s+', 
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    logger.info(f"Loaded ASCII file with {len(df)} entries")
                except (pd.errors.EmptyDataError, FileNotFoundError):
                    # Create new DataFrame if file doesn't exist or is empty
                    df = pd.DataFrame(columns=self.ascii_columns)
                    logger.info(f"Created new ASCII file: {self.output_ascii}")

                # Check if entry with same normalized Facility and Trigger_num already exists
                existing_idx: Optional[int] = None
                if 'Facility' in df.columns and 'Trigger_num' in df.columns:
                    # Find matching entry using normalized facility name
                    for i, (idx, row) in enumerate(df.iterrows()):
                        row_facility = row.get('Facility', '')
                        row_trigger = str(row.get('Trigger_num', ''))
                        
                        # Normalize the row's facility
                        normalized_row_facility = self._normalize_facility_name(row_facility)
                        
                        # Check if normalized facilities and trigger numbers match
                        if normalized_row_facility == normalized_facility and row_trigger == trigger_num:
                            existing_idx = i  # Use the enumerate index i instead of idx
                            break
                    
                    if existing_idx is not None:
                        # Entry exists, update with new data
                        logger.info(f"Found existing entry for {facility} trigger {trigger_num} at index {existing_idx}. Updating with new data.")
                        
                        # Get the actual index from the DataFrame
                        actual_idx = df.index[existing_idx]
                        
                        # Update existing row with new data - preserve certain fields
                        for col in self.ascii_columns:
                            if col in formatted_data and formatted_data[col] not in ('', None):
                                # Don't overwrite Redshift and Host_info with empty values
                                if col in ['Redshift', 'Host_info'] and (formatted_data[col] == '' or formatted_data[col] is None):
                                    logger.debug(f"Preserving existing {col} value")
                                    continue
                                df.at[actual_idx, col] = formatted_data[col]
                    else:
                        # No existing entry, create a new row
                        logger.info(f"No existing entry found. Creating new row for {facility} trigger {trigger_num}")
                        new_row = pd.DataFrame([{
                            'GCN_ID': f"GCN_{facility}_{trigger_num}",
                            'Name': formatted_data.get('Name', ''),
                            'RA': formatted_data.get('RA', ''),
                            'DEC': formatted_data.get('DEC', ''),
                            'Error': formatted_data.get('Error', ''),
                            'Discovery_UTC': formatted_data.get('Discovery_UTC', ''),
                            'Facility': formatted_data.get('Facility', ''),
                            'Trigger_num': trigger_num,
                            'Notice_date': formatted_data.get('Notice_date', ''),
                            'Redshift': '',  # Empty value for Redshift
                            'Host_info': ''  # Empty value for Host_info
                        }])
                        # Add to beginning of DataFrame
                        df = pd.concat([new_row, df], ignore_index=True)
                        logger.info(f"Added new entry for {facility} trigger {trigger_num} at the top.")
                else:
                    # First row case - add header and first row
                    logger.info(f"No existing entries found. Creating first entry for {facility} trigger {trigger_num}")
                    new_row = pd.DataFrame([{
                        'GCN_ID': f"GCN_{facility}_{trigger_num}",
                        'Name': formatted_data.get('Name', ''),
                        'RA': formatted_data.get('RA', ''),
                        'DEC': formatted_data.get('DEC', ''),
                        'Error': formatted_data.get('Error', ''),
                        'Discovery_UTC': formatted_data.get('Discovery_UTC', ''),
                        'Facility': formatted_data.get('Facility', ''),
                        'Trigger_num': trigger_num,
                        'Notice_date': formatted_data.get('Notice_date', ''),
                        'Redshift': '',  # Empty value for Redshift
                        'Host_info': ''  # Empty value for Host_info
                    }])
                    df = pd.concat([new_row, df], ignore_index=True)
                    logger.info(f"Added first entry for {facility} trigger {trigger_num}.")
                    
                # Limit to max events    
                df = df.head(self.ascii_max_events)
                
                # Format dataframe columns before writing
                for col in ['RA', 'DEC', 'Error']:
                    if col in df.columns:
                        df[col] = df[col].apply(lambda x: round(float(x), 2) if pd.notnull(x) and x != '' else x)

                # Write the updated ASCII file
                with open(self.output_ascii, 'w') as f:
                    # Write header
                    header = ' '.join(self.ascii_columns)
                    f.write(f"{header}\n")
                    
                    # Write each row with proper formatting
                    for _, row in df.iterrows():
                        formatted_values = []
                        for col in self.ascii_columns:
                            if col not in row:
                                formatted_values.append('""')
                                continue
                                
                            value = str(row[col]) if pd.notnull(row[col]) else ''
                            # Wrap in quotes if the data has space
                            if col in ['Name', 'Discovery_UTC', 'Notice_date'] or ' ' in value:
                                formatted_values.append(f'"{value}"')
                            else:
                                formatted_values.append(value)
                        
                        line = ' '.join(formatted_values)
                        f.write(f"{line}\n")

                if existing_idx is not None:
                    logger.info(f"Successfully updated existing entry in ASCII file for {facility} trigger {trigger_num}")
                else:
                    logger.info(f"Successfully added new entry to ASCII file for {facility} trigger {trigger_num}")
                
                return True

        except Exception as e:
            logger.error(f"Error saving to ASCII: {e}")
            return False

#---------------------------------------Test Code----------------------------------------
if __name__ == "__main__":
    import shutil
    
    ######################## Setup for test ########################
    
    # Set output CSV file and time window
    csv_test_file = './test/gcn_notices_test.csv'
    ascii_test_file = './test/gcn_notices_test_ascii.ascii'
    
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
            logging.FileHandler('./test/visibility_test.log'),
            logging.StreamHandler()
        ]
    )
    test_logger = logging.getLogger(__name__)

    # Verify CSV integrity
    test_logger.info("\nVerifying CSV integrity...")
    is_valid, issues, repairs = handler._verify_csv_integrity(repair=True)
    if not is_valid:
        test_logger.error(f"Issues found: {', '.join(issues)}")
        if repairs:
            test_logger.warning(f"Repairs made: {', '.join(repairs)}")
    else:
        test_logger.info("CSV integrity verified, no issues found.")
    
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
    
    # Test caching mechanism
    test_logger.info("\nTesting GRB name cache...")
    # Reset cache to force initialization
    handler._cache_loaded = False
    handler._grb_name_cache = {}
    
    # Generate a name and check cache
    test_date = datetime.now()
    name1 = handler._generate_grb_name(test_date)
    test_logger.info(f"Generated name from empty cache: {name1}")
    test_logger.info(f"Cache contents: {handler._grb_name_cache}")
    
    # Generate another name for same date
    name2 = handler._generate_grb_name(test_date)
    test_logger.info(f"Generated second name for same date: {name2}")
    test_logger.info(f"Cache contents: {handler._grb_name_cache}")
    
    test_logger.info("\nTest complete! Check the output files for results.")