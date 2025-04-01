#!/usr/bin/env python3
"""
GCN Circular Handler
==================
A module for processing GCN (Gamma-ray Coordinates Network) circulars and updating relevant databases.

This module processes GCN circulars to extract key information about GRB 
(Gamma-Ray Burst) events, and maintains both CSV and ASCII databases of event information.

Author:         YoungPyo Hong
Created:        2025-03-27
Version:        1.1.0
License:        MIT

Usage Examples:
    # Process a single circular from a file
    handler = GCNCircularHandler()
    handler.process_circular_from_file("path/to/circular.json")
    
    # Process a batch of circulars
    handler.process_batch_from_directory("path/to/circulars/")
    
    # Monitor for new circulars
    handler.monitor_circulars()  # Requires Kafka consumer credentials
"""

import pandas as pd
import re
from datetime import datetime
import json
import os
import logging
import threading
from typing import Dict, Optional, Tuple, List, Any, Union, Pattern
import csv
import numpy as np
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gcn_circular_handler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GCNCircularHandler:
    def __init__(self, output_csv: str = 'gcn_circulars.csv', 
                 output_ascii: str = 'grb_targets.ascii',
                 client_id: str = '',
                 client_secret: str = '') -> None:
        """
        Initialize the GCN Circular Handler.
        
        Args:
            output_csv (str): Path to the CSV database file
            output_ascii (str): Path to the ASCII database file
            client_id (str, optional): Kafka client ID for monitoring circulars
            client_secret (str, optional): Kafka client secret for monitoring circulars
        """
        self.output_csv = output_csv
        self.output_ascii = output_ascii
        self.file_lock = threading.Lock()
        self.consumer = None  # Initialize consumer as None
        
        # Initialize Kafka consumer if credentials are provided
        if client_id and client_secret:
            try:
                from gcn_kafka import Consumer
                self.consumer = Consumer(
                    client_id=client_id,
                    client_secret=client_secret
                )
                self.consumer.subscribe(['gcn.circulars'])
                logger.info("Kafka consumer initialized")
            except ImportError:
                logger.error("Failed to import gcn_kafka. Make sure the package is installed.")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka consumer: {e}")
        
        # Precompile regex patterns for performance
        self._compile_regex_patterns()
        
        # Define facility mappings
        self.facility_mappings = {
            'Swift-BAT': 'SwiftBAT',
            'Swift-XRT': 'SwiftXRT',
            'Swift-UVOT': 'SwiftUVOT',
            'Swift/BAT': 'SwiftBAT',
            'Swift/XRT': 'SwiftXRT',
            'Swift/UVOT': 'SwiftUVOT',
            'Swift BAT': 'SwiftBAT',
            'Swift XRT': 'SwiftXRT',
            'Swift UVOT': 'SwiftUVOT',
            'Fermi GBM': 'FermiGBM',
            'Fermi-GBM': 'FermiGBM',
            'Fermi LAT': 'FermiLAT',
            'Fermi-LAT': 'FermiLAT',
            'Einstein Probe': 'EinsteinProbe',
            'EP-WXT': 'EinsteinProbe',
            'EP-FXT': 'EinsteinProbe',
        }
        
        # CSV column definitions
        self.csv_columns = [
            'circular_id',
            'event_name',
            'subject',
            'facility',
            'trigger_num',
            'ra',
            'dec',
            'error',
            'error_unit',
            'redshift',
            'redshift_error',
            'host_info',
            'false_trigger',
            'created_on',
            'processed_on'
        ]
        
        # ASCII file columns
        self.ascii_columns = [
            'GCN_ID',
            'Name',
            'RA',
            'DEC',
            'Error',
            'Discovery_UTC',
            'Facility',
            'Trigger_num',
            'Notice_date',
            'Redshift',
            'Host_info'
        ]
        
    def _compile_regex_patterns(self) -> None:
        """
        Define and precompile regex patterns for extracting information from circulars.
        This improves performance for repeated pattern matching.
        """
        # Raw pattern definitions (not compiled)
        self.patterns = {
            # Position patterns for different facilities
            'position': {
                'Swift-XRT': [
                    # Enhanced Swift-XRT position with decimal degrees
                    r"Enhanced Swift-XRT position.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    # Enhanced Swift-XRT position with sexagesimal format
                    r"RA\s*\(J2000\):\s*(\d{2})h\s*(\d{2})m\s*([\d.]+)s.*?Dec\s*\(J2000\):\s*([-+]?\d{2})d\s*(\d{2})\'\s*([\d.]+)\".*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    # General XRT position format
                    r"XRT.*?RA,Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ],
                'Swift-BAT': [
                    # BAT position format
                    r"Swift.*?BAT.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?error\s+radius\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ],
                'Fermi-GBM': [
                    # Fermi GBM position
                    r"best\s+(?:LAT\s+)?(?:on-ground\s+)?location.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?\(J2000\).*?error\s+radius\s+of\s+([\d.]+)\s*deg",
                ],
                'Fermi-LAT': [
                    # Fermi LAT position
                    r"LAT.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?\(J2000\).*?error\s+radius\s+of\s+([\d.]+)\s*deg",
                    # Added specific format seen in sample5
                    r"best LAT on-ground location.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?\(J2000\).*?error\s+radius\s+of\s+([\d.]+)\s*deg",
                ],
                'Einstein-Probe': [
                    # EP position formats
                    r"EP.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"EP.*?RA\s*=\s*([\d.]+),\s*Dec\s*=\s*([-+]?[\d.]+).*?error\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ],
            },
            # False trigger patterns
            'false_trigger': {
                'general': [
                    r"(?:is\s+not\s+a\s+GRB|is\s+not\s+due\s+to\s+a\s+GRB|not\s+a\s+real\s+source|false\s+trigger|retraction)",
                    # Add this pattern to catch "in fact not due to a GRB"
                    r"in\s+fact\s+not\s+due\s+to\s+a\s+GRB",
                    # Add pattern to detect from the subject line
                    r"is\s+not\s+a\s+GRB"
                ],
                'Fermi': [
                    r"Fermi.*?trigger\s+([\d/]+).*?is\s+not\s+a\s+GRB",
                    r"Fermi.*?trigger\s+([\d/]+).*?not\s+due\s+to\s+a\s+GRB",
                    # Add more specific pattern for Fermi false triggers
                    r"Fermi.*?trigger.*?tentatively\s+classified\s+as\s+a\s+GRB.*?not\s+due\s+to"
                ],
                'Swift': [
                    r"Swift\s+Trigger\s+(\d+)\s+is\s+not\s+a\s+GRB",
                ]
            },
            # Enhanced redshift patterns
            'redshift': {
                'general': [
                    # Common redshift notations
                    r"redshift\s*(?:=|of)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"common\s+redshift\s+(?:of\s+)?z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"redshift\s+at\s+z\s*(?:=|~)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    # Added formats - separated from general pattern for clarity
                    r"spectroscopic\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"measured\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"confirmed\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    # Pattern to catch format in sample3 (very specific)
                    r"common\s+redshift\s+of\s+([\d.]+)±([\d.]+)",
                ]
            },
            # Host galaxy patterns
            'host_galaxy': {
                'general': [
                    r"host\s+galaxy.*?([^\.;]+(?:galaxy|mag|magnitude)[^\.;]+)",
                    r"([^\.;]+host\s+galaxy[^\.;]+)",
                    r"putative\s+host.*?([^\.;]+)",
                    # Add more specific pattern to match sample3
                    r"bright\s+galaxy\s+within\s+the\s+localization.*?([^\.;]+)",
                    # Pattern to catch the last sentence in sample3
                    r"supporting\s+it\s+as\s+a\s+likely\s+host\s+galaxy\s+of\s+the\s+GRB",
                    # Additional general patterns
                    r"coincidence\s+between\s+the\s+(?:bright\s+)?galaxy\s+and\s+the\s+XRT\s+localization.*?([^\.;]+)",
                    r"([^\.;]+likely\s+host\s+galaxy[^\.;]+)",
                ]
            },
            # Trigger number patterns - IMPROVED
            'trigger_num': {
                'Swift': [
                    r"(?:Swift|BAT).*?trigger(?:[=:])?\s*(\d+)",
                    r"Swift\s+Trigger\s+(\d+)",
                    r"BAT\s+trigger\s+#?(\d+)",
                    r"(?:after|of)\s+the\s+BAT\s+trigger\s+\(.*?GCN\s+Circ\.\s+(\d+)\)",
                    # Added patterns
                    r"Trigger\s+Number\s*:\s*(\d+)",
                    r"\(Gupta\s+et\s+al\.,\s+GCN\s+Circ\.\s+(\d+)\)",
                ],
                'Einstein-Probe': [
                    r"EP-WXT\s+trigger\s+([\w\d]+)",
                    r"EP.*?ID\s*(?::|=)\s*(?:\[)?[\'\"]*(\d+)[\'\"]*(?:\])?",
                    r"EP.*?trigger\s+#?(\d+)",
                ],
                'Fermi': [
                    r"(?:Fermi-GBM|GBM)\s+(?:\()?trigger\s+([\d/]+)",
                    r"trigger(?:[=:])?\s*(\d+)/(\d+)",
                    r"Fermi.*?trigger\s+(\d+)",
                    r"GBM.*?trigger\s+#?(\d+)",
                    # Add pattern for specific format "(trigger 764205327 / 250320969, GCN 39792)"
                    r"Fermi-GBM\s+\(trigger\s+(\d+)\s*/\s*\d+",
                    r"\(trigger\s+(\d+)\s*/\s*\d+",
                ]
            },
            # Facility patterns
            'facility': [
                (r'Swift[\s/\-]?(?:BAT|XRT|UVOT)', 'Swift'),
                (r'Fermi[\s/\-]?(?:GBM|LAT)', 'Fermi'),
                (r'Einstein\s+Probe|EP[\s/\-]?(?:WXT|FXT)', 'EinsteinProbe')
            ],
            # GRB event name patterns
            'event_name': [
                r'(?:GRB|grb)\s+(\d{6}[A-Za-z])',   # e.g., GRB 250322A
                r'(?:EP)(\d{6}[a-z])',              # e.g., EP250321a
                r'(AT20\d{2}[a-z]{3})'              # e.g., AT2025dws
            ]
        }
        
        # Compiled pattern storage
        self.compiled_patterns = {
            'position': {},
            'false_trigger': {},
            'redshift': {},
            'host_galaxy': {},
            'trigger_num': {},
            'facility': [],
            'event_name': []
        }
        
        # Compile position patterns
        for facility, patterns in self.patterns['position'].items():
            self.compiled_patterns['position'][facility] = [
                re.compile(pattern, re.IGNORECASE | re.DOTALL) for pattern in patterns
            ]
        
        # Compile false trigger patterns
        for category, patterns in self.patterns['false_trigger'].items():
            self.compiled_patterns['false_trigger'][category] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile redshift patterns
        for category, patterns in self.patterns['redshift'].items():
            self.compiled_patterns['redshift'][category] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile host galaxy patterns
        for category, patterns in self.patterns['host_galaxy'].items():
            self.compiled_patterns['host_galaxy'][category] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile trigger number patterns
        for facility, patterns in self.patterns['trigger_num'].items():
            self.compiled_patterns['trigger_num'][facility] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile facility patterns
        self.compiled_patterns['facility'] = [
            (re.compile(pattern, re.IGNORECASE), facility) for pattern, facility in self.patterns['facility']
        ]
        
        # Compile event name patterns
        self.compiled_patterns['event_name'] = [
            re.compile(pattern) for pattern in self.patterns['event_name']
        ]
        
    def _extract_redshift(self, body: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract redshift and error from circular body.
        
        Args:
            body (str): The body of the circular
            
        Returns:
            Tuple[Optional[float], Optional[float]]: (Redshift, Redshift Error) or (None, None) if not found
            
        Examples:
            >>> handler._extract_redshift("We measure a redshift of 0.4215±0.0005")
            (0.4215, 0.0005)
            >>> handler._extract_redshift("The host galaxy has z = 1.23")
            (1.23, None)
        """
        try:
            # Try each redshift pattern
            for pattern in self.compiled_patterns['redshift']['general']:
                match = pattern.search(body)
                if match:
                    redshift = float(match.group(1))
                    redshift_error = float(match.group(2)) if len(match.groups()) > 1 and match.group(2) else None
                    logger.info(f"Extracted redshift: z={redshift}{f' ± {redshift_error}' if redshift_error else ''}")
                    return redshift, redshift_error
            
            # Special case: search for decimal numbers following "redshift" within a reasonable distance
            redshift_mentions = re.finditer(r"redshift", body, re.IGNORECASE)
            for mention in redshift_mentions:
                # Look for numbers within 50 characters after "redshift"
                pos = mention.end()
                snippet = body[pos:pos+50]
                num_match = re.search(r"([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?", snippet)
                if num_match:
                    redshift = float(num_match.group(1))
                    redshift_error = float(num_match.group(2)) if num_match.group(2) else None
                    logger.info(f"Extracted redshift using proximity search: z={redshift}{f' ± {redshift_error}' if redshift_error else ''}")
                    return redshift, redshift_error
                    
            logger.debug("No redshift found in circular")
            return None, None
        except Exception as e:
            logger.error(f"Error extracting redshift: {e}")
            return None, None
        
    def _extract_host_info(self, body: str) -> Optional[str]:
        """
        Extract host galaxy information from circular body.
        
        Args:
            body (str): The body of the circular
            
        Returns:
            Optional[str]: Host galaxy information or None if not found
            
        Examples:
            >>> handler._extract_host_info("We observed the bright galaxy within the localization of GRB...")
            "bright galaxy within the localization of GRB"
        """
        try:
            for pattern in self.compiled_patterns['host_galaxy']['general']:
                match = pattern.search(body)
                if match:
                    host_info = match.group(1).strip()
                    logger.info(f"Extracted host galaxy info: {host_info[:50]}...")
                    return host_info
            
            # Try additional approaches: look for context containing "host" and "galaxy"
            host_mentions = re.finditer(r"host|galaxy", body, re.IGNORECASE)
            for mention in host_mentions:
                # Get surrounding context (100 chars before and after)
                start = max(0, mention.start() - 100)
                end = min(len(body), mention.end() + 100)
                context = body[start:end]
                
                # Extract a sentence containing the mention
                sentence_match = re.search(r"([^\.;]+host[^\.;]+galaxy[^\.;]+)", context, re.IGNORECASE)
                if sentence_match:
                    host_info = sentence_match.group(1).strip()
                    logger.info(f"Extracted host galaxy info using context: {host_info[:50]}...")
                    return host_info
                    
            logger.debug("No host galaxy information found in circular")
            return None
        except Exception as e:
            logger.error(f"Error extracting host info: {e}")
            return None
        
    def _check_false_trigger(self, body: str, facility: Optional[str]) -> bool:
        """
        Check if the circular is reporting a false trigger.
        
        Args:
            body (str): The body of the circular
            facility (Optional[str]): The detected facility name, if available
            
        Returns:
            bool: True if it's a false trigger, False otherwise
        """
        try:
            # First check general false trigger patterns
            for pattern in self.compiled_patterns['false_trigger']['general']:
                if pattern.search(body):
                    logger.info("Detected false trigger based on general pattern")
                    return True
            
            # Also check if "not a GRB" appears anywhere in the body
            if re.search(r"not\s+(?:a|due\s+to\s+a)\s+GRB", body, re.IGNORECASE):
                logger.info("Detected false trigger based on 'not a GRB' pattern")
                return True
                    
            # Then check facility-specific patterns if facility is known
            if facility:
                facility_key = None
                if 'Swift' in facility:
                    facility_key = 'Swift'
                elif 'Fermi' in facility:
                    facility_key = 'Fermi'
                elif 'Einstein' in facility:
                    facility_key = 'Einstein-Probe'
                    
                if facility_key and facility_key in self.compiled_patterns['false_trigger']:
                    for pattern in self.compiled_patterns['false_trigger'][facility_key]:
                        if pattern.search(body):
                            logger.info(f"Detected false trigger based on {facility_key} pattern")
                            return True
            
            # Special handling for common phrases that indicate false triggers
            for phrase in ["not due to a GRB", "not a GRB", "false trigger", "not a real"]:
                if phrase.lower() in body.lower():
                    logger.info(f"Detected false trigger based on phrase: '{phrase}'")
                    return True
                    
            return False
        except Exception as e:
            logger.error(f"Error checking for false trigger: {e}")
            return False
    
    def _extract_coordinates(self, body: str, facility: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
        """
        Extract RA, Dec and position error from circular body.
        
        Args:
            body (str): The body of the circular
            facility (Optional[str]): The detected facility name, if available
            
        Returns:
            Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]: 
                (RA, Dec, Error, Error Unit) or (None, None, None, None) if not found
                
        Examples:
            >>> handler._extract_coordinates("RA, Dec = 106.76048, +7.19313 with uncertainty of 2.6 arcsec", "SwiftXRT")
            (106.76048, 7.19313, 2.6, 'arcsec')
        """
        try:
            if not facility:
                # Try all position patterns if facility is unknown
                for facility_key, patterns in self.compiled_patterns['position'].items():
                    for pattern in patterns:
                        match = pattern.search(body)
                        if match:
                            if facility_key in ['Swift-XRT', 'Swift-BAT', 'Einstein-Probe']:
                                # These patterns extract decimal degrees directly
                                if len(match.groups()) >= 3:
                                    ra = float(match.group(1))
                                    dec = float(match.group(2))
                                    error = float(match.group(3))
                                    error_unit = match.group(4) if len(match.groups()) >= 4 else 'arcsec'
                                    return ra, dec, error, error_unit
                            elif facility_key in ['Fermi-GBM', 'Fermi-LAT']:
                                # These also extract decimal degrees but error is in degrees
                                if len(match.groups()) >= 3:
                                    ra = float(match.group(1))
                                    dec = float(match.group(2))
                                    error = float(match.group(3))
                                    error_unit = 'deg'
                                    return ra, dec, error, error_unit
                
                # Try to match sexagesimal format (RA: HH MM SS.SS, Dec: DD MM SS.SS)
                sex_pattern = re.compile(
                    r"RA\s*\(J2000\):\s*(\d{2})h\s*(\d{2})m\s*([\d.]+)s.*?" +
                    r"Dec\s*\(J2000\):\s*([-+]?\d{2})d\s*(\d{2})\'\s*([\d.]+)\".*?" +
                    r"uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    re.IGNORECASE | re.DOTALL
                )
                
                sex_match = sex_pattern.search(body)
                if sex_match:
                    # Convert sexagesimal to decimal degrees
                    ra_h = float(sex_match.group(1))
                    ra_m = float(sex_match.group(2))
                    ra_s = float(sex_match.group(3))
                    ra = (ra_h + ra_m / 60 + ra_s / 3600) * 15  # Convert hours to degrees
                    
                    dec_d = float(sex_match.group(4))
                    dec_m = float(sex_match.group(5))
                    dec_s = float(sex_match.group(6))
                    dec_sign = -1 if dec_d < 0 else 1
                    dec = dec_sign * (abs(dec_d) + dec_m / 60 + dec_s / 3600)
                    
                    error = float(sex_match.group(7))
                    error_unit = sex_match.group(8)
                    
                    return ra, dec, error, error_unit
                    
                return None, None, None, None
                
            # Facility-specific coordinate extraction
            if 'Swift' in facility:
                relevant_patterns = self.compiled_patterns['position']['Swift-XRT'] if 'XRT' in facility else self.compiled_patterns['position']['Swift-BAT']
            elif 'Fermi' in facility:
                relevant_patterns = self.compiled_patterns['position']['Fermi-GBM'] if 'GBM' in facility else self.compiled_patterns['position']['Fermi-LAT']
            elif 'Einstein' in facility:
                relevant_patterns = self.compiled_patterns['position']['Einstein-Probe']
            else:
                # Try all patterns if specific instrument not identified
                return self._extract_coordinates(body, None)
                
            # Try to match the relevant patterns
            for pattern in relevant_patterns:
                match = pattern.search(body)
                if match:
                    # Check if it's a sexagesimal format
                    if len(match.groups()) >= 6 and 'XRT' in facility:
                        # Convert sexagesimal to decimal degrees
                        ra_h = float(match.group(1))
                        ra_m = float(match.group(2))
                        ra_s = float(match.group(3))
                        ra = (ra_h + ra_m / 60 + ra_s / 3600) * 15  # Convert hours to degrees
                        
                        dec_d = float(match.group(4))
                        dec_m = float(match.group(5))
                        dec_s = float(match.group(6))
                        dec_sign = -1 if dec_d < 0 else 1
                        dec = dec_sign * (abs(dec_d) + dec_m / 60 + dec_s / 3600)
                        
                        error = float(match.group(7))
                        error_unit = match.group(8)
                        
                        return ra, dec, error, error_unit
                    else:
                        # Standard decimal degree format
                        ra = float(match.group(1))
                        dec = float(match.group(2))
                        error = float(match.group(3))
                        # Get error unit if available, otherwise use default based on facility
                        error_unit = match.group(4) if len(match.groups()) >= 4 else 'deg' if 'Fermi' in facility else 'arcsec'
                        
                        return ra, dec, error, error_unit
                    
            logger.debug(f"Could not extract coordinates for facility {facility}")
            return None, None, None, None
        except Exception as e:
            logger.error(f"Error extracting coordinates: {e}")
            return None, None, None, None

    def _extract_event_name(self, subject: str) -> Optional[str]:
        """
        Extract GRB/EP event name from circular subject.
        
        Args:
            subject (str): The subject line of the circular
            
        Returns:
            Optional[str]: The extracted event name or None if not found
            
        Examples:
            >>> handler._extract_event_name("GRB 250322A: Swift/UVOT Upper Limits")
            "GRB 250322A"
        """
        try:
            for pattern in self.compiled_patterns['event_name']:
                match = pattern.search(subject)
                if match:
                    if 'GRB' in subject:
                        return f"GRB {match.group(1)}"
                    elif 'EP' in subject:
                        return f"EP {match.group(1)}"
                    else:
                        return match.group(1)
                
            # Try extracting using custom patterns based on subject structure
            grb_match = re.search(r'^(GRB\s+\d{6}[A-Za-z])', subject)
            if grb_match:
                return grb_match.group(1)
                
            logger.debug(f"Could not extract event name from subject: {subject}")
            return None
        except Exception as e:
            logger.error(f"Error extracting event name: {e}")
            return None

    def _extract_facility(self, subject: str, body: str) -> Optional[str]:
        """
        Extract facility name from circular subject and body.
        
        Args:
            subject (str): The subject line of the circular
            body (str): The body of the circular
            
        Returns:
            Optional[str]: The extracted facility name in standardized format or None if not found
            
        Examples:
            >>> handler._extract_facility("GRB 250322A: Swift/XRT Upper Limits", "...")
            "SwiftXRT"
        """
        try:
            # First, check the subject for common facility patterns
            for pattern, facility_base in self.compiled_patterns['facility']:
                if pattern.search(subject):
                    # Determine specific instrument
                    if 'BAT' in subject:
                        return f"{facility_base}BAT"
                    elif 'XRT' in subject:
                        return f"{facility_base}XRT"
                    elif 'UVOT' in subject:
                        return f"{facility_base}UVOT"
                    elif 'GBM' in subject:
                        return f"{facility_base}GBM"
                    elif 'LAT' in subject:
                        return f"{facility_base}LAT"
                    elif 'WXT' in subject or 'FXT' in subject:
                        return 'EinsteinProbe'
                    return facility_base
            
            # If not found in subject, check the first few lines of the body
            body_lines = body.split('\n')[:5]  # Check only first 5 lines
            body_text = ' '.join(body_lines)
            
            for pattern, facility_base in self.compiled_patterns['facility']:
                if pattern.search(body_text):
                    # Determine specific instrument from body
                    if 'BAT' in body_text:
                        return f"{facility_base}BAT"
                    elif 'XRT' in body_text:
                        return f"{facility_base}XRT"
                    elif 'UVOT' in body_text:
                        return f"{facility_base}UVOT"
                    elif 'GBM' in body_text:
                        return f"{facility_base}GBM"
                    elif 'LAT' in body_text:
                        return f"{facility_base}LAT"
                    elif 'WXT' in body_text or 'FXT' in body_text:
                        return 'EinsteinProbe'
                    return facility_base
                    
            # Look for facility name in the "on behalf of" text
            behalf_pattern = re.compile(r'(?:report|reports)\s+on\s+behalf\s+of\s+the\s+([\w\s\-/]+)(?:team|collaboration)', re.IGNORECASE)
            behalf_match = behalf_pattern.search(body)
            if behalf_match:
                facility_text = behalf_match.group(1).strip()
                if 'Swift' in facility_text:
                    if 'BAT' in facility_text:
                        return 'SwiftBAT'
                    elif 'XRT' in facility_text:
                        return 'SwiftXRT'
                    elif 'UVOT' in facility_text:
                        return 'SwiftUVOT'
                    return 'Swift'
                elif 'Fermi' in facility_text:
                    if 'GBM' in facility_text:
                        return 'FermiGBM'
                    elif 'LAT' in facility_text:
                        return 'FermiLAT'
                    return 'Fermi'
                elif 'Einstein' in facility_text or 'EP' in facility_text:
                    return 'EinsteinProbe'
                    
            logger.debug(f"Could not extract facility from circular: subject={subject[:30]}...")
            return None
        except Exception as e:
            logger.error(f"Error extracting facility: {e}")
            return None

    def _extract_trigger_number(self, subject: str, body: str, facility: Optional[str]) -> Optional[str]:
        """
        Extract trigger number from circular subject and body.
        
        Args:
            subject (str): The subject line of the circular
            body (str): The body of the circular
            facility (Optional[str]): The detected facility name, if available
            
        Returns:
            Optional[str]: The extracted trigger number or None if not found
            
        Examples:
            >>> handler._extract_trigger_number("...", "Fermi GBM trigger 764205327", "FermiGBM")
            "764205327"
        """
        try:
            if not facility:
                # Try to match any trigger number pattern if facility is unknown
                for facility_key, patterns in self.compiled_patterns['trigger_num'].items():
                    for pattern in patterns:
                        match = pattern.search(body)
                        if match:
                            # Handle special case for Fermi with two trigger numbers
                            if len(match.groups()) > 1 and 'Fermi' in facility_key and match.group(2):
                                logger.info(f"Extracted trigger number (Fermi dual format): {match.group(1)}")
                                return match.group(1)  # Use first trigger number
                            
                            logger.info(f"Extracted trigger number: {match.group(1)}")
                            return match.group(1)
                            
                # Check subject if not found in body
                for facility_key, patterns in self.compiled_patterns['trigger_num'].items():
                    for pattern in patterns:
                        match = pattern.search(subject)
                        if match:
                            if len(match.groups()) > 1 and 'Fermi' in facility_key and match.group(2):
                                logger.info(f"Extracted trigger number from subject (Fermi dual format): {match.group(1)}")
                                return match.group(1)
                            
                            logger.info(f"Extracted trigger number from subject: {match.group(1)}")
                            return match.group(1)
                return None
                
            # Facility-specific trigger extraction
            patterns_to_check = []
            if 'Swift' in facility:
                patterns_to_check = self.compiled_patterns['trigger_num']['Swift']
            elif 'Fermi' in facility:
                patterns_to_check = self.compiled_patterns['trigger_num']['Fermi']
            elif 'Einstein' in facility:
                patterns_to_check = self.compiled_patterns['trigger_num']['Einstein-Probe']
            else:
                # If unknown facility, try all patterns
                return self._extract_trigger_number(subject, body, None)
                
            # First check the body
            for pattern in patterns_to_check:
                match = pattern.search(body)
                if match:
                    # Fermi trigger numbers sometimes have format trigger=12345/67890
                    if len(match.groups()) > 1 and 'Fermi' in facility and match.group(2):
                        logger.info(f"Extracted trigger number (Fermi dual format): {match.group(1)}")
                        return match.group(1)  # Use first trigger number
                    
                    logger.info(f"Extracted trigger number: {match.group(1)}")
                    return match.group(1)
                    
            # Then check the subject if not found in body
            for pattern in patterns_to_check:
                match = pattern.search(subject)
                if match:
                    if len(match.groups()) > 1 and 'Fermi' in facility and match.group(2):
                        logger.info(f"Extracted trigger number from subject (Fermi dual format): {match.group(1)}")
                        return match.group(1)
                    
                    logger.info(f"Extracted trigger number from subject: {match.group(1)}")
                    return match.group(1)
                    
            # Try a generic approach if still not found - looking for numbers in parentheses in first 300 chars
            if not ('Einstein' in facility):  # Einstein Probe IDs can be complex, so skip this for them
                first_part = body[:300]
                number_matches = re.finditer(r"\((\d{5,})\)", first_part)
                for match in number_matches:
                    trigger_num = match.group(1)
                    if len(trigger_num) >= 5 and len(trigger_num) <= 10:  # Typical trigger number length
                        logger.info(f"Extracted trigger number using generic pattern: {trigger_num}")
                        return trigger_num
                
            logger.debug(f"Could not extract trigger number for facility {facility}")
            return None
        except Exception as e:
            logger.error(f"Error extracting trigger number: {e}")
            return None

    def _convert_error_to_degrees(self, error: float, error_unit: str) -> float:
        """
        Convert position error to degrees for ASCII file consistency.
        
        Args:
            error (float): The error value
            error_unit (str): The unit of the error ('arcsec', 'arcmin', 'deg')
            
        Returns:
            float: The error converted to degrees
            
        Examples:
            >>> handler._convert_error_to_degrees(3600, "arcsec")
            1.0
            >>> handler._convert_error_to_degrees(60, "arcmin") 
            1.0
        """
        try:
            if 'arcsec' in error_unit or '"' in error_unit:
                return error / 3600.0
            elif 'arcmin' in error_unit or "'" in error_unit:
                return error / 60.0
            elif 'deg' in error_unit:
                return error
            else:
                logger.warning(f"Unknown error unit: {error_unit}, assuming degrees")
                return error
        except Exception as e:
            logger.error(f"Error converting error to degrees: {e}")
            return error
            
    def process_circular(self, circular_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a GCN circular and extract relevant information.
        """
        try:
            subject = circular_data.get('subject', '')
            body = circular_data.get('body', '')
            circular_id = circular_data.get('circularId')
            created_on = circular_data.get('createdOn')
            
            logger.info(f"Processing circular {circular_id}: {subject}")
            
            # Extract basic information
            event_name = self._extract_event_name(subject)
            facility = self._extract_facility(subject, body)
            trigger_num = self._extract_trigger_number(subject, body, facility)
            false_trigger = self._check_false_trigger(body, facility)
            
            # Log extracted basic info
            logger.info(f"Circular {circular_id} - extracted: event={event_name}, facility={facility}, trigger={trigger_num}, false={false_trigger}")
            
            # Extract coordinates
            ra, dec, error, error_unit = self._extract_coordinates(body, facility)
            
            # Extract redshift and host info
            redshift, redshift_error = self._extract_redshift(body)
            host_info = self._extract_host_info(body)
            
            # Prepare result
            processed_data = {
                'circular_id': circular_id,
                'event_name': event_name,
                'subject': subject,
                'facility': facility,
                'trigger_num': trigger_num,
                'ra': ra,
                'dec': dec,
                'error': error,
                'error_unit': error_unit,
                'redshift': redshift,
                'redshift_error': redshift_error,
                'host_info': host_info,
                'false_trigger': false_trigger,
                'created_on': created_on,
                'processed_on': datetime.now().timestamp() * 1000  # Milliseconds timestamp
            }
            
            logger.info(f"Successfully processed circular {circular_id} - complete information extracted")
            return processed_data
        except Exception as e:
            logger.error(f"Error processing circular: {e}", exc_info=True)
            # Return basic information even if extraction fails
            return {
                'circular_id': circular_data.get('circularId'),
                'subject': circular_data.get('subject', ''),
                'created_on': circular_data.get('createdOn'),
                'processed_on': datetime.now().timestamp() * 1000
            }
        
    def _update_csv_database(self, processed_data: Dict[str, Any]) -> None:
        """
        Update the CSV database with processed circular data.
        
        Args:
            processed_data (Dict[str, Any]): The processed circular data
        """
        with self.file_lock:
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(os.path.abspath(self.output_csv)), exist_ok=True)
                
                # Load existing CSV or create new one if it doesn't exist
                if os.path.exists(self.output_csv):
                    df = pd.read_csv(self.output_csv)
                    
                    # Ensure trigger_num is stored as string to avoid dtype issues
                    if 'trigger_num' in df.columns:
                        df['trigger_num'] = df['trigger_num'].astype(str).replace('nan', '')
                else:
                    # Create an empty DataFrame with proper dtypes
                    empty_data = {
                        'circular_id': pd.Series(dtype='int64'),
                        'event_name': pd.Series(dtype='object'),
                        'subject': pd.Series(dtype='object'),
                        'facility': pd.Series(dtype='object'),
                        'trigger_num': pd.Series(dtype='object'),  # Store as string to avoid dtype issues
                        'ra': pd.Series(dtype='float64'),
                        'dec': pd.Series(dtype='float64'),
                        'error': pd.Series(dtype='float64'),
                        'error_unit': pd.Series(dtype='object'),
                        'redshift': pd.Series(dtype='float64'),
                        'redshift_error': pd.Series(dtype='float64'),
                        'host_info': pd.Series(dtype='object'),
                        'false_trigger': pd.Series(dtype='bool'),
                        'created_on': pd.Series(dtype='float64'),
                        'processed_on': pd.Series(dtype='float64')
                    }
                    
                    # Only keep columns that are in self.csv_columns
                    columns_to_keep = [col for col in self.csv_columns if col in empty_data]
                    df = pd.DataFrame({col: empty_data[col] for col in columns_to_keep})
                
                # Check if this circular is already in database
                if 'circular_id' in df.columns and processed_data['circular_id'] in df['circular_id'].values:
                    # Update existing entry
                    idx = df[df['circular_id'] == processed_data['circular_id']].index[0]
                    for key in processed_data:
                        if key in df.columns and processed_data[key] is not None:
                            try:
                                # For trigger_num, always store as string
                                if key == 'trigger_num':
                                    df.at[idx, key] = str(processed_data[key])
                                else:
                                    df.at[idx, key] = processed_data[key]
                            except Exception as e:
                                logger.warning(f"Could not update column {key}: {e}")
                else:
                    # Add new entry
                    new_row = {}
                    for col in self.csv_columns:
                        value = processed_data.get(col)
                        
                        # Special handling for trigger_num to ensure string storage
                        if col == 'trigger_num' and value is not None:
                            new_row[col] = str(value)
                        else:
                            new_row[col] = value
                    
                    # Create a new DataFrame with the new row
                    new_df = pd.DataFrame([new_row])
                    
                    # Make sure all columns exist in both DataFrames before concatenation
                    for col in df.columns:
                        if col not in new_df:
                            new_df[col] = None
                            
                    for col in new_df.columns:
                        if col not in df.columns:
                            df[col] = None
                    
                    # Concatenate with original dataframe
                    df = pd.concat([df, new_df], ignore_index=True)
                
                # Save updated dataframe
                df.to_csv(self.output_csv, index=False)
                logger.info(f"CSV database updated with circular ID {processed_data['circular_id']}")
                
            except Exception as e:
                logger.error(f"Error updating CSV database: {e}", exc_info=True)
                
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

    def _update_ascii_database(self, processed_data: Dict[str, Any]) -> None:
        """
        Update the ASCII database with processed circular data.
        """
        # Skip if essential data is missing
        if not processed_data.get('event_name') or not processed_data.get('facility') or not processed_data.get('trigger_num'):
            logger.warning(f"Skipping ASCII update due to missing essential data: event_name={processed_data.get('event_name')}, facility={processed_data.get('facility')}, trigger_num={processed_data.get('trigger_num')}")
            return
                    
        # Skip if it's a false trigger
        if processed_data.get('false_trigger'):
            logger.info(f"Skipping ASCII update for false trigger: {processed_data.get('event_name')}")
            # Remove this event from ASCII if it exists
            self._remove_false_trigger_from_ascii(processed_data)
            return
                    
        with self.file_lock:
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(os.path.abspath(self.output_ascii)), exist_ok=True)
                
                # Read the file as raw lines to handle different GCN_ID formats
                lines = []
                header = None
                if os.path.exists(self.output_ascii):
                    with open(self.output_ascii, 'r') as f:
                        header = f.readline().strip().split()
                        for line in f:
                            if line.strip():
                                lines.append(line.strip())
                    logger.info(f"Loaded ASCII file with {len(lines)} entries")
                else:
                    header = self.ascii_columns
                    logger.info(f"Created new ASCII file with columns: {header}")
                
                # Parse the lines into a DataFrame manually
                data = []
                for line in lines:
                    # Handle quoted strings properly
                    fields = []
                    in_quotes = False
                    current_field = ""
                    
                    for char in line + " ":  # Add space at end to handle last field
                        if char == '"':
                            in_quotes = not in_quotes
                            current_field += char
                        elif char == ' ' and not in_quotes:
                            if current_field:
                                fields.append(current_field)
                                current_field = ""
                        else:
                            current_field += char
                    
                    # Ensure we have exactly the right number of columns
                    if len(fields) == len(header):
                        data.append(dict(zip(header, fields)))
                    else:
                        logger.warning(f"Skipping line with incorrect number of fields: {line}")
                
                # Convert to DataFrame
                df = pd.DataFrame(data) if data else pd.DataFrame(columns=header)
                
                # Get current circular info
                facility = processed_data['facility']
                trigger_num = processed_data['trigger_num']
                circular_id = processed_data.get('circular_id')
                
                # Get normalized facility name for matching
                normalized_facility = self._normalize_facility_name(facility)
                
                # Check if this event is already in database by matching normalized facility and trigger_num
                existing_idx = None
                
                for idx, row in df.iterrows():
                    row_facility = row.get('Facility', '')
                    row_trigger = row.get('Trigger_num', '')
                    
                    # Normalize the row's facility
                    normalized_row_facility = self._normalize_facility_name(row_facility)
                    
                    # Check if normalized facilities and trigger numbers match
                    if (normalized_row_facility == normalized_facility and 
                        str(row_trigger) == str(trigger_num)):
                        existing_idx = idx
                        break
                
                # Now proceed with update using the circular ID as GCN_ID
                if existing_idx is not None:
                    # Update existing entry
                    logger.info(f"Found existing entry for normalized facility={normalized_facility}, trigger={trigger_num} at index {idx}")
                    
                    # Always update GCN_ID to use the circular ID if available
                    if circular_id:
                        df.at[idx, 'GCN_ID'] = str(circular_id)
                        logger.info(f"Updated GCN_ID to circular ID: {circular_id}")
                    
                    # Rest of the update logic...
                    # [continue with your existing update logic]
                else:
                    # Add new entry if we have coordinates
                    if processed_data['ra'] is not None and processed_data['dec'] is not None and processed_data['error'] is not None:
                        error_in_degrees = self._convert_error_to_degrees(processed_data['error'], processed_data['error_unit'])
                        
                        # Use circular_id directly as GCN_ID
                        new_row = {
                            'GCN_ID': str(circular_id) if circular_id else f"GCN_{facility}_{trigger_num}",
                            'Name': processed_data["event_name"],  # No quotes here - they'll be added during file writing
                            'RA': processed_data['ra'],
                            'DEC': processed_data['dec'],
                            'Error': error_in_degrees,
                            'Discovery_UTC': datetime.fromtimestamp(processed_data["created_on"]/1000).strftime("%Y-%m-%d %H:%M:%S"),  # No quotes
                            'Facility': processed_data['facility'],
                            'Trigger_num': processed_data['trigger_num'],
                            'Notice_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # No quotes
                            'Redshift': processed_data['redshift'] if processed_data['redshift'] is not None else '',
                            'Host_info': processed_data["host_info"] if processed_data['host_info'] else ''  # No quotes
                        }
                        
                        # Add to DataFrame
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                        logger.info(f"Added new entry to ASCII database: {new_row['GCN_ID']} with RA={processed_data['ra']}, DEC={processed_data['dec']}")
                
                # Save the DataFrame to the ASCII file with proper spacing and quoting
                with open(self.output_ascii, 'w') as f:
                    # Write header
                    f.write(" ".join(header) + "\n")
                    
                    # Write each row manually to ensure proper formatting
                    for _, row in df.iterrows():
                        row_values = []
                        for col in header:
                            val = row.get(col, '')
                            # Ensure proper quoting for strings with spaces
                            if isinstance(val, str) and (' ' in val or col in ['Name', 'Discovery_UTC', 'Notice_date', 'Host_info']):
                                if not (val.startswith('"') and val.endswith('"')):
                                    val = f'"{val}"'
                            row_values.append(str(val))
                        
                        f.write(" ".join(row_values) + "\n")
                    
                logger.info(f"ASCII database updated for event {processed_data['event_name']} - Saved to {self.output_ascii}")
                    
            except Exception as e:
                logger.error(f"Error updating ASCII database: {e}", exc_info=True)
    
    def _remove_false_trigger_from_ascii(self, processed_data: Dict[str, Any]) -> None:
        """
        Remove a false trigger from the ASCII database if it exists.
        Uses normalized facility names for matching but preserves original GCN_IDs.
        
        Args:
            processed_data (Dict[str, Any]): The processed circular data containing false trigger
        """
        if not os.path.exists(self.output_ascii):
            return
            
        if not processed_data.get('facility') or not processed_data.get('trigger_num'):
            return
            
        with self.file_lock:
            try:
                # Load existing ASCII
                df = pd.read_csv(self.output_ascii, sep=' ', header=0, dtype=str)
                
                # Get normalized facility name for the processed data
                facility = processed_data['facility']
                trigger_num = processed_data['trigger_num']
                normalized_facility = self._normalize_facility_name(facility)
                
                # Find rows to remove - any with matching normalized facility and trigger
                rows_to_remove = []
                gcn_ids_to_remove = []
                
                for idx, row in df.iterrows():
                    row_facility = row.get('Facility', '')
                    row_trigger = row.get('Trigger_num', '')
                    row_gcn_id = row.get('GCN_ID', '')
                    
                    # Normalize the row's facility
                    normalized_row_facility = self._normalize_facility_name(row_facility)
                    
                    # Check if normalized facilities and trigger numbers match
                    if (normalized_row_facility == normalized_facility and 
                        str(row_trigger) == str(trigger_num)):
                        rows_to_remove.append(idx)
                        gcn_ids_to_remove.append(row_gcn_id)
                
                if rows_to_remove:
                    # Remove the false triggers
                    df = df.drop(rows_to_remove)
                    
                    # Save updated dataframe
                    df.to_csv(self.output_ascii, sep=' ', index=False, na_rep='', quoting=csv.QUOTE_MINIMAL)
                    
                    # Log the removal with original GCN_IDs for reference
                    gcn_ids_str = ", ".join(gcn_ids_to_remove)
                    logger.info(f"Removed {len(rows_to_remove)} false trigger entries matching {normalized_facility}_{trigger_num} from ASCII database. GCN_IDs: {gcn_ids_str}")
                else:
                    logger.debug(f"No entries found to remove for false trigger {normalized_facility}_{trigger_num}")
                    
            except Exception as e:
                logger.error(f"Error removing false trigger from ASCII database: {e}", exc_info=True)
                
    def process_circular_from_json(self, json_str: str) -> None:
        """
        Process a GCN circular from JSON string.
        
        Args:
            json_str (str): The circular data in JSON format
            
        Examples:
            >>> handler.process_circular_from_json('{"subject": "GRB 250322A", "body": "...", "circularId": 12345}')
            # Processes the circular and updates databases
        """
        try:
            circular_data = json.loads(json_str)
            processed_data = self.process_circular(circular_data)
            
            # Update databases
            self._update_csv_database(processed_data)
            self._update_ascii_database(processed_data)
            
            logger.info(f"Successfully processed circular {processed_data['circular_id']}: {processed_data['subject']}")
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error processing circular: {e}")
        except Exception as e:
            logger.error(f"Error processing circular: {e}", exc_info=True)
            
    def process_circular_from_file(self, file_path: str) -> None:
        """
        Process a GCN circular from a JSON file.
        
        Args:
            file_path (str): Path to the JSON file
            
        Examples:
            >>> handler.process_circular_from_file("circulars/circular12345.json")
            # Processes the circular from the file and updates databases
        """
        try:
            with open(file_path, 'r') as f:
                json_str = f.read()
            self.process_circular_from_json(json_str)
            
        except (IOError, FileNotFoundError) as e:
            logger.error(f"File error processing circular from {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error processing circular from file {file_path}: {e}", exc_info=True)
            
    def monitor_circulars(self, timeout: int = 0) -> None:
        """
        Monitor for new GCN circulars via Kafka and process them.
        
        Args:
            timeout (int, optional): Maximum time to monitor in seconds.
                                    Set to 0 for indefinite monitoring.
            
        Examples:
            >>> handler.monitor_circulars(timeout=1800)  # Monitor for 30 minutes
            # Monitors for new circulars and processes them as they arrive
        """
        logger.info("Starting GCN circular monitoring...")
        
        if not self.consumer:
            logger.error("No Kafka consumer initialized. Please initialize with client credentials.")
            return
            
        start_time = time.time()
        try:
            while True:
                # Check timeout if specified
                if timeout > 0 and (time.time() - start_time) > timeout:
                    logger.info(f"Monitoring timeout reached ({timeout}s)")
                    break
                    
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                    
                # Process the message
                json_str = msg.value().decode('utf-8')
                self.process_circular_from_json(json_str)
                
        except KeyboardInterrupt:
            logger.info("Circular monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in circular monitoring: {e}", exc_info=True)
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Circular monitoring stopped")
    
    def process_batch_from_directory(self, directory_path: str) -> None:
        """
        Process a batch of GCN circulars from a directory of JSON files.
        
        Args:
            directory_path (str): Path to directory containing JSON circular files
            
        Examples:
            >>> handler.process_batch_from_directory("circulars/")
            # Processes all JSON files in the directory
        """
        try:
            if not os.path.exists(directory_path):
                logger.error(f"Directory not found: {directory_path}")
                return
                
            files = [f for f in os.listdir(directory_path) if f.endswith('.json')]
            logger.info(f"Found {len(files)} JSON files to process")
            
            for file_name in files:
                file_path = os.path.join(directory_path, file_name)
                self.process_circular_from_file(file_path)
                
            logger.info(f"Completed batch processing of {len(files)} files")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}", exc_info=True)
            
def main():
    """Main function to run the GCN Circular Handler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='GCN Circular Handler')
    parser.add_argument('--batch-directory', help='Directory of JSON files to process in batch mode')
    parser.add_argument('--file', help='Single JSON file to process')
    parser.add_argument('--monitor', action='store_true', help='Monitor for new circulars')
    parser.add_argument('--timeout', type=int, default=0, help='Monitoring timeout in seconds (0 for indefinite)')
    
    args = parser.parse_args()
    
    # Try to import configuration, default to empty values if not available
    try:
        from config import OUTPUT_CIRCULAR_CSV, OUTPUT_ASCII, GCN_ID, GCN_SECRET
    except ImportError:
        logger.warning("Config file not found, using default values")
        OUTPUT_CIRCULAR_CSV = 'gcn_circulars.csv'
        OUTPUT_ASCII = 'grb_targets.ascii'
        GCN_ID = ''
        GCN_SECRET = ''
    
    # Initialize handler with client credentials if provided
    handler = GCNCircularHandler(
        output_csv=OUTPUT_CIRCULAR_CSV,
        output_ascii=OUTPUT_ASCII,
        client_id=GCN_ID,
        client_secret=GCN_SECRET
    )
    
    if args.file:
        # Process a single file
        handler.process_circular_from_file(args.file)
    elif args.batch_directory:
        # Process a batch of files
        handler.process_batch_from_directory(args.batch_directory)
    elif args.monitor:
        # Monitor for circulars
        handler.monitor_circulars(timeout=args.timeout)
    else:
        # Print usage if no arguments provided
        parser.print_help()

if __name__ == "__main__":
    main()