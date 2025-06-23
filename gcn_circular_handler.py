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
        self.patterns = {
            'position': {
                'Swift-XRT': [
                    r"Enhanced Swift-XRT position.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA\s*\(J2000\):\s*(\d{2})h\s*(\d{2})m\s*([\d.]+)s.*?Dec\s*\(J2000\):\s*([-+]?\d{2})d\s*(\d{2})\'\s*([\d.]+)\".*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA,\s*Dec[:\s]*=?\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA,\s*Dec[:\s]*=?\s*([\d.]+),\s*([-+]?[\d.]+).*?with\s+an\s+uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA,\s*Dec:\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"located\s+at\s+RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ],
                'Swift-BAT': [
                    r"BAT.*?RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcmin]+)",
                    r"RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcmin]+)",
                ],
                'Swift': [
                    r"RA\s*\(J2000\)\s*=\s*(\d{2})h\s*(\d{2})m\s*([\d.]+)s.*?Dec\s*\(J2000\)\s*=\s*([-+]?\d{2})d\s*(\d{2})\'\s*([\d.]+)\".*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ],
                'Fermi': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?error\s+radius\s+of\s+([\d.]+)\s*([\"\'deg]+)",
                ],
                'IceCube': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'deg]+)",
                    r"best\s+fit\s+position.*?RA\s*[=:]\s*([\d.]+).*?Dec\s*[=:]\s*([-+]?[\d.]+).*?uncertainty.*?([\d.]+)\s*([\"\'deg]+)",
                ],
                'CALET': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?error\s+(?:radius|box)\s+of\s+([\d.]+)\s*([\"\'deg]+)",
                ],
                'SVOM': [
                    r"R\.A\.,\s*Dec\.\s*([\d.]+),\s*([-+]?[\d.]+)\s*degrees.*?radius\s+of\s+([\d.]+)\s*([\"\'arcmin]+)",
                    r"RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?error\s+radius\s+of\s+([\d.]+)\s*([\"\'arcmin\s]+)",
                ],
                'GECAM': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?error\s+radius\s+of\s+([\d.]+)\s*([\"\'deg]+)",
                ],
                'AMON': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty.*?([\d.]+)\s*([\"\'deg]+)",
                ],
                'HAWC': [
                    r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?containment.*?([\d.]+)\s*([\"\'deg]+)",
                ],
            },
            'false_trigger': {
                'general': [
                    r"not\s+(?:due\s+to\s+)?(?:a\s+)?GRB",
                    r"false\s+(?:positive|trigger|alarm)",
                    r"not\s+a\s+(?:real\s+)?(?:burst|GRB)",
                    r"(?:likely\s+)?(?:due\s+to|caused\s+by)\s+(?:local\s+particles|SAA|background)",
                    r"retraction",
                ],
                'Swift': [
                    r"Swift.*?(?:false|not\s+a\s+GRB)",
                ],
                'Fermi': [
                    r"Fermi.*?(?:not\s+due\s+to\s+a\s+GRB|false\s+trigger)",
                    r"GBM.*?(?:not\s+due\s+to\s+a\s+GRB|false\s+trigger)",
                ],
                'GECAM': [
                    r"GECAM.*?(?:false|not\s+a\s+GRB)",
                ],
                'SVOM': [
                    r"SVOM.*?(?:false|not\s+a\s+GRB)",
                ],
            },
            'redshift': {
                'general': [
                    r"redshift\s+(?:of\s+)?z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"at\s+z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"common\s+redshift\s+(?:of\s+)?z?\s*=?\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"redshift\s+at\s+z\s*(?:=|~)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"spectroscopic\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"measured\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"confirmed\s+redshift.*?(?:of|=)\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                    r"common\s+redshift\s+of\s+([\d.]+)±([\d.]+)",
                ]
            },
            'host_galaxy': {
                'general': [
                    r"host\s+galaxy.*?([^\.;]+(?:galaxy|mag|magnitude)[^\.;]+)",
                    r"([^\.;]+host\s+galaxy[^\.;]+)",
                    r"putative\s+host.*?([^\.;]+)",
                    r"bright\s+galaxy\s+within\s+the\s+localization.*?([^\.;]+)",
                    r"supporting\s+it\s+as\s+a\s+likely\s+host\s+galaxy\s+of\s+the\s+GRB",
                    r"coincidence\s+between\s+the\s+(?:bright\s+)?galaxy\s+and\s+the\s+XRT\s+localization.*?([^\.;]+)",
                    r"([^\.;]+likely\s+host\s+galaxy[^\.;]+)",
                ]
            },
            'trigger_num': {
                'Swift': [
                    r"\(trigger\s*=\s*(\d+)\)",
                    r"trigger\s*=\s*(\d+)",
                    r"trigger\s*[:\s]\s*(\d+)",
                    r"(?:Swift|BAT).*?trigger(?:[=:])?\s*(\d+)",
                    r"Swift\s+Trigger\s+(\d+)",
                    r"BAT\s+trigger\s+#?(\d+)",
                    r"(?:after|of)\s+the\s+BAT\s+trigger\s+\(.*?GCN\s+Circ\.\s+(\d+)\)",
                    r"Trigger\s+Number\s*:\s*(\d+)",
                    r"\(Gupta\s+et\s+al\.,\s+GCN\s+Circ\.\s+(\d+)\)",
                    r"GCN\s+Circ\.\s+(\d+)",
                    r"GCN\s+Circular\s+(\d+)",
                    r"for\s+GRB.*?\(.*?(\d{6,})\)",
                ],
                'Fermi': [
                    r"(?:Fermi-GBM|GBM)\s+(?:\()?trigger\s+([\d/]+)",
                    r"trigger(?:[=:])?\s*(\d+)/(\d+)",
                    r"Fermi.*?trigger\s+(\d+)",
                    r"GBM.*?trigger\s+#?(\d+)",
                    r"Fermi-GBM\s+\(trigger\s+(\d+)\s*/\s*\d+",
                    r"\(trigger\s+(\d+)\s*/\s*\d+",
                ],
                'Einstein-Probe': [
                    r"EP-WXT\s+trigger\s+([\w\d]+)",
                    r"EP.*?ID\s*(?::|=)\s*(?:\[)?[\'\"]*(\d+)[\'\"]*(?:\])?",
                    r"EP.*?trigger\s+#?(\d+)",
                ],
                'IceCube': [
                    r"IceCube.*?event\s+(\w+)",
                    r"Event\s+ID[:\s]*(\w+)",
                    r"IC\d+\s+(\w+)",
                    r"alert\s+(\w+)",
                    r"neutrino\s+event.*?(\w+)",
                ],
                'CALET': [
                    r"CALET.*?trigger\s+(\d+)",
                    r"trigger\s+(\d+).*?CALET",
                    r"event\s+(\d+).*?CALET",
                ],
                'SVOM': [
                    r"sb\d+",
                    r"SVOM.*?burst-id\s+(sb\d+)",
                    r"SVOM.*?trigger\s+(\w+)",
                ],
                'GECAM': [
                    r"GECAM.*?GRB\s+(\d{6}[A-Z])",
                    r"burst\s+GRB\s+(\d{6}[A-Z])",
                    r"GRB\s+(\d{6}[A-Z]).*?GECAM",
                ],
                'AMON': [
                    r"Event\s+(\d+)",
                    r"AMON.*?(\d+)",
                    r"coincidence.*?(\d+)",
                ],
                'HAWC': [
                    r"HAWC.*?trigger\s+(\d+)",
                    r"burst.*?(\d+)",
                ],
                'LVC': [
                    r"(S\d+\w+)",
                    r"LIGO.*?(S\d+\w+)",
                    r"Virgo.*?(S\d+\w+)",
                    r"LVK.*?(S\d+\w+)",
                    r"GW\d+",
                ],
            },
            'facility': [
                (r'Swift[\s/\-]?(?:BAT|XRT|UVOT)', 'Swift'),
                (r'Fermi[\s/\-]?(?:GBM|LAT)', 'Fermi'),
                (r'GECAM', 'GECAM'),
                (r'SVOM|ECLAIRs', 'SVOM'),
                (r'CALET', 'CALET'),
            ],
            'event_name': [
                r'(?:GRB|grb)\s+(\d{6}[A-Za-z])',
                r'(?:GRB|grb)(\d{6}[A-Za-z])',
            ],
        }
        
        # Compile all patterns for performance
        self.compiled_patterns = {}
        
        # Compile position patterns
        self.compiled_patterns['position'] = {}
        for facility, patterns in self.patterns['position'].items():
            self.compiled_patterns['position'][facility] = [
                re.compile(pattern, re.IGNORECASE | re.DOTALL) for pattern in patterns
            ]
        
        # Compile trigger number patterns
        self.compiled_patterns['trigger_num'] = {}
        for facility, patterns in self.patterns['trigger_num'].items():
            self.compiled_patterns['trigger_num'][facility] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile false trigger patterns
        self.compiled_patterns['false_trigger'] = {}
        for facility, patterns in self.patterns['false_trigger'].items():
            self.compiled_patterns['false_trigger'][facility] = [
                re.compile(pattern, re.IGNORECASE) for pattern in patterns
            ]
        
        # Compile redshift patterns
        self.compiled_patterns['redshift'] = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.patterns['redshift']['general']
        ]
        
        # Compile host galaxy patterns
        self.compiled_patterns['host_galaxy'] = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.patterns['host_galaxy']['general']
        ]
        
        # Compile facility patterns
        self.compiled_patterns['facility'] = [
            (re.compile(pattern, re.IGNORECASE), facility) for pattern, facility in self.patterns['facility']
        ]
        
        # Compile event name patterns
        self.compiled_patterns['event_name'] = [
            re.compile(pattern, re.IGNORECASE) for pattern in self.patterns['event_name']
        ]
        
        logger.info("Compiled all regex patterns for GCN circular processing")
        
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

    def _extract_trigger_number(self, subject: str, body: str, facility: str = None) -> Optional[str]:
        """
        Extract trigger number from circular with improved error handling.
        """
        try:
            # Try multiple patterns based on facility
            patterns = []
            
            if facility and 'Fermi' in facility:
                patterns.extend([
                    r'trigger\s+(\d+)/?(\d+)',  # Fermi dual format
                    r'trigger\s+(\d+)',         # Single trigger
                    r'GBM\s+trigger\s+(\d+)',   # GBM specific
                    r'LAT\s+trigger\s+(\d+)',   # LAT specific
                ])
            
            if facility and 'Swift' in facility:
                patterns.extend([
                    r'trigger\s+(\d+)',
                    r'BAT\s+trigger\s+(\d+)',
                    r'XRT\s+trigger\s+(\d+)',
                    r'trigger\s+number\s+(\d+)',
                    r'\(trigger\s*=\s*(\d+)\)',
                ])
            
            # General patterns
            patterns.extend([
                r'trigger[:\s]+(\d+)',
                r'Trigger\s+Number[:\s]+(\d+)',
                r'#(\d+)',  # For numbered triggers
                r'(\d{8,})',  # Long number sequences
            ])
            
            # Try each pattern with error handling
            combined_text = subject + " " + body
            for pattern in patterns:
                try:
                    match = re.search(pattern, combined_text, re.IGNORECASE)
                    if match and match.groups():
                        # For Fermi dual format, return first number
                        trigger_num = match.group(1)
                        if trigger_num and trigger_num.isdigit():
                            if facility and 'Fermi' in facility:
                                logger.info(f"Extracted trigger number (Fermi dual format): {trigger_num}")
                            else:
                                logger.info(f"Extracted trigger number: {trigger_num}")
                            return trigger_num
                except (AttributeError, IndexError) as e:
                    logger.debug(f"Pattern '{pattern}' failed: {e}")
                    continue
            
            logger.debug("No trigger number found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting trigger number: {e}")
            return None

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

    def _extract_coordinates(self, body: str, facility: str = None) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
        """
        Extract coordinates with improved error handling.
        FIXED: Better handling of regex group access and type conversions.
        """
        try:
            # Your existing coordinate extraction logic here
            # but wrapped in try-catch for each step
            
            if facility and facility in self.patterns.get('position', {}):
                patterns = self.patterns['position'][facility]
                
                for pattern in patterns:
                    try:
                        match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
                        if match and match.groups():
                            groups = match.groups()
                            if len(groups) >= 3:  # Check if we have enough groups
                                ra = float(groups[0])
                                dec = float(groups[1])
                                error = float(groups[2])
                                error_unit = groups[3] if len(groups) > 3 and groups[3] else 'arcsec'
                                logger.info(f"Extracted coordinates: RA={ra}, Dec={dec}, Error={error} {error_unit}")
                                return ra, dec, error, error_unit
                    except (ValueError, IndexError, TypeError) as e:
                        logger.debug(f"Pattern failed: {e}")
                        continue
            
            # Fallback to general patterns
            general_patterns = [
                r'RA[:\s=]*(\d+\.?\d*)[,\s]+Dec[:\s=]*([-+]?\d+\.?\d*)',
                r'RA\s*=\s*(\d+\.?\d*),?\s*Dec\s*=\s*([-+]?\d+\.?\d*)',
                r'located\s+at\s+RA[:\s=]*(\d+\.?\d*)[,\s]+Dec[:\s=]*([-+]?\d+\.?\d*)',
            ]
            
            for pattern in general_patterns:
                try:
                    match = re.search(pattern, body, re.IGNORECASE)
                    if match and match.groups() and len(match.groups()) >= 2:
                        ra = float(match.group(1))
                        dec = float(match.group(2))
                        logger.info(f"Extracted coordinates (general): RA={ra}, Dec={dec}")
                        return ra, dec, None, None
                except (ValueError, IndexError, TypeError, AttributeError):
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting coordinates: {e}")
        
        return None, None, None, None

    def _extract_redshift(self, body: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract redshift with improved error handling.
        """
        try:
            patterns = [
                r'redshift\s+(?:of\s+)?z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?',
                r'at\s+z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?',
                r'z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?',
                r'redshift.*?(\d+\.?\d*)(?:\s*±\s*([\d.]+))?',
            ]
            
            for pattern in patterns:
                try:
                    match = re.search(pattern, body, re.IGNORECASE)
                    if match and match.groups():
                        redshift = float(match.group(1))
                        redshift_error = None
                        if len(match.groups()) > 1 and match.group(2):
                            try:
                                redshift_error = float(match.group(2))
                            except (ValueError, TypeError):
                                pass
                        
                        logger.info(f"Extracted redshift: z={redshift}{f' ± {redshift_error}' if redshift_error else ''}")
                        return redshift, redshift_error
                except (ValueError, IndexError, TypeError, AttributeError):
                    continue
                    
            return None, None
            
        except Exception as e:
            logger.error(f"Error extracting redshift: {e}")
            return None, None

    def _extract_host_info(self, body: str) -> Optional[str]:
        """
        Extract host information with improved error handling.
        """
        try:
            patterns = [
                r'host\s+galaxy.*?([^.\n]+)',
                r'host[:\s]*([^.\n]+)',
                r'galaxy[:\s]*([^.\n]+)',
                r'supporting\s+it\s+as\s+a\s+likely\s+host\s+galaxy\s+of\s+the\s+GRB',
                r'bright\s+galaxy\s+within.*?([^.\n]+)',
            ]
            
            for pattern in patterns:
                try:
                    match = re.search(pattern, body, re.IGNORECASE)
                    if match and match.groups():
                        host_info = match.group(1).strip()
                        if host_info and len(host_info) > 3:  # Minimum meaningful length
                            logger.info(f"Extracted host info: {host_info[:50]}...")
                            return host_info
                except (AttributeError, IndexError, TypeError):
                    continue
                    
            return None
            
        except Exception as e:
            logger.error(f"Error extracting host info: {e}")
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
                    df = self._safe_concat_dataframes(df, new_df, ignore_index=True)
                
                # Save updated dataframe
                df.to_csv(self.output_csv, index=False)
                logger.info(f"CSV database updated with circular ID {processed_data['circular_id']}")
                
            except Exception as e:
                logger.error(f"Error updating CSV database: {e}", exc_info=True)
    
    def _safe_concat_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, ignore_index: bool = True) -> pd.DataFrame:
        """
        Safely concatenate two DataFrames, handling empty DataFrames and avoiding FutureWarnings.
        """
        # If first DataFrame is empty, return the second one
        if df1.empty:
            return df2.copy() if not df2.empty else df2
        
        # If second DataFrame is empty, return the first one
        if df2.empty:
            return df1.copy()
        
        # Both DataFrames have data - ensure column compatibility
        for col in df1.columns:
            if col not in df2.columns:
                df2[col] = None
                
        for col in df2.columns:
            if col not in df1.columns:
                df1[col] = None
        
        # Reorder columns to match
        df2 = df2[df1.columns]
        
        # Now safe to concatenate
        return pd.concat([df1, df2], ignore_index=ignore_index)
    
    def _normalize_facility_name(self, facility: str) -> str:
        """
        Normalize facility names for consistent matching.
        Focused on core GRB detection facilities.
        """
        if not facility:
            return ""
            
        facility = facility.strip()
        
        # Swift family normalization
        swift_mappings = {
            'Swift': 'Swift',
            'SwiftBAT': 'Swift',
            'SwiftXRT': 'Swift', 
            'SwiftUVOT': 'Swift',
            'Swift-BAT': 'Swift',
            'Swift-XRT': 'Swift',
            'Swift-UVOT': 'Swift',
            'Swift/BAT': 'Swift',
            'Swift/XRT': 'Swift',
            'Swift/UVOT': 'Swift',
            'Swift BAT': 'Swift',
            'Swift XRT': 'Swift',
            'Swift UVOT': 'Swift',
        }
        
        # Check Swift mappings first
        for key, value in swift_mappings.items():
            if key.lower() in facility.lower():
                return value
                
        # Fermi family normalization  
        if 'Fermi' in facility or 'GBM' in facility or 'LAT' in facility:
            return 'Fermi'
            
        # GECAM normalization
        if 'GECAM' in facility:
            return 'GECAM'
            
        # SVOM normalization
        if 'SVOM' in facility or 'ECLAIRs' in facility:
            return 'SVOM'
            
        # CALET normalization
        if 'CALET' in facility:
            return 'CALET'
            
        return facility

    def _load_ascii_with_recovery(self, filepath: str) -> pd.DataFrame:
        """
        Load ASCII file using pandas + manual recovery for skipped lines.
        Drop-in replacement for pd.read_csv() with zero data loss.
        """
        if not os.path.exists(filepath):
            return pd.DataFrame(columns=self.ascii_columns)
        
        # Read original lines for comparison
        with open(filepath, 'r') as f:
            all_lines = f.readlines()
        
        if len(all_lines) <= 1:
            return pd.DataFrame(columns=self.ascii_columns)
        
        header_line = all_lines[0].strip()
        data_lines = [line.strip() for line in all_lines[1:] if line.strip()]
        total_data_lines = len(data_lines)
        
        # Try pandas first (your existing approach)
        try:
            df = pd.read_csv(filepath, sep=' ', header=0, dtype=str)
            pandas_count = len(df)
            
            logger.info(f"pandas loaded {pandas_count}/{total_data_lines} entries")
            
            # If pandas got everything, return as-is (fast path)
            if pandas_count == total_data_lines:
                return df
                
            # Some lines were skipped - recover them
            logger.info(f"Recovering {total_data_lines - pandas_count} skipped lines...")
            
            # Convert pandas result to list for easier manipulation
            pandas_rows = df.to_dict('records')
            
            # Create signatures of successfully parsed rows
            parsed_signatures = set()
            for row in pandas_rows:
                sig = f"{row.get('GCN_ID', '')}__{row.get('Trigger_num', '')}__{row.get('Name', '')}"
                parsed_signatures.add(sig)
            
            # Manually parse all lines and find missing ones
            recovered_rows = []
            for line in data_lines:
                try:
                    manual_row = self._parse_line_to_dict(line, header_line.split())
                    if manual_row:
                        manual_sig = f"{manual_row.get('GCN_ID', '')}__{manual_row.get('Trigger_num', '')}__{manual_row.get('Name', '')}"
                        
                        if manual_sig not in parsed_signatures:
                            recovered_rows.append(manual_row)
                            logger.info(f"Recovered: {manual_row.get('GCN_ID', 'Unknown')}")
                except:
                    continue
            
            # Combine results and return as DataFrame
            all_rows = pandas_rows + recovered_rows
            logger.info(f"Final: {len(pandas_rows)} pandas + {len(recovered_rows)} recovered = {len(all_rows)} total")
            
            return pd.DataFrame(all_rows)
            
        except Exception as e:
            logger.warning(f"pandas failed completely: {e}, using manual parsing")
            # Fallback to pure manual parsing
            manual_rows = []
            for line in data_lines:
                try:
                    manual_row = self._parse_line_to_dict(line, header_line.split())
                    if manual_row:
                        manual_rows.append(manual_row)
                except:
                    continue
            
            return pd.DataFrame(manual_rows)

    def _parse_line_to_dict(self, line: str, header: list) -> dict:
        """
        Parse a single ASCII line to dictionary. Used for recovery.
        """
        fields = []
        current_field = ""
        in_quotes = False
        
        i = 0
        while i < len(line):
            char = line[i]
            
            if char == '"':
                in_quotes = not in_quotes
                current_field += char
            elif char == ' ' and not in_quotes:
                if current_field:
                    fields.append(current_field)
                    current_field = ""
                # Skip multiple spaces
                while i + 1 < len(line) and line[i + 1] == ' ':
                    i += 1
            else:
                current_field += char
            i += 1
        
        if current_field:
            fields.append(current_field)
        
        # Adjust field count to match header
        while len(fields) < len(header):
            fields.append('')
        if len(fields) > len(header):
            fields = fields[:len(header)]
        
        # Clean quotes
        cleaned_fields = []
        for field in fields:
            if field.startswith('"') and field.endswith('"') and len(field) > 1:
                cleaned_fields.append(field[1:-1])
            else:
                cleaned_fields.append(field)
        
        return dict(zip(header, cleaned_fields))

    def _save_ascii_with_backup(self, df: pd.DataFrame, filepath: str) -> None:
        """
        Save ASCII file with backup and validation. Drop-in replacement for df.to_csv().
        """
        # Create backup with automatic cleanup (keep only 5 most recent)
        backup_path = self._create_backup_with_limit(filepath, max_backups=5)
        logger.info(f"Created backup: {backup_path}")

        # Save using your existing approach but with better formatting
        try:
            with open(filepath, 'w') as f:
                # Write header
                f.write(" ".join(self.ascii_columns) + "\n")
                
                # Write data rows
                for _, row in df.iterrows():
                    row_values = []
                    for col in self.ascii_columns:
                        val = str(row.get(col, '')).strip()
                        # Quote fields with spaces (your existing logic)
                        if val and (' ' in val or col in ['Name', 'Discovery_UTC', 'Notice_date', 'Host_info']):
                            if not (val.startswith('"') and val.endswith('"')):
                                val = f'"{val}"'
                        row_values.append(val)
                    
                    f.write(" ".join(row_values) + "\n")
            
            logger.info(f"ASCII file saved with {len(df)} entries")
            
        except Exception as e:
            logger.error(f"Error saving ASCII file: {e}", exc_info=True)
            raise

    def _update_ascii_database(self, processed_data: Dict[str, Any]) -> None:
        """
        Update the ASCII database with processed circular data.
        Enhanced multi-facility design: one row per GRB with facility tracking.
        """
        # Early return checks
        event_name = processed_data.get('event_name')
        facility = processed_data.get('facility')
        trigger_num = processed_data.get('trigger_num')
        
        # Skip ASCII update if essential data is missing
        if not event_name or not facility:
            logger.warning("Skipping ASCII update due to missing essential data (event_name or facility)")
            return
            
        # Skip certain facilities without trigger numbers unless they have coordinates
        has_coordinates = (processed_data.get('ra') is not None and 
                        processed_data.get('dec') is not None)
        is_swift_facility = facility and 'Swift' in facility
        
        if is_swift_facility and not trigger_num and not has_coordinates:
            logger.warning(f"Skipping ASCII update for Swift circular without trigger number or coordinates")
            return
                
        with self.file_lock:
            try:
                # Load existing ASCII file
                os.makedirs(os.path.dirname(os.path.abspath(self.output_ascii)), exist_ok=True)
                
                # Load existing data with backward compatibility
                df = self._load_ascii_with_recovery(self.output_ascii)
                df = self._ensure_ascii_columns(df)  # Add new columns if missing
                
                # CRITICAL FIX: Ensure all required columns exist before accessing them
                required_columns = ['Best_Facility', 'All_Facilities', 'Primary_Facility', 'Last_Update', 'GCN_ID']
                for col in required_columns:
                    if col not in df.columns:
                        logger.info(f"Adding missing column: {col}")
                        if col == 'Best_Facility' and 'Facility' in df.columns:
                            df[col] = df['Facility']
                        elif col == 'Primary_Facility' and 'Facility' in df.columns:
                            df[col] = df['Facility']
                        elif col == 'All_Facilities' and 'Facility' in df.columns:
                            df[col] = df['Facility']
                        elif col == 'Last_Update' and 'Notice_date' in df.columns:
                            df[col] = df['Notice_date']
                        else:
                            df[col] = ''
                
                # Get circular info
                circular_id = processed_data.get('circular_id')
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Find existing entry for the same GRB event
                existing_idx = None
                
                # Search by event name (most reliable for same GRB from different facilities)
                for idx, row in df.iterrows():
                    row_event = str(row.get('Name', '')).strip().strip('"')
                    
                    if row_event == event_name:
                        existing_idx = idx
                        logger.info(f"Found existing entry for {event_name} at index {existing_idx}")
                        break
                
                if existing_idx is not None:
                    # UPDATE EXISTING ENTRY (same GRB, different facility)
                    current_best = str(df.at[existing_idx, 'Best_Facility']).strip()
                    current_all = str(df.at[existing_idx, 'All_Facilities']).strip()
                    current_gcn_id = str(df.at[existing_idx, 'GCN_ID']).strip()
                    
                    should_update_position = self._should_update_position(current_best, facility)
                    
                    logger.info(f"Updating existing entry for {event_name}: current_best={current_best}, new_facility={facility}, should_update={should_update_position}")
                    
                    # Update facility tracking
                    if facility not in current_all:
                        updated_all = f"{current_all},{facility}" if current_all else facility
                        df.at[existing_idx, 'All_Facilities'] = updated_all
                    
                    # Update best facility if new one has higher priority
                    if should_update_position:
                        df.at[existing_idx, 'Best_Facility'] = facility
                        logger.info(f"Updated best facility from {current_best} to {facility}")
                        
                        # Update position if coordinates available
                        if has_coordinates:
                            df.at[existing_idx, 'RA'] = f"{processed_data['ra']:.5f}"
                            df.at[existing_idx, 'DEC'] = f"{processed_data['dec']:.5f}"
                            
                            # Convert error to degrees if needed
                            error_in_degrees = processed_data.get('error', 0)
                            if processed_data.get('error_unit') == 'arcsec':
                                error_in_degrees = error_in_degrees / 3600.0
                            elif processed_data.get('error_unit') == 'arcmin':
                                error_in_degrees = error_in_degrees / 60.0
                            
                            df.at[existing_idx, 'Error'] = f"{error_in_degrees:.6f}" if error_in_degrees is not None else "N/A"
                            df.at[existing_idx, 'Facility'] = facility  # Keep main facility field updated
                            
                            if trigger_num:
                                df.at[existing_idx, 'Trigger_num'] = trigger_num
                    
                    # Always update GCN_ID list and Last_Update
                    updated_gcn_id = f"{current_gcn_id},{circular_id}" if current_gcn_id else str(circular_id)
                    df.at[existing_idx, 'GCN_ID'] = updated_gcn_id
                    df.at[existing_idx, 'Last_Update'] = current_time
                    
                    # Update other fields if available
                    if processed_data.get('redshift') is not None:
                        df.at[existing_idx, 'Redshift'] = f"{processed_data['redshift']:.4f}"
                    if processed_data.get('host_info'):
                        df.at[existing_idx, 'Host_info'] = f'"{processed_data["host_info"]}"'
                    
                    logger.info(f"Updated existing entry for {event_name}")
                    
                else:
                    # ADD NEW ENTRY
                    if has_coordinates and trigger_num:
                        # Convert error to degrees if needed
                        error_in_degrees = processed_data.get('error', 0)
                        if processed_data.get('error_unit') == 'arcsec':
                            error_in_degrees = error_in_degrees / 3600.0
                        elif processed_data.get('error_unit') == 'arcmin':
                            error_in_degrees = error_in_degrees / 60.0
                        
                        # Format discovery time
                        discovery_time = processed_data.get('discovery_utc', '')
                        if not discovery_time and processed_data.get('discovery_date') and processed_data.get('discovery_time'):
                            discovery_time = f"{processed_data['discovery_date']} {processed_data['discovery_time']}"
                        
                        new_row = {
                            'GCN_ID': str(circular_id),
                            'Name': f'"{event_name}"',
                            'RA': f"{processed_data['ra']:.5f}",
                            'DEC': f"{processed_data['dec']:.5f}",
                            'Error': f"{error_in_degrees:.6f}",
                            'Discovery_UTC': discovery_time,
                            'Primary_Facility': facility,       # First detector
                            'Best_Facility': facility,          # Initially same as primary
                            'All_Facilities': facility,         # Start with just this facility
                            'Facility': facility,               # Backward compatibility
                            'Trigger_num': trigger_num,         # From primary facility
                            'Notice_date': current_time,        # First circular time
                            'Last_Update': current_time,        # Same as notice_date initially
                            'Redshift': f"{processed_data['redshift']:.4f}" if processed_data.get('redshift') is not None else '',
                            'Host_info': f'"{processed_data["host_info"]}"' if processed_data.get('host_info') else '',
                            'thread_ts': ''
                        }
                        
                        df = self._safe_concat_dataframes(df, pd.DataFrame([new_row]), ignore_index=True)
                        logger.info(f"Added new entry to ASCII database: {new_row['GCN_ID']}")
                    else:
                        logger.info(f"Circular {circular_id} doesn't contain sufficient position data for new entry")
                
                # Save updated DataFrame
                self._save_ascii_with_backup(df, self.output_ascii)
                    
                logger.info(f"ASCII database saved successfully")
                        
            except Exception as e:
                logger.error(f"Error updating ASCII database: {e}", exc_info=True)

    def _ensure_ascii_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure ASCII DataFrame has all required columns for backward compatibility.
        Gradually migrate old format to new multi-facility format.
        """
        try:
            # Check if this is old format (has 'Facility' instead of 'Primary_Facility')
            if 'Facility' in df.columns and 'Primary_Facility' not in df.columns:
                logger.info("Migrating ASCII format from old to new multi-facility structure")
                
                # Rename and add columns for gradual migration
                if 'Facility' in df.columns:
                    df['Primary_Facility'] = df['Facility']
                    df['Best_Facility'] = df['Facility']
                    df['All_Facilities'] = df['Facility']
                    # Keep old 'Facility' column temporarily for compatibility
                
                # Add new columns if missing
                if 'Last_Update' not in df.columns:
                    df['Last_Update'] = df.get('Notice_date', '')
            
            # Ensure all required columns exist
            for col in self.ascii_columns:
                if col not in df.columns:
                    df[col] = ''
                    logger.debug(f"Added missing column: {col}")
            
            # Reorder columns to match expected format
            df = df[self.ascii_columns]
            
            return df
            
        except Exception as e:
            logger.error(f"Error ensuring ASCII columns: {e}")
            return df

    def _get_facility_priority(self, facility: str) -> int:
        """
        Get facility priority for position accuracy.
        Higher number = higher priority/accuracy.
        """
        if not facility:
            return 0
            
        facility_priorities = {
            'SwiftXRT': 10,      # Highest - arcsec precision
            'Swift-XRT': 10,
            'SwiftBAT': 7,       # Good - arcmin precision  
            'Swift-BAT': 7,
            'Swift': 6,          # General Swift
            'FermiLAT': 5,       # Degree precision but good
            'Fermi-LAT': 5,
            'SVOM': 4,           # Arcmin precision
            'FermiGBM': 3,       # Detection mainly
            'Fermi-GBM': 3,
            'Fermi': 3,
            'GECAM': 2,          # Detection confirmation
            'CALET': 2,          # Detection confirmation
        }
        
        return facility_priorities.get(facility, 1)

    def _should_update_position(self, current_facility: str, new_facility: str) -> bool:
        """
        Determine if position should be updated based on facility priority.
        Only update if new facility has higher priority (more accurate).
        """
        current_priority = self._get_facility_priority(current_facility)
        new_priority = self._get_facility_priority(new_facility)
        
        return new_priority > current_priority

    def get_facility_summary(self) -> None:
        """
        Display a summary of facilities and their tracking for debugging.
        Useful for understanding the multi-facility system.
        """
        try:
            if not os.path.exists(self.output_ascii):
                logger.info("No ASCII file found")
                return
                
            df = self._load_ascii_with_recovery(self.output_ascii)
            df = self._ensure_ascii_columns(df)
            
            logger.info("=== FACILITY TRACKING SUMMARY ===")
            
            for _, row in df.iterrows():
                name = str(row.get('Name', '')).strip().strip('"')
                primary = str(row.get('Primary_Facility', '')).strip()
                best = str(row.get('Best_Facility', '')).strip() 
                all_fac = str(row.get('All_Facilities', '')).strip()
                gcn_id = str(row.get('GCN_ID', '')).strip()
                
                logger.info(f"{name}:")
                logger.info(f"  Primary: {primary} | Best: {best} | All: {all_fac}")
                logger.info(f"  Circulars: {gcn_id}")
                logger.info(f"  Position: RA={row.get('RA', '')}, DEC={row.get('DEC', '')}, Error={row.get('Error', '')}")
                logger.info("")
                
        except Exception as e:
            logger.error(f"Error generating facility summary: {e}")
        
    def _remove_false_trigger_from_ascii(self, processed_data: Dict[str, Any]) -> None:
        """
        Remove a false trigger from the ASCII database with improved error handling.
        FIXED: Better column handling and safe row processing.
        """
        if not os.path.exists(self.output_ascii):
            logger.warning("ASCII file does not exist, skipping false trigger removal")
            return
            
        if not processed_data.get('facility') or not processed_data.get('trigger_num'):
            logger.warning("Missing facility or trigger_num in processed data, skipping ASCII update")
            return
            
        with self.file_lock:
            try:
                # Load existing ASCII
                df = self._load_ascii_with_recovery(self.output_ascii)
                df = self._ensure_ascii_columns(df)  # Ensure all columns exist
                logger.info(f"pandas loaded {len(df)}/{len(df)} entries")
                
                # Get normalized facility name for the processed data
                facility = processed_data['facility']
                trigger_num = str(processed_data['trigger_num'])
                normalized_facility = self._normalize_facility_name(facility)
                
                logger.info(f"Processing false trigger for removal: {facility} trigger {trigger_num}")
                logger.info(f"Looking for false trigger to remove: facility='{facility}' (normalized='{normalized_facility}'), trigger='{trigger_num}'")
                
                # Find rows to remove - any with matching normalized facility and trigger
                rows_to_remove = []
                gcn_ids_to_remove = []
                
                # Use the correct column name based on what exists
                facility_columns = ['Primary_Facility', 'Facility', 'Best_Facility']
                facility_column = None
                for col in facility_columns:
                    if col in df.columns:
                        facility_column = col
                        break
                
                if not facility_column:
                    logger.warning("No suitable facility column found for false trigger removal")
                    return
                
                for idx, row in df.iterrows():
                    try:
                        row_facility = str(row.get(facility_column, '')).strip()
                        row_trigger = str(row.get('Trigger_num', '')).strip()
                        row_gcn_id = str(row.get('GCN_ID', '')).strip()
                        
                        # Normalize the row's facility
                        normalized_row_facility = self._normalize_facility_name(row_facility)
                        
                        logger.debug(f"Checking row {idx}: facility='{row_facility}' (normalized='{normalized_row_facility}'), trigger='{row_trigger}', gcn_id='{row_gcn_id}'")
                        
                        # Check if normalized facilities and trigger numbers match
                        if (normalized_row_facility == normalized_facility and 
                            row_trigger == trigger_num):
                            rows_to_remove.append(idx)
                            gcn_ids_to_remove.append(row_gcn_id)
                            logger.info(f"Found matching entry to remove: {row_gcn_id} (original facility: {row_facility})")
                    
                    except Exception as row_error:
                        logger.debug(f"Error processing row {idx}: {row_error}")
                        continue
                
                if rows_to_remove:
                    # Remove the false triggers
                    df = df.drop(rows_to_remove)
                    
                    # Save updated dataframe
                    self._save_ascii_with_backup(df, self.output_ascii)
                    
                    # Log the removal with original GCN_IDs for reference
                    gcn_ids_str = ", ".join(gcn_ids_to_remove)
                    logger.info(f"Successfully removed {len(rows_to_remove)} false trigger entries matching {normalized_facility}_{trigger_num} from ASCII database. GCN_IDs: {gcn_ids_str}")
                    
                    # Log summary of remaining entries for this facility
                    try:
                        remaining_count = len(df[df[facility_column].apply(lambda x: self._normalize_facility_name(str(x)) == normalized_facility)])
                        logger.info(f"Remaining entries for {normalized_facility}: {remaining_count}")
                    except Exception:
                        logger.debug("Could not count remaining entries")
                    
                else:
                    logger.warning(f"No entries found to remove for false trigger {normalized_facility}_{trigger_num}")
                    
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
            import shutil
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
            import glob
            
            # Find all backup files for this filepath
            backup_pattern = f"{filepath}.backup.*"
            backup_files = glob.glob(backup_pattern)
            
            if len(backup_files) <= max_backups:
                logger.debug(f"Only {len(backup_files)} backup files, no cleanup needed")
                return
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            
            # Keep only the most recent max_backups files
            files_to_keep = backup_files[:max_backups]
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
            
            logger.info(f"Cleaned up {removed_count} old backup files, keeping {len(files_to_keep)} most recent")
                
        except Exception as e:
            logger.error(f"Error during backup cleanup: {e}")

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