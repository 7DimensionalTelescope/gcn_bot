#!/usr/bin/env python3
"""
GCN Circular Handler
==================
A module for processing GCN (Gamma-ray Coordinates Network) circulars and updating relevant databases.

This module processes GCN circulars to extract key information about GRB 
(Gamma-Ray Burst) events, and maintains both CSV and ASCII databases of event information.

Author:         YoungPyo Hong
Created:        2025-03-27
Version:        2.0.0
License:        MIT
"""

import pandas as pd
import re
from datetime import datetime
import json
import os
import logging
import threading
from typing import Dict, Optional, Tuple, List, Any
import numpy as np
import time
import shutil
import glob

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
                 ascii_max_events: int = 10,
                 client_id: str = '',
                 client_secret: str = '') -> None:
        """
        Initialize the GCN Circular Handler.
        
        Args:
            output_csv (str): Path to the CSV database file
            output_ascii (str): Path to the ASCII database file
            ascii_max_events (int, optional): Maximum number of events to keep in the ASCII file
            client_id (str, optional): Kafka client ID for monitoring circulars
            client_secret (str, optional): Kafka client secret for monitoring circulars
        """
        self.output_csv = output_csv
        self.output_ascii = output_ascii
        self.ascii_max_events = ascii_max_events
        self.file_lock = threading.Lock()
        self.consumer = None
        
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
        
        # CSV column definitions
        self.csv_columns = [
            'circular_id', 'event_name', 'subject', 'facility', 'trigger_num',
            'ra', 'dec', 'error', 'error_unit', 'redshift', 'redshift_error',
            'host_info', 'false_trigger', 'created_on', 'processed_on'
        ]
        
        # ASCII file columns
        self.ascii_columns = [
            'GCN_ID', 'Name', 'RA', 'DEC', 'Error', 'Discovery_UTC', 
            'Primary_Facility', 'Best_Facility', 'All_Facilities', 'Trigger_num', 
            'Notice_date', 'Last_Update', 'Redshift', 'Host_info', 'thread_ts'
        ]
        
        # Facility priority mapping for position accuracy
        self.facility_priorities = {
            'SwiftXRT': 10, 'Swift-XRT': 10,           # Highest - arcsec precision
            'SwiftBAT': 7, 'Swift-BAT': 7,             # Good - arcmin precision
            'Swift': 6,                                # General Swift
            'FermiLAT': 5, 'Fermi-LAT': 5,             # Degree precision but good
            'SVOM': 4,                                 # Arcmin precision
            'FermiGBM': 3, 'Fermi-GBM': 3, 'Fermi': 3, # Detection mainly
            'GECAM': 2, 'CALET': 2,                    # Detection confirmation
        }

    def process_circular(self, circular_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a GCN circular and extract all relevant information.
        Consolidated extraction logic for improved efficiency.
        """
        try:
            subject = circular_data.get('subject', '')
            body = circular_data.get('body', '')
            circular_id = circular_data.get('circularId')
            created_on = circular_data.get('createdOn')
            
            logger.info(f"Processing circular {circular_id}: {subject}")
            
            # Initialize result dictionary
            result = {
                'circular_id': circular_id,
                'subject': subject,
                'created_on': created_on,
                'processed_on': datetime.now().timestamp() * 1000,
                'event_name': None,
                'facility': None,
                'trigger_num': None,
                'ra': None,
                'dec': None,
                'error': None,
                'error_unit': None,
                'redshift': None,
                'redshift_error': None,
                'host_info': None,
                'false_trigger': False
            }
            
            # Extract event name (GRB/EP)
            event_match = re.search(r'(?:GRB|grb)\s*(\d{6}[A-Za-z])', subject, re.IGNORECASE)
            if event_match:
                result['event_name'] = f"GRB {event_match.group(1)}"
            else:
                event_match = re.search(r'(?:EP)\s*(\d{6}[A-Za-z])', subject, re.IGNORECASE)
                if event_match:
                    result['event_name'] = f"EP {event_match.group(1)}"
            
            # Extract facility
            combined_text = subject + " " + body[:500]  # Check subject and first part of body
            facility_patterns = [
                (r'Swift[\s/\-]?XRT', 'SwiftXRT'),
                (r'Swift[\s/\-]?BAT', 'SwiftBAT'),
                (r'Swift[\s/\-]?UVOT', 'SwiftUVOT'),
                (r'Swift', 'Swift'),
                (r'Fermi[\s/\-]?GBM', 'FermiGBM'),
                (r'Fermi[\s/\-]?LAT', 'FermiLAT'),
                (r'Fermi', 'Fermi'),
                (r'Einstein[\s\-]?Probe|EP[\s\-]?[WF]XT', 'EinsteinProbe'),
                (r'GECAM', 'GECAM'),
                (r'SVOM|ECLAIRs', 'SVOM'),
                (r'CALET', 'CALET'),
            ]
            
            for pattern, facility_name in facility_patterns:
                if re.search(pattern, combined_text, re.IGNORECASE):
                    result['facility'] = facility_name
                    break
            
            # Extract trigger number based on facility
            if result['facility']:
                trigger_patterns = []
                if 'Swift' in result['facility']:
                    trigger_patterns = [
                        r'\(trigger\s*=\s*(\d+)\)',
                        r'trigger\s*[:#]?\s*(\d+)',
                        r'BAT\s+trigger\s*#?(\d+)',
                    ]
                elif 'Fermi' in result['facility']:
                    trigger_patterns = [
                        r'trigger\s+(\d+)/?(?:\d+)?',
                        r'GBM\s+trigger\s+(\d+)',
                    ]
                else:
                    trigger_patterns = [r'trigger\s*[:#]?\s*(\d+)']
                
                for pattern in trigger_patterns:
                    match = re.search(pattern, combined_text, re.IGNORECASE)
                    if match:
                        result['trigger_num'] = match.group(1)
                        break
            
            # Check for false trigger
            false_patterns = [
                r"not\s+(?:due\s+to\s+)?(?:a\s+)?GRB",
                r"false\s+(?:positive|trigger|alarm)",
                r"not\s+a\s+(?:real\s+)?(?:burst|GRB)",
                r"(?:likely\s+)?(?:due\s+to|caused\s+by)\s+(?:local\s+particles|SAA|background)",
                r"retraction",
            ]
            
            for pattern in false_patterns:
                if re.search(pattern, body, re.IGNORECASE):
                    result['false_trigger'] = True
                    logger.info("Detected false trigger")
                    break
            
            # Extract coordinates based on facility
            coord_patterns = []
            if result['facility'] and 'Swift' in result['facility'] and 'XRT' in result['facility']:
                coord_patterns = [
                    r"Enhanced Swift-XRT position.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA,\s*Dec[:\s]*=?\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ]
            elif result['facility'] and 'Swift' in result['facility'] and 'BAT' in result['facility']:
                coord_patterns = [
                    r"BAT.*?RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcmin]+)",
                ]
            
            # Add general patterns as fallback
            coord_patterns.extend([
                r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?(?:uncertainty|error)\s+(?:of\s+)?([\d.]+)\s*([\"\'arcsecmindeg]+)?",
                r"RA[:\s=]*([\d.]+)[,\s]+Dec[:\s=]*([-+]?[\d.]+)",
            ])
            
            for pattern in coord_patterns:
                match = re.search(pattern, body, re.IGNORECASE | re.DOTALL)
                if match:
                    groups = match.groups()
                    try:
                        result['ra'] = float(groups[0])
                        result['dec'] = float(groups[1])
                        if len(groups) > 2 and groups[2]:
                            result['error'] = float(groups[2])
                            result['error_unit'] = groups[3] if len(groups) > 3 else 'arcsec'
                        logger.info(f"Extracted coordinates: RA={result['ra']}, Dec={result['dec']}")
                        break
                    except (ValueError, IndexError):
                        continue
            
            # Extract redshift
            redshift_patterns = [
                r"redshift\s+(?:of\s+)?z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                r"at\s+z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                r"z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
            ]
            
            for pattern in redshift_patterns:
                match = re.search(pattern, body, re.IGNORECASE)
                if match:
                    try:
                        result['redshift'] = float(match.group(1))
                        if match.group(2):
                            result['redshift_error'] = float(match.group(2))
                        logger.info(f"Extracted redshift: z={result['redshift']}")
                        break
                    except (ValueError, IndexError):
                        continue
            
            # Extract host information
            host_match = re.search(r"host\s+galaxy[^.]+", body, re.IGNORECASE)
            if host_match:
                result['host_info'] = host_match.group(0).strip()
            
            logger.info(f"Successfully processed circular {circular_id}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing circular: {e}", exc_info=True)
            return {
                'circular_id': circular_data.get('circularId'),
                'subject': circular_data.get('subject', ''),
                'created_on': circular_data.get('createdOn'),
                'processed_on': datetime.now().timestamp() * 1000
            }

    def _check_false_trigger(self, body: str, facility: Optional[str]) -> bool:
        """
        Check if the circular is reporting a false trigger.
        Integrated into process_circular for efficiency.
        """
        # This method is now integrated into process_circular
        # Kept for backward compatibility if needed
        false_patterns = [
            r"not\s+(?:due\s+to\s+)?(?:a\s+)?GRB",
            r"false\s+(?:positive|trigger|alarm)",
            r"not\s+a\s+(?:real\s+)?(?:burst|GRB)",
        ]
        
        for pattern in false_patterns:
            if re.search(pattern, body, re.IGNORECASE):
                return True
        return False

    def _update_databases(self, processed_data: Dict[str, Any]) -> None:
        """
        Update both CSV and ASCII databases with processed circular data.
        Unified database update logic for improved efficiency.
        """
        with self.file_lock:
            try:
                # Update CSV database
                self._update_csv(processed_data)
                
                # Update ASCII database (skip if essential data is missing)
                has_coordinates = (processed_data.get('ra') is not None and 
                                   processed_data.get('dec') is not None)
                has_trigger = processed_data.get('trigger_num') is not None
                has_actionable_data = has_coordinates and has_trigger

                if processed_data.get('event_name') and processed_data.get('facility') and has_actionable_data:
                    if processed_data.get('false_trigger'):
                        self._remove_false_trigger_from_ascii(processed_data)
                    else:
                        self._update_ascii(processed_data)
                
            except Exception as e:
                logger.error(f"Error updating databases: {e}", exc_info=True)

    def _update_csv(self, data: Dict[str, Any]) -> None:
        """
        Update CSV database with inline file handling.
        """
        try:
            # Load or create dataframe
            if os.path.exists(self.output_csv):
                df = pd.read_csv(self.output_csv)
            else:
                df = pd.DataFrame(columns=self.csv_columns)
            
            # Update or add entry
            if 'circular_id' in df.columns and data['circular_id'] in df['circular_id'].values:
                # Update existing
                idx = df[df['circular_id'] == data['circular_id']].index[0]
                for key, value in data.items():
                    if key in df.columns and value is not None:
                        df.at[idx, key] = str(value) if key == 'trigger_num' else value
            else:
                # Add new
                new_row = {col: data.get(col) for col in self.csv_columns}
                if new_row.get('trigger_num') is not None:
                    new_row['trigger_num'] = str(new_row['trigger_num'])
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            df.to_csv(self.output_csv, index=False)
            logger.info(f"CSV database updated with circular ID {data['circular_id']}")
            
        except Exception as e:
            logger.error(f"Error updating CSV: {e}", exc_info=True)

    def _update_ascii(self, data: Dict[str, Any]) -> None:
        """
        Update ASCII database with inline file handling and backup.
        """
        try:
            # Load existing ASCII
            if os.path.exists(self.output_ascii):
                df = pd.read_csv(self.output_ascii, sep=' ', header=0, dtype=str)
            else:
                df = pd.DataFrame(columns=self.ascii_columns)
            
            # Convert error to degrees if needed
            error_deg = data.get('error')
            if error_deg is not None and data.get('error_unit'):
                if 'arcsec' in data['error_unit']:
                    error_deg = error_deg / 3600.0
                elif 'arcmin' in data['error_unit']:
                    error_deg = error_deg / 60.0
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            event_name = data['event_name']
            facility = data['facility']
            
            # Find existing entry for the same event
            existing_idx = None
            for idx, row in df.iterrows():
                if str(row.get('Name', '')).strip().strip('"') == event_name:
                    existing_idx = idx
                    break
            
            if existing_idx is not None:
                # Update existing entry
                current_best = str(df.at[existing_idx, 'Best_Facility'])
                current_all = str(df.at[existing_idx, 'All_Facilities'])
                current_error_str = str(df.at[existing_idx, 'Error'])
                current_error = None
                if current_error_str and current_error_str != 'N/A':
                    current_error = float(current_error_str)
                
                # Update facility tracking
                if facility not in current_all:
                    df.at[existing_idx, 'All_Facilities'] = f"{current_all},{facility}" if current_all else facility
                
                # Update if new facility should become Best_Facility
                if self._compare_facilities(current_best, current_error, facility, error_deg):
                    df.at[existing_idx, 'Best_Facility'] = facility
                    
                    # Update position data if new facility has coordinates
                    if data.get('ra') is not None and data.get('dec') is not None:
                        df.at[existing_idx, 'RA'] = f"{data['ra']:.3f}"
                        df.at[existing_idx, 'DEC'] = f"{data['dec']:.3f}"
                        df.at[existing_idx, 'Error'] = f"{error_deg:.3f}" if error_deg else "N/A"
                    
                    # Update trigger number if available
                    if data.get('trigger_num'):
                        df.at[existing_idx, 'Trigger_num'] = data['trigger_num']
                
                # Update GCN_ID and timestamp
                current_gcn = str(df.at[existing_idx, 'GCN_ID']).strip()
                circular_id = str(data['circular_id'])
                if circular_id not in current_gcn.split(','):
                    df.at[existing_idx, 'GCN_ID'] = f"{current_gcn},{circular_id}" if current_gcn else circular_id
                df.at[existing_idx, 'Last_Update'] = current_time
                
                # Update optional fields
                if data.get('redshift') is not None:
                    df.at[existing_idx, 'Redshift'] = f"{data['redshift']:.1f}"
                if data.get('host_info'):
                    df.at[existing_idx, 'Host_info'] = f'"{data["host_info"]}"'
                    
            else:
                # Add new entry if coordinates and trigger exist
                if data.get('ra') is not None and data.get('dec') is not None and data.get('trigger_num'):
                    new_row = {
                        'GCN_ID': str(data['circular_id']),
                        'Name': f'"{event_name}"',
                        'RA': f"{data['ra']:.3f}",
                        'DEC': f"{data['dec']:.3f}",
                        'Error': f"{error_deg:.3f}" if error_deg else '',
                        'Discovery_UTC': '',  # Preserve existing, set by notice handler
                        'Primary_Facility': '',  # Preserve existing, set by notice handler
                        'Best_Facility': facility,
                        'All_Facilities': facility,
                        'Trigger_num': data['trigger_num'],
                        'Notice_date': current_time,
                        'Last_Update': current_time,
                        'Redshift': f"{data['redshift']:.1f}" if data.get('redshift') is not None else '',
                        'Host_info': f'"{data["host_info"]}"' if data.get('host_info') else '',
                        'thread_ts': ''  # Preserve existing, set by bot
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            # Apply max events limit
            if len(df) > self.ascii_max_events:
                df = df.sort_values('Last_Update', ascending=False).head(self.ascii_max_events)
            
            # Create backup and save
            if os.path.exists(self.output_ascii):
                backup_path = f"{self.output_ascii}.backup.{int(time.time())}"
                shutil.copy2(self.output_ascii, backup_path)
                
                # Keep only 5 most recent backups
                backups = sorted(glob.glob(f"{self.output_ascii}.backup.*"), 
                               key=lambda x: os.path.getmtime(x), reverse=True)
                if len(backups) > 5:
                    # Sort by modification time, oldest first
                    backups.sort(key=os.path.getmtime)
                    # Remove oldest backups
                    for old_backup in backups[:-5]:
                        os.remove(old_backup)
            
            # Write ASCII file
            with open(self.output_ascii, 'w') as f:
                f.write(" ".join(self.ascii_columns) + "\n")
                for _, row in df.iterrows():
                    row_values = []
                    for col in self.ascii_columns:
                        val = str(row.get(col, '')).strip()
                        if val and (' ' in val or col in ['Name', 'Discovery_UTC', 'Notice_date', 'Host_info']):
                            if not (val.startswith('"') and val.endswith('"')):
                                val = f'"{val}"'
                        row_values.append(val)
                    f.write(" ".join(row_values) + "\n")
            
            logger.info(f"ASCII database updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating ASCII: {e}", exc_info=True)

    def _remove_false_trigger_from_ascii(self, data: Dict[str, Any]) -> None:
        """
        Remove false trigger entries from ASCII database.
        """
        if not os.path.exists(self.output_ascii) or not data.get('facility') or not data.get('trigger_num'):
            return
        
        try:
            df = pd.read_csv(self.output_ascii, sep=' ', header=0, dtype=str)
            
            # Normalize facility names for matching
            facility = data['facility']
            trigger = str(data['trigger_num'])
            
            # Find rows to remove
            mask = (df['Trigger_num'] == trigger) & (df['Facility'].str.contains(facility.split('Swift')[-1] if 'Swift' in facility else facility))
            
            if mask.any():
                df = df[~mask]
                
                # Save updated dataframe
                with open(self.output_ascii, 'w') as f:
                    f.write(" ".join(self.ascii_columns) + "\n")
                    for _, row in df.iterrows():
                        row_values = [str(row.get(col, '')) for col in self.ascii_columns]
                        f.write(" ".join(row_values) + "\n")
                
                logger.info(f"Removed false trigger {facility}_{trigger} from ASCII database")
                
        except Exception as e:
            logger.error(f"Error removing false trigger: {e}", exc_info=True)

    def _get_facility_priority(self, facility: str) -> int:
        """
        Get facility priority for position accuracy.
        """
        return self.facility_priorities.get(facility, 1) if facility else 0
    
    def _compare_facilities(self, current_facility: str, current_error: Optional[float], 
                        new_facility: str, new_error: Optional[float]) -> bool:
        """
        Compare facilities to determine if new facility should become Best_Facility.
        Returns True if new facility should be used.
        """
        current_priority = self._get_facility_priority(current_facility)
        new_priority = self._get_facility_priority(new_facility)
        
        # Higher priority wins
        if new_priority > current_priority:
            return True
        elif new_priority < current_priority:
            return False
        
        # Same priority: compare error radius (smaller is better)
        if current_error is not None and new_error is not None:
            return new_error < current_error
        elif new_error is not None and current_error is None:
            return True
        
        return False
    
    def process_circular_from_json(self, json_str: str) -> None:
        """
        Process a GCN circular from JSON string and update databases.
        """
        try:
            circular_data = json.loads(json_str)
            processed_data = self.process_circular(circular_data)
            self._update_databases(processed_data)
            logger.info(f"Successfully processed circular {processed_data['circular_id']}")
        except Exception as e:
            logger.error(f"Error processing circular: {e}", exc_info=True)

    def monitor_circulars(self, timeout: int = 0) -> None:
        """
        Monitor for new GCN circulars via Kafka and process them.
        """
        if not self.consumer:
            logger.error("No Kafka consumer initialized")
            return
        
        logger.info("Starting GCN circular monitoring...")
        start_time = time.time()
        
        try:
            while True:
                if timeout > 0 and (time.time() - start_time) > timeout:
                    logger.info(f"Monitoring timeout reached ({timeout}s)")
                    break
                
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                self.process_circular_from_json(msg.value().decode('utf-8'))
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error in monitoring: {e}", exc_info=True)
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Monitoring stopped")


def main():
    """Main function to run the GCN Circular Handler."""
    import argparse
    
    parser = argparse.ArgumentParser(description='GCN Circular Handler')
    parser.add_argument('--file', help='Single JSON file to process')
    parser.add_argument('--monitor', action='store_true', help='Monitor for new circulars')
    parser.add_argument('--timeout', type=int, default=0, help='Monitoring timeout in seconds')
    
    args = parser.parse_args()
    
    # Try to import configuration
    try:
        from config import OUTPUT_CIRCULAR_CSV, OUTPUT_ASCII, ASCII_MAX_EVENTS, GCN_ID, GCN_SECRET
    except ImportError:
        logger.warning("Config file not found, using default values")
        OUTPUT_CIRCULAR_CSV = 'gcn_circulars.csv'
        OUTPUT_ASCII = 'grb_targets.ascii'
        ASCII_MAX_EVENTS = 10
        GCN_ID = ''
        GCN_SECRET = ''
    
    handler = GCNCircularHandler(
        output_csv=OUTPUT_CIRCULAR_CSV,
        output_ascii=OUTPUT_ASCII,
        ascii_max_events=ASCII_MAX_EVENTS,
        client_id=GCN_ID,
        client_secret=GCN_SECRET
    )
    
    if args.file:
        with open(args.file, 'r') as f:
            handler.process_circular_from_json(f.read())
    elif args.monitor:
        handler.monitor_circulars(timeout=args.timeout)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()