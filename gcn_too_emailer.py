import os
import json
import logging
import ssl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

class GCNToOEmailer:
    """
    Send ToO email requests for observable IceCube neutrino events.
    
    This class checks if IceCube neutrino events are immediately observable
    at the 7DT telescope and sends email requests for immediate observation.
    """
    
    def __init__(self, 
                email_from: str, 
                email_to: List[str],
                email_password: str,
                smtp_server: str = "smtp.gmail.com",
                smtp_port: int = 465,
                min_altitude: int = 30,
                min_moon_sep: int = 30):
        """
        Initialize the GCN ToO Email Requester.
        
        Args:
            email_from: Sender email address
            email_to: List of recipient email addresses
            email_password: Email account password or app password
            smtp_server: SMTP server hostname
            smtp_port: SMTP server port
            min_altitude: Minimum altitude for observation (degrees)
            min_moon_sep: Minimum moon separation (degrees)
        """
        self.email_from = email_from
        self.email_to = email_to
        self.email_password = email_password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        
        # Observability criteria
        self.min_altitude = min_altitude
        self.min_moon_sep = min_moon_sep
        
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        
        # IceCube facility sources of interest - more likely to trigger ToOs
        self.icecube_facilities = [
            'AMON', 
            'IceCubeCASCADE', 
            'HAWC', 
            'IceCubeBRONZE', 
            'IceCubeGOLD'
        ]
        
        self.logger.info("Initialized GCN ToO Emailer")

    def should_send_too_request(self, 
                               notice_data: Dict[str, Any], 
                               visibility_info: Optional[Dict[str, Any]] = None) -> bool:
        """
        Determine if a ToO request should be sent based on notice and visibility.
        
        A request should be sent if:
        1. The notice is from an IceCube facility, OR
        2. The target is currently observable or will be observable soon
        
        Args:
            notice_data: Dictionary containing notice information
            visibility_info: Optional visibility analysis information
            
        Returns:
            Boolean indicating if ToO request should be sent
        """
        try:
            # Get facility name
            facility = notice_data.get('Facility', '')
            
            # Always send ToO request for IceCube facilities (high priority)
            icecube_source = any(icecube_fac in facility for icecube_fac in self.icecube_facilities)
            
            if icecube_source:
                self.logger.info(f"IceCube facility detected: {facility}. Sending ToO request.")
                return True
            
            # If we have visibility info, check if currently observable or soon
            if visibility_info:
                status = visibility_info.get('status', '')
                
                if status == 'observable_now':
                    self.logger.info(f"Target is currently observable. Sending ToO request.")
                    return True
                    
                elif status == 'observable_later':
                    # Check how soon it will be observable
                    hours_until = visibility_info.get('hours_until_observable', 99)
                    
                    # If observable within 2 hours, send ToO request
                    if hours_until <= 2:
                        self.logger.info(f"Target will be observable in {hours_until:.1f} hours. Sending ToO request.")
                        return True
                    else:
                        self.logger.info(f"Target will be observable in {hours_until:.1f} hours (>2 hours). No immediate ToO request.")
                        return False
                else:
                    self.logger.info(f"Target not observable. Status: {status}. No ToO request.")
                    return False
            
            # If no visibility info but coordinates available, default to sending
            # This is a safety fallback in case visibility analysis fails
            if 'RA' in notice_data and 'DEC' in notice_data:
                self.logger.info(f"Missing visibility info but coordinates available. Sending ToO request as precaution.")
                return True
                
            # No visibility info and no coordinates - don't send
            self.logger.info(f"No visibility info and no coordinates. No ToO request.")
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking if ToO request should be sent: {e}")
            
            # Default to sending if there's an error (better to send unnecessarily than miss a valuable target)
            return True

    def _prepare_email_content(self, notice_data: Dict[str, Any], 
                            visibility_info: Optional[Dict[str, Any]] = None,
                            too_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Prepare email content from notice data with visibility information.
        
        Args:
            notice_data: Dictionary containing notice information
            visibility_info: Optional dictionary with visibility analysis
            too_config: Optional dictionary with configuration options
                
        Returns:
            Dictionary containing email content
        """
        # Default configuration
        default_config = {
            'singleExposure': 100,       # Default exposure time in seconds
            'imageCount': 3,             # Default number of images
            'obsmode': 'Deep',           # Default observation mode
            'selectedFilters': ['r', 'i'], # Default filters
            'selectedTelNumber': 1,      # Default number of telescopes
            'abortObservation': 'Yes',   # Default abort setting
            'priority': 'High',          # Default priority
            'gain': 'High',              # Default gain
            'radius': '0',               # Default radius
            'binning': '1',              # Default binning
        }
        
        # Use provided config or defaults
        if too_config is None:
            too_config = {}
        
        # Merge configs with provided values taking precedence
        obs_config = {**default_config, **too_config}
        
        # Calculate total exposure time
        total_exposure = obs_config['singleExposure'] * obs_config['imageCount']
        
        # Create visibility status string
        visibility_status = "Not available"
        if visibility_info:
            status = visibility_info.get('status', '')
            
            if status == 'observable_now':
                end_time = visibility_info.get('observable_end')
                if end_time:  # Check if end_time is not None
                    end_time_str = end_time.strftime('%H:%M')
                else:
                    end_time_str = "Unknown"
                visibility_status = f"Currently observable until {end_time_str} CLT"
            elif status == 'observable_later':
                hours_until = visibility_info.get('hours_until_observable', 0)
                start_time = visibility_info.get('observable_start')
                if start_time:  # Check if start_time is not None
                    start_time_str = start_time.strftime('%H:%M')
                else:
                    start_time_str = "Unknown"
                visibility_status = f"Observable in {hours_until:.1f} hours (from {start_time_str} CLT)"
            else:
                visibility_status = f"Not observable: {visibility_info.get('reason', 'Unknown limitation')}"
        
        # Create data dictionary for email
        email_data = {
            'requester': self.email_from,
            'target': notice_data.get('Name', 'New_GRB_Event'),
            'ra': notice_data.get('RA'),
            'dec': notice_data.get('DEC'),
            'singleExposure': obs_config['singleExposure'],
            'imageCount': obs_config['imageCount'],
            'exposure': total_exposure,
            'obsmode': obs_config['obsmode'],
            'selectedFilters': obs_config['selectedFilters'],
            'selectedTelNumber': obs_config['selectedTelNumber'],
            'abortObservation': obs_config['abortObservation'],
            'priority': obs_config['priority'],
            'gain': obs_config['gain'],
            'radius': obs_config['radius'],
            'binning': obs_config['binning'],
            'obsStartTime': obs_config.get('obsStartTime', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'comments': f"New event {notice_data.get('Name')}. "
                        f"Facility: {notice_data.get('Facility')}. "
                        f"Trigger: {notice_data.get('Trigger_num')}. "
                        f"Visibility: {visibility_status}. "
                        f"Automatic ToO request from GCN Alert System."
        }
        
        # Allow custom comments to be appended
        if too_config.get('additional_comments'):
            email_data['comments'] += f" {too_config['additional_comments']}"
        
        return email_data
    def _determine_neutrino_priority(self, notice_data):
        """
        Determine priority level for neutrino events.
        
        Args:
            notice_data (dict): Parsed notice data
            
        Returns:
            str: Priority level ('NORMAL', 'HIGH', or 'URGENT')
        """
        priority = "NORMAL"
        
        # Check if this is a neutrino event
        facility = notice_data.get('Facility', '')
        if any(fac in facility for fac in ["IceCube", "AMON"]):
            # Default to HIGH for all neutrino events
            priority = "HIGH"
            
            # Handle different neutrino types
            if "GOLD" in facility:
                priority = "HIGH"
            elif "CASCADE" in facility:
                priority = "HIGH"
            elif "COINC" in facility or "NU_EM_COINC" in facility:
                priority = "URGENT"  # Always urgent for coincidence events
        
        return priority
    def customize_too_for_neutrino(self, too_config, notice_data, visibility_info):
        """
        Customize ToO request parameters for neutrino events.
        
        Args:
            too_config (dict): Base ToO configuration
            notice_data (dict): Parsed notice data
            visibility_info (dict): Visibility analysis data
            
        Returns:
            dict: Customized ToO configuration for neutrino observations
        """
        # Copy the base configuration
        neutrino_config = too_config.copy()
        
        # Get facility type and error radius
        facility = notice_data.get('Facility', '')
        error_deg = float(notice_data.get('Error', 1.0))
        
        # Set strategy based on event type
        if "CASCADE" in facility:
            # Cascades have large error circles - optimize for coverage
            neutrino_config.update({
                'singleExposure': 60,           # Shorter exposures
                'imageCount': 9,                # More images for tiling
                'obsmode': 'Mosaic',            # Use mosaic mode
                'selectedFilters': ['r'],       # Only r filter to maximize depth
                'radius': f"{min(error_deg, 2.0)}", # Set radius to error (up to 2 degrees max)
                'additional_comments': f"CASCADE neutrino event with large error radius ({error_deg:.2f}°). Use mosaic observation strategy to cover error region."
            })
        elif "GOLD" in facility:
            # Gold tracks have high reliability - deep imaging
            neutrino_config.update({
                'singleExposure': 180,          # Longer exposures
                'imageCount': 5,                # More images for depth
                'priority': 'HIGH',             # Higher priority
                'additional_comments': f"GOLD neutrino track with high reliability. Deep imaging recommended."
            })
        elif "BRONZE" in facility:
            # Bronze tracks - standard imaging
            neutrino_config.update({
                'singleExposure': 120,          # Moderate exposures
                'imageCount': 3,                # Standard number of images
                'additional_comments': f"BRONZE neutrino track. Standard follow-up recommended."
            })
        elif "COINC" in facility or "NU_EM_COINC" in facility:
            # Coincidence events - highest priority
            neutrino_config.update({
                'singleExposure': 150,          # Moderate-long exposures
                'imageCount': 5,                # More images
                'priority': 'URGENT',           # Highest priority
                'abortObservation': 'Yes',      # Abort current observations
                'additional_comments': f"NEUTRINO-EM COINCIDENCE event with high significance. Immediate follow-up recommended."
            })
        
        # Determine priority
        priority = self._determine_neutrino_priority(notice_data)
        neutrino_config['priority'] = priority
        
        if priority == "URGENT":
            neutrino_config['abortObservation'] = 'Yes'
        
        return neutrino_config

    def send_too_email(self, 
                       notice_data: Dict[str, Any],
                       visibility_info: Optional[Dict[str, Any]] = None,
                       too_config: Optional[Dict[str, Any]] = None) -> bool:
        """
        Send ToO request email with visibility information.
        
        Args:
            notice_data: Dictionary containing notice information
            visibility_info: Optional dictionary with visibility analysis
            too_config: Optional dictionary with configuration options
            
        Returns:
            Boolean indicating if email was sent successfully
        """
        try:
            # Prepare email data with visibility info
            email_data = self._prepare_email_content(notice_data, visibility_info, too_config)
            
            # Construct subject with visibility status for quick recognition
            status_prefix = ""
            if visibility_info:
                if visibility_info.get('status') == 'observable_now':
                    status_prefix = "🟢 [URGENT-OBSERVABLE NOW] "
                elif visibility_info.get('status') == 'observable_later':
                    status_prefix = "🟠 [SCHEDULED] "
                else:
                    status_prefix = "🔴 [ALERT] "
            
            subject = f"{status_prefix}7DT ToO Request for {email_data['target']}"
            
            # Construct email body
            if email_data['obsmode'] == "Spec":
                details1 = f"- Specmode: {email_data.get('selectedSpecFile', 'Default')}"
                details2 = ""
            else:  # Deep imaging mode
                selected_filters = ",".join(email_data['selectedFilters'])
                details1 = f"- Filters: {selected_filters}"
                details2 = f"- NumberofTelescopes: {email_data['selectedTelNumber']}"
                
            # Include visibility status in the email body with appropriate emoji
            visibility_section = ""
            if visibility_info:
                status = visibility_info.get('status', '')
                
                if status == 'observable_now':
                    # Safe access to observable_end with fallback
                    end_time = visibility_info.get('observable_end')
                    end_time_str = end_time.strftime('%H:%M') if end_time else "Unknown"
                    
                    visibility_section = (
                        "**Visibility Status: 🟢 CURRENTLY OBSERVABLE**\n"
                        f"- Observable until: {end_time_str} CLT\n"
                        f"- Current altitude: {visibility_info.get('current_altitude', 0):.1f}°\n"
                        f"- Moon separation: {visibility_info.get('current_moon_separation', 0):.1f}°\n"
                    )
                elif status == 'observable_later':
                    # Safe access with fallbacks
                    start_time = visibility_info.get('observable_start')
                    start_time_str = start_time.strftime('%H:%M') if start_time else "Unknown"
                    
                    visibility_section = (
                        "**Visibility Status: 🟠 OBSERVABLE LATER TONIGHT**\n"
                        f"- Observable from: {start_time_str} CLT\n"
                        f"- Observable window: {visibility_info.get('observable_hours', 0):.1f} hours\n"
                        f"- Recommended start: {start_time_str} CLT\n"
                    )
                else:
                    visibility_section = (
                        "**Visibility Status: 🔴 NOT OBSERVABLE**\n"
                        f"- Reason: {visibility_info.get('reason', 'Unknown limitation')}\n"
                    )
            
            email_body = f"""
================================
AUTOMATIC ToO Request - GRB Alert
================================

**Observation Information**
----------------------
- Requester: {email_data['requester']}
- Target Name: {email_data['target']}
- Right Ascension (R.A.): {email_data['ra']}
- Declination (Dec.): {email_data['dec']}
- Total Exposure Time (seconds): {email_data['exposure']}
- Single Exposure Time (seconds): {email_data['singleExposure']}
- # of images: {email_data['imageCount']}
- Obsmode: {email_data['obsmode']}
    {details1}
    {details2}

{visibility_section}

**Detailed Settings**
--------------------
- Abort Current Observation: {email_data['abortObservation']}
- Priority: {email_data['priority']}
- Gain: {email_data['gain']}
- Radius: {email_data['radius']}
- Binning: {email_data['binning']}
- Observation Start Time: {email_data['obsStartTime']}
- Comments: {email_data['comments']}

================================
THIS IS AN AUTOMATED REQUEST 
PLEASE TAKE APPROPRIATE ACTION
================================
            """
            
            # Save data as JSON
            now_str = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"too_request_{now_str}.json"
            file_path = os.path.join(os.getcwd(), file_name)
            
            with open(file_path, "w") as file:
                json.dump(email_data, file, indent=4)
            
            # Create a multipart message
            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = self.email_from
            msg['To'] = ", ".join(self.email_to)
            
            # Attach the message body
            msg.attach(MIMEText(email_body, 'plain'))
            
            # Attach the JSON file
            with open(file_path, "rb") as file:
                attachment = MIMEApplication(file.read(), Name=file_name)
                attachment['Content-Disposition'] = f'attachment; filename="{file_name}"'
                msg.attach(attachment)
            
            # Send the email
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context) as server:
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
            
            self.logger.info(f"ToO request email sent successfully for {email_data['target']}")
            
            # Clean up
            os.remove(file_path)
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending ToO email: {e}")
            return False
    
    def process_notice(self, 
                      notice_data: Dict[str, Any],
                      too_config: Optional[Dict[str, Any]] = None,
                      visibility_info: Optional[Dict[str, Any]] = None) -> bool:
        """
        Process a notice and send ToO request if criteria are met.
        
        Args:
            notice_data: Dictionary containing notice information
            too_config: Optional dictionary with configuration options
            visibility_info: Optional dictionary with visibility analysis
            
        Returns:
            Boolean indicating if ToO request was sent
        """
        # Check if the notice is from an IceCube facility
        if any(ice_fac in notice_data.get('Facility', '') for ice_fac in ["IceCube", "AMON"]):
            # For neutrino events, apply specialized configuration
            too_config = self.customize_too_for_neutrino(too_config, notice_data, visibility_info)
        
        # Check if ToO request should be sent
        if self.should_send_too_request(notice_data, visibility_info):
            # Send ToO request with visibility info
            return self.send_too_email(notice_data, visibility_info, too_config)
        
        return False