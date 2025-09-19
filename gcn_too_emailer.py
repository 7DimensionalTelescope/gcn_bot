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
    
    def evaluate_too_criteria(self, 
                            visibility_info: Optional[Dict[str, Any]] = None
                            ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Evaluate whether a ToO request should be sent and determine priority based on visibility.
        
        Args:
            visibility_info: Visibility analysis information
            
        Returns:
            Tuple[bool, str, dict]: (should_send, reason, too_config_prior)
        """
        
        # Initialize base config
        too_config_prior = {
            'priority': '50',  # Default priority   
        }
        
        # For automatic requests, check visibility-based criteria
        if visibility_info:
            status = visibility_info.get('current_status', '')
            next_opportunity = visibility_info.get('next_opportunity', '')
            
            if status in ['observable_now', 'OBSERVABLE']:
                if visibility_info.get('when_observable') == 'now':
                    remaining_hours = visibility_info.get('current_window', '').get('remaining_hours', 0)
                    # if remaining_hours >= 2.0:
                    #     too_config_prior['priority'] = '50'  # High priority for currently observable
                    #     too_config_prior['abortObservation'] = 'Yes' if remaining_hours < 2 else 'No'
                    #     return True, f"Currently Observable ({remaining_hours:.1f}h remaining)", too_config_prior
                    # else:
                    #     return False, f"Observable but limited ({remaining_hours:.1f}h remaining)", too_config_prior
                    return True, f"Currently Observable ({remaining_hours:.1f}h remaining)", too_config_prior
            
                else: # Observable later tonight
                    hours_until = visibility_info.get('current_window', '').get('hours_until_observable', 0)
                    
                    if hours_until <= 2.0:
                        too_config_prior['priority'] = '60'  # Medium priority for soon observable
                        return True, f"Observable in {hours_until:.1f} hours", too_config_prior
                    else:
                        return False, f"Observable later but not urgent (in {hours_until:.1f}h)", too_config_prior
            
            elif next_opportunity:
                # Skip next opportunity
                return False, f"Observable later - plan for {next_opportunity['days_from_now']} days from now", too_config_prior
            
            else:  # not_observable
                return False, "Not observable from Chile", too_config_prior
        
        return False, "No coordinates available", too_config_prior

    def prepare_email_data(self, 
                        notice_data: Dict[str, Any],
                        too_config: Dict[str, Any],
                        requester_email: Optional[str] = None,
                        submitter_info: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Unified method to prepare email data from any source.
        """
        # Use provided requester or default
        requester = requester_email or self.email_from
        
        # Calculate total exposure
        single_exposure = too_config.get('singleExposure', 100)
        image_count = too_config.get('imageCount', 3)
        total_exposure = single_exposure * image_count
        
        # Build email data structure
        email_data = {
            'requester': requester,
            'target': notice_data.get('target', notice_data.get('Name', 'Unknown_Target')),
            'ra': notice_data.get('ra', notice_data.get('RA')),
            'dec': notice_data.get('dec', notice_data.get('DEC')),
            'singleExposure': single_exposure,
            'imageCount': image_count,
            'exposure': total_exposure,
            'obsmode': too_config.get('obsmode', 'Deep'),
            'specmode': too_config.get('specmode', 'specall'),
            'selectedFilters': too_config.get('selectedFilters', ['r', 'i']),
            'selectedTelNumber': too_config.get('selectedTelNumber', 1),
            'abortObservation': too_config.get('abortObservation', 'No'),
            'priority': too_config.get('priority', 'Medium'),
            'gain': too_config.get('gain', 'High'),
            'radius': too_config.get('radius', '0'),
            'binning': too_config.get('binning', '1'),
            'obsStartTime': too_config.get('obsStartTime', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        }
        
        # Build comments
        comments_parts = []
        
        # Add submitter info if from Slack
        if submitter_info:
            comments_parts.append(f"Submitted via Slack by {submitter_info['name']} ({submitter_info['email']})")
            submission_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            comments_parts.append(f"at {submission_time}")
        
        # Add GCN event info
        event_info = []
        if notice_data.get('Name'):
            event_info.append(f"Target: {notice_data['Name']}")
        if notice_data.get('Facility'):
            event_info.append(f"Facility: {notice_data['Facility']}")
        if notice_data.get('Trigger_num'):
            event_info.append(f"Trigger: {notice_data['Trigger_num']}")
        
        if event_info:
            comments_parts.append("; ".join(event_info))
        
        # Add custom comment if provided
        if too_config.get('comment'):
            comments_parts.append(too_config['comment'])
        elif too_config.get('additional_comments'):
            comments_parts.append(too_config['additional_comments'])
        
        # Default comment if nothing else
        if not comments_parts:
            comments_parts.append("Automatic ToO request from GCN Alert System")
        
        email_data['comments'] = ". ".join(comments_parts)
        
        return email_data
    
    def send_too_email(self, email_data: Dict[str, Any]) -> bool:
        """
        Send ToO request email with prepared data.
        
        Args:
            email_data: Dictionary containing all email fields (already prepared)
            
        Returns:
            Boolean indicating if email was sent successfully
        """
        try:
            # Validate email_data
            if not email_data:
                self.logger.error("No email data provided to send_too_email")
                return False
                
            # Log what we're about to send
            self.logger.debug(f"Preparing to send email for target: {email_data.get('target', 'Unknown')}")
            
            subject = f"7DT ToO Request for {email_data['target']}"
            
            # Construct email body based on obsmode
            if email_data.get('obsmode') == "Spec":
                details1 = f"- Specmode: {email_data.get('specmode', 'specall')}"
                details2 = ""
            else:  # Deep imaging mode
                selected_filters = email_data.get('selectedFilters', ['r', 'i'])
                if isinstance(selected_filters, list):
                    filter_str = ",".join(selected_filters)
                else:
                    filter_str = str(selected_filters)
                details1 = f"- Filters: {filter_str}"
                details2 = f"- NumberofTelescopes: {email_data.get('selectedTelNumber', 1)}"
            
            email_body = f"""
    ================================
    AUTOMATIC ToO Request - GRB Alert
    ================================

    **Observation Information**
    ----------------------
    - Requester: {email_data.get('requester', 'Unknown')}
    - Target Name: {email_data.get('target', 'Unknown')}
    - Right Ascension (R.A.): {email_data.get('ra', 'N/A')}
    - Declination (Dec.): {email_data.get('dec', 'N/A')}
    - Total Exposure Time (seconds): {email_data.get('exposure', 0)}
    - Single Exposure Time (seconds): {email_data.get('singleExposure', 0)}
    - # of images: {email_data.get('imageCount', 0)}
    - Obsmode: {email_data.get('obsmode', 'Deep')}
        {details1}
        {details2}

    **Detailed Settings**
    --------------------
    - Abort Current Observation: {email_data.get('abortObservation', 'No')}
    - Priority: {email_data.get('priority', 'Medium')}
    - Gain: {email_data.get('gain', 'High')}
    - Radius: {email_data.get('radius', '0')}
    - Binning: {email_data.get('binning', '1')}
    - Observation Start Time: {email_data.get('obsStartTime', 'ASAP')}
    - Comments: {email_data.get('comments', 'No comments')}

    ================================
    THIS IS AN AUTOMATED REQUEST 
    PLEASE TAKE APPROPRIATE ACTION
    ================================
            """
            
            # Save data as JSON
            now_str = datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = f"too_request_{now_str}.json"
            file_path = os.path.join(os.getcwd(), file_name)
            
            self.logger.debug(f"Saving JSON file to: {file_path}")
            
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
            
            self.logger.info(f"Sending email to {', '.join(self.email_to)} for target {email_data.get('target')}")
            
            # Send the email
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context) as server:
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
            
            self.logger.info(f"ToO request email sent successfully for {email_data.get('target')}")
            
            # Clean up
            try:
                os.remove(file_path)
                self.logger.debug(f"Cleaned up temporary file: {file_path}")
            except Exception as cleanup_error:
                self.logger.warning(f"Could not remove temporary file {file_path}: {cleanup_error}")
            
            return True
            
        except KeyError as ke:
            self.logger.error(f"Missing required field in email_data: {ke}")
            return False
        except smtplib.SMTPException as smtp_error:
            self.logger.error(f"SMTP error sending ToO email: {smtp_error}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending ToO email: {e}", exc_info=True)
            return False