"""
GCN Bot Configuration
====================
Configuration settings for the GCN Alert Monitoring System.

This file contains all customizable parameters for the GCN Alert Bot,
separated from the main code for easier maintenance and deployment.
"""

# Connection settings
CONNECTION_TIMEOUT = 300  # GCN server connection timeout in seconds

# Slack configuration
SLACK_TOKEN = "your_slack_bot_token"  # Replace with your Slack bot token
SLACK_CHANNEL = "your_slack_channel"  # Replace with your Slack channel name

# GCN configuration
GCN_ID = 'your_gcn_client_id'  # Replace with your GCN client ID
GCN_SECRET = 'your_gcn_client_secret'  # Replace with your GCN client secret
SLACK_CHANNEL_TEST = "your_slack_channel"  # Replace with your Slack channel name (Optional)

# Plotting configuration
MIN_ALTITUDE = 30  # Minimum altitude for visibility plots
MIN_MOON_SEP = 30  # Minimum moon separation for visibility plots
SEND_TO_THREAD = False # Send plot to Slack in a separate thread

# GCNNoticeHandler configuration
TURN_ON_NOTICE = True  # Enable notice saving
OUTPUT_CSV = 'gcn_notices.csv'  # CSV file path
OUTPUT_ASCII = 'grb_targets.ascii'  # ASCII file path
ASCII_MAX_EVENTS = 10  # Maximum events to store in the ASCII file

# GCNCircularHandler configuration
OUTPUT_CIRCULAR_CSV = 'gcn_circular.csv'  # CSV file path

# GCNToOEmailer configuration
TURN_ON_TOO_EMAIL = False  # Enable email requests
EMAIL_FROM = "your_email@example.com"  # Replace with your email address
EMAIL_PASSWORD = "your_email_password"  # Replace with your email password
TOO_CONFIG = {
    'singleExposure': 100,       # Set up exposure time in seconds
    'imageCount': 3,             # Set up number of images
    'obsmode': 'Deep',           # Set up observation mode
    'selectedFilters': ['r', 'i'], # Set up filters
    'selectedTelNumber': 1,      # Set up number of telescopes
    'abortObservation': 'Yes',   # Set up abort setting
    'priority': 'High',          # Set up priority
    'gain': 'High',              # Set up gain
    'radius': '0',               # Set up radius
    'binning': '1',              # Set up binning
}

# GCN Topics to monitor - add or remove topics as needed
DISPLAY_TOPICS = [    
    'gcn.classic.text.AMON_NU_EM_COINC',           # Combined IC+HAWC and ANTARES+Fermi                 | 4-8 alerts per year    | 7 h
    'gcn.classic.text.ICECUBE_CASCADE',            # Hi-energy single neutrino cascade event direction  | 8   alerts per year    | 0.5-1 min
    'gcn.classic.text.HAWC_BURST_MONITOR',         # HAWC alert of GRB-like events                      | 1   alert  per year    | 0.5-1 min
    'gcn.classic.text.ICECUBE_ASTROTRACK_BRONZE',  # Hi-energy single neutrino directions               | 1.3 alert  per month   | 0.5-1 min
    'gcn.classic.text.ICECUBE_ASTROTRACK_GOLD',    # Hi-energy single neutrino directions               | 1   alert  per month   | 0.5-1 min
    
    'gcn.classic.text.FERMI_GBM_GND_POS',          # Position Notice, Automated-Ground-calculated       | 1 alert per week       | 20-300 sec
    # 'gcn.classic.text.FERMI_GBM_FIN_POS',          # Position Notice, Human-in-the-Loop                 | 1 alert per week       | 15 min
    # 'gcn.classic.text.FERMI_LAT_POS_UPD',          # LAT burst-and/or-afterglow location, Updated       | < 1 alert per week     | 2–32 sec  # Not available
    'gcn.classic.text.FERMI_LAT_OFFLINE',          # LAT location of a ground-found burst               | < 1 alert per week     | 8–12 h
    
    'gcn.classic.text.SWIFT_BAT_GRB_POS_ACK',      # First Position Notice, the BAT Position            | < 1 alert per week     | 13–30 sec
    'gcn.classic.text.SWIFT_UVOT_POS',             # UVOT afterglow location                            | < 1 alert per week     | 1-3 h
    'gcn.classic.text.SWIFT_XRT_POSITION',         # XRT afterglow location                             | < 1 alert per week     | 30–80 sec
    
    # JSON format topics
    'gcn.notices.einstein_probe.wxt.alert'         # Einstein Probe WXT alerts                          | 2 alerts per week      | ~1 min
]