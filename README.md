# GCN Alert Processing System

A comprehensive system for monitoring GCN (Gamma-ray Coordinates Network) alerts, processing notices and circulars, and facilitating rapid ToO (Target of Opportunity) observation requests.

## Overview

This system monitors the GCN Kafka stream for gamma-ray burst (GRB) events and other transient phenomena, processes them through a pipeline of specialized handlers, delivers notifications via Slack, and maintains structured databases of events for astronomical follow-up observations.

### Key Components

The system consists of five main Python modules:

1. **gcn_bot.py**: Main entry point that monitors the GCN Kafka stream, formats messages, and sends Slack notifications.
2. **gcn_notice_handler.py**: Processes GCN notices, extracts key information, and saves event data to CSV and ASCII databases.
3. **gcn_circular_handler.py**: Processes GCN circulars (human-written messages), updates databases, and synchronizes with notice data.
4. **visibility_plotter.py**: Analyzes event coordinates to determine observability from telescope locations, creates visibility plots, and provides timing information.
5. **gcn_too_emailer.py**: Generates and sends automated ToO observation requests based on event priority and visibility analysis.

## Architecture

```
                              ┌────────────────────┐
                              │                    │
                              │    GCN Kafka       │
                              │    Stream          │
                              │                    │
                              └─────────┬──────────┘
                                        │
                                        ▼
                              ┌────────────────────┐
                              │                    │
                              │    gcn_bot.py      │ ───────► Slack Notifications
                              │                    │
                              └─────────┬──────────┘
                                        │
            ┌────────────────────┬──────┴─────────┬───────────────────┐
            │                    │                │                   │
            ▼                    ▼                ▼                   ▼
┌───────────────────────┐ ┌─────────────┐ ┌────────────────┐ ┌─────────────────┐
│                       │ │             │ │                │ │                 │
│ gcn_notice_handler.py │ │visibility_  │ │ gcn_circular_  │ │ gcn_too_        │
│                       │ │plotter.py   │ │ handler.py     │ │ emailer.py      │
│                       │ │             │ │                │ │                 │
└───────────┬───────────┘ └──────┬──────┘ └────────┬───────┘ └────────┬────────┘
            │                    │                 │                  │
            ▼                    │                 ▼                  ▼
┌───────────────────────┐        │        ┌────────────────┐   ┌─────────────────┐
│                       │        │        │                │   │                 │
│ gcn_notices.csv       │        │        │ gcn_circulars. │   │ Automated ToO   │
│                       │        │        │ csv            │   │ Email Requests  │
└───────────┬───────────┘        │        └────────┬───────┘   └─────────────────┘
            │                    │                 │
            │                    ▼                 │
            │         ┌───────────────────┐        │
            │         │                   │        │
            │         │ Visibility Plots  │        │
            │         │                   │        │
            │         └───────────────────┘        │
            │                                      │
            └──────────────────┬───────────────────┘
                               ▼
                    ┌───────────────────────┐
                    │                       │
                    │  grb_targets.ascii    │
                    │                       │
                    └───────────────────────┘
```

## Features

- **Real-time Monitoring**: Continuous connection to GCN Kafka stream with heartbeat tracking
- **Multi-facility Support**: Processing for Swift, Fermi, IceCube, Einstein Probe, and other major observatories
- **Alert Formatting**: Structured message formatting with key information highlighted
- **Visibility Analysis**: Automatic calculation of observability from configurable telescope locations
- **Database Management**: Maintenance of CSV and ASCII databases with duplicate detection and conflict resolution
- **Slack Integration**: Real-time notifications with formatted alert data and visibility plots
- **ToO Requests**: Automated email generation for Target of Opportunity observation requests
- **Robust Error Handling**: Comprehensive logging, connection monitoring, and recovery systems

## Prerequisites

- Python 3.7+
- Kafka client with Python bindings
- GCN Kafka client credentials
- Slack API token (for notifications)
- Email credentials (for ToO requests)
- Required Python packages:
  - gcn_kafka
  - slack_sdk
  - astropy
  - matplotlib
  - pandas
  - numpy
  - astroplan

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/gcn-alert-system.git
   cd gcn-alert-system
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy the configuration template:
   ```bash
   cp config.py
   ```

4. Edit `config.py` with your credentials and configuration settings

## Configuration

Edit `config.py` with your settings:

```python
# GCN credentials
GCN_ID = 'your_gcn_client_id'
GCN_SECRET = 'your_gcn_client_secret'

# Slack configuration
SLACK_TOKEN = 'your_slack_bot_token'
SLACK_CHANNEL = '#your-slack-channel'

# Visibility plotter configuration
MIN_ALTITUDE = 30  # Minimum altitude for visibility plots in degrees
MIN_MOON_SEP = 30  # Minimum moon separation in degrees

# ToO email configuration
TURN_ON_TOO_EMAIL = True  # Set to False to disable email requests
EMAIL_FROM = 'your_email@example.com'
EMAIL_PASSWORD = 'your_email_password'
TOO_CONFIG = {
    'singleExposure': 100,       # Exposure time in seconds
    'imageCount': 3,             # Number of images
    'obsmode': 'Deep',           # Observation mode
    'selectedFilters': ['r', 'i'], # Filters to use
    'selectedTelNumber': 1,      # Number of telescopes
    'abortObservation': 'Yes',   # Whether to abort current observations
    'priority': 'High',          # Priority level
    'gain': 'High',              # Gain setting
    'radius': '0',               # Radius setting
    'binning': '1',              # Binning setting
}

# Monitoring topics (add/remove as needed)
DISPLAY_TOPICS = [
    'gcn.classic.text.FERMI_GBM_FIN_POS',
    'gcn.classic.text.SWIFT_XRT_POSITION',
    # Add more topics as needed
]
```

## Usage

### Basic Operation

Run the main bot to start monitoring GCN alerts:

```bash
python gcn_bot.py
```

### Testing Mode

Test the system without sending messages to Slack:

```bash
python gcn_bot.py --test
```

Test and actually send to Slack:

```bash
python gcn_bot.py --test --send
```

### Output Files

The system generates several data files:

- `gcn_notices.csv`: Database of GCN notices
- `gcn_circulars.csv`: Database of GCN circulars
- `grb_targets.ascii`: Combined database with the most recent events, used for observation planning

## Component Details

### Visibility Plotter

The `visibility_plotter.py` module provides:

- **Astronomical visibility analysis**: Determines if targets are observable from specified telescope locations
- **Time window calculation**: Calculates when targets rise above minimum altitude and have sufficient moon separation
- **Visibility plots**: Generates altitude vs. time plots highlighting observable periods
- **Observability status**: Classifies targets as "observable now", "observable later", or "not observable"
- **Recommendation system**: Provides best timing for observations based on astronomical conditions

Configuration parameters:
- `MIN_ALTITUDE`: Minimum altitude in degrees (default: 30°)
- `MIN_MOON_SEP`: Minimum moon separation in degrees (default: 30°)

### ToO Emailer

The `gcn_too_emailer.py` module provides:

- **Automated observing requests**: Generates emails for Target of Opportunity observations
- **Smart scheduling**: Determines whether to request immediate or scheduled observations
- **Visibility integration**: Includes visibility information in requests
- **Request prioritization**: Customizes request urgency based on event characteristics
- **Configurable settings**: Customizable observation parameters (exposure time, filters, etc.)

Configuration parameters:
- `TURN_ON_TOO_EMAIL`: Enable/disable the emailer
- `EMAIL_FROM`: Sender email address
- `EMAIL_PASSWORD`: Email account password or app password
- `TOO_CONFIG`: Dictionary of observation settings

## Monitored Facilities

The system can process alerts from these observatories/instruments:

- **Fermi**: GBM and LAT gamma-ray instruments
- **Swift**: BAT, XRT, and UVOT instruments
- **Einstein Probe**: Wide-field X-ray telescope
- **IceCube**: High-energy neutrino detector
- **HAWC**: High-Altitude Water Cherenkov Gamma-Ray Observatory
- **AMON**: Astrophysical Multimessenger Observatory Network

## Logging

The system generates detailed log files:

- `gcn_bot.log`: Main bot activity log
- `gcn_notice_handler.log`: Notice processing activity
- `gcn_circular_handler.log`: Circular processing activity

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- NASA GCN team for the Kafka alert stream
- The astronomical community for the collaborative alert system
