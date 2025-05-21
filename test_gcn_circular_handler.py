#!/usr/bin/env python3
"""
GCN Circular Handler Test
=========================
A test script to demonstrate the functionality of the GCN Circular Handler.

This script processes the sample GCN circulars provided and shows how
information is extracted and databases are updated.

Author:         YoungPyo Hong
Created:        2025-03-27
"""

import json
import os
import logging
import shutil
from gcn_circular_handler import GCNCircularHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_sample_circulars():
    """Create sample circular files from the provided data."""
    # Create test output directory
    os.makedirs('test_output', exist_ok=True)
    os.makedirs('sample_circulars', exist_ok=True)
    
    # Sample 1: GRB 250322A: Swift/UVOT Upper Limits
    circular1 = {
        "subject": "GRB 250322A: Swift/UVOT Upper Limits",
        "eventId": "GRB 250322A",
        "submittedHow": "web",
        "createdOn": 1742820759410,
        "circularId": 39861,
        "submitter": "Sam Shilling at Lancaster University ",
        "format": "text/plain",
        "body": """S. P. R. Shilling (Lancaster U.) and R. Gupta (NASA/GSFC)
report on behalf of the Swift/UVOT team:

The Swift/UVOT began settled observations of the field of GRB 250322A
77 s after the BAT trigger (Gupta et al., GCN Circ. 39835).
No optical afterglow consistent with the enhanced XRT position
(Goad et al., GCN Circ. 39841) is detected in the initial UVOT exposures.
Preliminary 3-sigma upper limits using the UVOT photometric system
(Breeveld et al. 2011, AIP Conf. Proc. 1358, 373) for the first
finding chart (FC) exposure and subsequent exposures are:

Filter         T_start(s)   T_stop(s)      Exp(s)         Mag

white_FC            77          226          147         >20.5
u_FC               291          541          246         >19.8
white               77         1711          411         >20.9
v                  620         1760          136         >18.9
b                  546         1686          117         >19.7
u                  291         1661          343         >20.0
w1                 669         1807          134         >19.2
m2                 644         1785          136         >19.0
w2                 596         1736          136         >19.2

The magnitudes in the table are not corrected for the Galactic extinction
due to the reddening of E(B-V) = 0.153 in the direction of the burst
(Schlegel et al. 1998)."""
    }
    
    # Sample 2: GRB 250322A: Enhanced Swift-XRT position
    circular2 = {
        "subject": "GRB 250322A: Enhanced Swift-XRT position",
        "eventId": "GRB 250322A",
        "submittedHow": "email",
        "createdOn": 1742679229556,
        "circularId": 39841,
        "submitter": "Phil Evans at U of Leicester ",
        "body": """M.R. Goad, J.P. Osborne, A.P. Beardmore and P.A. Evans (U. Leicester) 
report on behalf of the Swift-XRT team.

Using 1707 s of XRT Photon Counting mode data and 1 UVOT
images for GRB 250322A, we find an astrometrically corrected X-ray
position (using the XRT-UVOT alignment and matching UVOT field sources
to the USNO-B1 catalogue): RA, Dec = 106.76048, +7.19313 which is equivalent
to:

RA (J2000): 07h 07m 2.52s
Dec (J2000): +07d 11' 35.3"

with an uncertainty of 2.6 arcsec (radius, 90% confidence).

This position may be improved as more data are received. The latest
position can be viewed at http://www.swift.ac.uk/xrt_positions. Position
enhancement is described by Goad et al. (2007, A&A, 476, 1401) and Evans
et al. (2009, MNRAS, 397, 1177).

This circular was automatically generated, and is an official product of the
Swift-XRT team."""
    }
    
    # Sample 3: GRB 250322A: VLT/X-shooter redshift confirmation
    circular3 = {
        "subject": "GRB 250322A: VLT/X-shooter redshift confirmation of the putative host galaxy ",
        "eventId": "GRB 250322A",
        "submittedHow": "web",
        "createdOn": 1742816460906,
        "circularId": 39859,
        "submitter": "Yu-Han Yang at University of Rome Tor Vergata ",
        "format": "text/markdown",
        "body": """Yu-Han Yang (U Rome), Eleonora Troja (U Rome), Rosa Becerra (U Rome), Massine El Kabir (U Rome)  report on behalf of the ERC BHianca team:

We observed the bright galaxy within the localization of GRB 250322A (Gupta et al., GCN 39835, Goad et al. GCN 39841, Martin-Carillo et al. GCN 39842) with the X-Shooter spectrograph on the ESO VLT UT3 (Melipal). Observations began at T+32.2 hours and obtained a total of 2x600s spectra at an average airmass of about 1.2 and seeing of 0.6". 

We detect a bright continuum in the VIS and NIR arms and identify multiple emission lines, including H_alpha, H_beta, NII, OII, SII, at a common redshift of 0.4215±0.0005, consistent with measurement by Fong et al. (GCN 39852). We estimated that the chance coincidence between the bright galaxy and the XRT localization is <2% (Bloom et al. 2002), supporting it as a likely host galaxy of the GRB.

We thank the staff at the VLT, for the rapid execution of these observations."""
    }
    
    # Sample 4: Fermi GBM false trigger
    circular4 = {
        "subject": "Fermi Gamma-ray Burst Monitor trigger 763509110/250312911 is not a GRB",
        "submittedHow": "web",
        "bibcode": "2025GCN.39696....1S",
        "createdOn": 1741820270056,
        "circularId": 39696,
        "submitter": "Lorenzo Scotton at UAH ",
        "format": "text/plain",
        "body": """L. Scotton (UAH) reports on behalf of the Fermi Gamma-ray Burst Monitor Team:

"The Fermi Gamma-ray Burst Monitor (GBM) trigger 763509110/250312911 at 21:51:45.47 UT
on 12 March 2025, tentatively classified as a GRB, is in fact not due
to a GRB. This trigger is likely due to SAA entry." """
    }
    
    # Sample 5: Fermi-LAT detection
    circular5 = {
        "subject": "GRB 250320B: Fermi-LAT detection",
        "eventId": "GRB 250320B",
        "submittedHow": "web",
        "createdOn": 1742580359768,
        "circularId": 39819,
        "submitter": "A. Holzmann Airasca at University of Trento and INFN Bari ",
        "format": "text/plain",
        "body": """A. Holzmann Airasca (UniTrento and INFN Bari), S. Lopez (CNRS / IN2P3), P. Monti-Guarnieri (University and INFN, Trieste), N. Di Lalla (Stanford Univ.), F. Longo (University and INFN, Trieste) and R. Gupta (NASA GSFC) report on behalf of the Fermi-LAT Collaboration:
On March 20, 2025, Fermi-LAT detected high-energy emission from GRB 250320B, which was also detected by Fermi-GBM (trigger 764205327 / 250320969, GCN 39792), AstroSat CZTI (GCN 39808) and SVOM/GRM (GCN 39813).

The best LAT on-ground location is found to be:
RA, Dec = 244.66, -30.37 (J2000)

with an error radius of 0.3 deg (90 % containment, statistical error only). 

This was 40 deg from the LAT boresight at the time of the GBM trigger (T0 = 23:15:22.02 UT).

The data from the Fermi-LAT shows a significant increase in the event rate that is spatially and temporally correlated with the GBM emission with high significance. The photon flux above 100 MeV in the time interval 0 - 1400 s after the GBM trigger is (3.6 ± 1.1) E-6 ph/cm2/s. The estimated photon index above 100 MeV is -2.35 ± 0.31.

The highest-energy photon is a 1.9 GeV event which is observed ~ 245 seconds after the GBM trigger.
A Swift ToO has been requested for this burst.

The Fermi-LAT point of contact for this burst is Aldana Holzmann Airasca (aldana.holzmannairasca@ba.infn.it).

The Fermi-LAT is a pair conversion telescope designed to cover the energy band from 20 MeV to greater than 300 GeV. It is the product of an international collaboration between NASA and DOE in the U.S. and many scientific institutions across France, Italy, Japan and Sweden."""
    }
    
    # Write the samples to files
    with open('sample_circulars/circular1.json', 'w') as f:
        json.dump(circular1, f, indent=2)
    
    with open('sample_circulars/circular2.json', 'w') as f:
        json.dump(circular2, f, indent=2)
    
    with open('sample_circulars/circular3.json', 'w') as f:
        json.dump(circular3, f, indent=2)
    
    with open('sample_circulars/circular4.json', 'w') as f:
        json.dump(circular4, f, indent=2)
    
    with open('sample_circulars/circular5.json', 'w') as f:
        json.dump(circular5, f, indent=2)
    
    logger.info("Created 5 sample circular files in 'sample_circulars' directory")

def setup_test_environment():
    """Setup test environment by creating necessary directories and sample ASCII file."""
    # Create test directories
    os.makedirs('test', exist_ok=True)
    
    # Create a sample ASCII file with existing entries for testing the update functionality
    sample_ascii = """GCN_ID Name RA DEC Error Discovery_UTC Facility Trigger_num Notice_date Redshift Host_info
GCN_FermiGBM_760683844 "GRB 250113K" 191.4 -11.82 6.77 "2025-01-13 05:32:07.170000" FermiGBM 760683844 "2025-01-13 02:13:25"  
GCN_SwiftBAT_1287821 "GRB 250113J" 16.09 -12.1645 0.0733333333333333 "2025-01-13 05:32:07.170000" SwiftBAT 1287821 "2025-01-13 01:45:10"  
GCN_EinsteinProbe_01709130131 "GRB 250113I" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:50:53.680736"  
GCN_EinsteinProbe_01709130131 "GRB 250113H" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:47:53.933089"  
GCN_EinsteinProbe_01709130131 "GRB 250113G" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:44:38.798483"  
GCN_EinsteinProbe_01709130131 "GRB 250113F" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:40:17.999829"  
GCN_EinsteinProbe_01709130131 "GRB 250113E" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:36:19.128232"  
GCN_EinsteinProbe_01709130131 "GRB 250113D" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:32:11.215927"  
GCN_EinsteinProbe_01709130131 "GRB 250113C" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:25:37.717336"  
GCN_EinsteinProbe_01709130131 "GRB 250113B" 94.224 56.893 0.05094559 "2025-01-13 01:20:44.949000" EinsteinProbe 1709130131 "2025-02-14 18:25:28.939265"  """
    
    with open('test/gcn_notices_test_ascii.ascii', 'w') as f:
        f.write(sample_ascii)
    
    logger.info("Test environment setup complete")

def cleanup_test_environment():
    """Clean up test files after running tests."""
    try:
        # Remove sample directories
        if os.path.exists('sample_circulars'):
            shutil.rmtree('sample_circulars')
        
        # Remove test output files
        if os.path.exists('test_output'):
            shutil.rmtree('test_output')
            
        logger.info("Test cleanup complete")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

def run_unit_tests():
    """Run unit tests for individual extraction functions."""
    import unittest
    
    class GCNCircularHandlerTests(unittest.TestCase):
        def setUp(self):
            self.handler = GCNCircularHandler(
                output_csv='test_output/test_output.csv',
                output_ascii='test_output/test_output.ascii'
            )
            
        def test_extract_redshift(self):
            # Test basic redshift extraction
            text = "We measure a redshift of 0.4215±0.0005"
            z, z_err = self.handler._extract_redshift(text)
            self.assertIsNotNone(z, "Redshift should not be None")
            if z is not None:
                self.assertAlmostEqual(float(z), 0.4215, places=4)
            self.assertIsNotNone(z_err, "Redshift error should not be None")
            if z_err is not None:
                self.assertAlmostEqual(float(z_err), 0.0005, places=4)
            
            # Test alternate format
            text = "The host galaxy has z = 1.23"
            z, z_err = self.handler._extract_redshift(text)
            self.assertIsNotNone(z, "Redshift should not be None")
            if z is not None:
                self.assertAlmostEqual(float(z), 1.23, places=2)
            self.assertIsNone(z_err, "Redshift error should be None")
            
        def test_extract_coordinates(self):
            # Test decimal degree format
            text = "Enhanced Swift-XRT position: RA, Dec = 106.76048, +7.19313 with an uncertainty of 2.6 arcsec"
            ra, dec, err, unit = self.handler._extract_coordinates(text, "SwiftXRT")
            self.assertIsNotNone(ra, "RA should not be None")
            self.assertIsNotNone(dec, "Dec should not be None")
            self.assertIsNotNone(err, "Error should not be None")
            
            if ra is not None and dec is not None and err is not None:
                self.assertAlmostEqual(float(ra), 106.76048, places=5)
                self.assertAlmostEqual(float(dec), 7.19313, places=5)
                self.assertAlmostEqual(float(err), 2.6, places=1)
            
            self.assertEqual(unit, "arcsec")
            
            # Test error conversion
            degrees = self.handler._convert_error_to_degrees(3600, "arcsec")
            self.assertIsNotNone(degrees, "Converted degrees should not be None")
            if degrees is not None:
                self.assertAlmostEqual(float(degrees), 1.0, places=1)
            
        def test_check_false_trigger(self):
            # Test false trigger detection
            text = "The Fermi Gamma-ray Burst Monitor (GBM) trigger 763509110/250312911 is not due to a GRB"
            self.assertTrue(self.handler._check_false_trigger(text, "FermiGBM"))
            
            # Test non-false trigger
            text = "GRB detected with coordinates RA, Dec = 244.66, -30.37"
            self.assertFalse(self.handler._check_false_trigger(text, "FermiLAT"))
            
        def test_extract_facility(self):
            # Test facility extraction from subject
            subject = "GRB 250322A: Swift/XRT Upper Limits"
            body = "report on behalf of the Swift/XRT team"
            facility = self.handler._extract_facility(subject, body)
            self.assertEqual(facility, "SwiftXRT")
            
        def test_extract_trigger_number(self):
            # Test trigger number extraction
            subject = "Fermi GBM trigger 764205327/250320969 is a GRB"
            body = "The Fermi GBM (trigger 764205327 / 250320969) detected a GRB"
            trigger = self.handler._extract_trigger_number(subject, body, "FermiGBM")
            self.assertEqual(trigger, "764205327")
    
    # Create a test suite and manually add tests        
    suite = unittest.TestSuite()
    suite.addTest(GCNCircularHandlerTests('test_extract_redshift'))
    suite.addTest(GCNCircularHandlerTests('test_extract_coordinates'))
    suite.addTest(GCNCircularHandlerTests('test_check_false_trigger'))
    suite.addTest(GCNCircularHandlerTests('test_extract_facility'))
    suite.addTest(GCNCircularHandlerTests('test_extract_trigger_number'))
    
    # Run the test suite
    logger.info("Running unit tests...")
    runner = unittest.TextTestRunner()
    runner.run(suite)
    logger.info("Unit tests completed")
    
def test_extraction_accuracy(handler):
    """Test the accuracy of extraction functions on sample circulars."""
    # Test on Fermi-LAT sample (circular5)
    with open('sample_circulars/circular5.json', 'r') as f:
        circular_data = json.load(f)
    
    processed = handler.process_circular(circular_data)
    
    # Check RA, Dec extraction
    assert processed['ra'] == 244.66, f"RA mismatch: {processed['ra']} != 244.66"
    assert processed['dec'] == -30.37, f"Dec mismatch: {processed['dec']} != -30.37"
    assert processed['error'] == 0.3, f"Error mismatch: {processed['error']} != 0.3"
    assert processed['error_unit'] == 'deg', f"Error unit mismatch: {processed['error_unit']} != 'deg'"
    
    # Check trigger number extraction (should extract 764205327)
    assert processed['trigger_num'] == '764205327', f"Trigger number mismatch: {processed['trigger_num']} != '764205327'"
    
    # Test on redshift sample (circular3)
    with open('sample_circulars/circular3.json', 'r') as f:
        circular_data = json.load(f)
    
    processed = handler.process_circular(circular_data)
    
    # Print the actual host info for debugging
    print(f"DEBUG - Extracted host info: {processed['host_info']}")
    
    # Check redshift extraction
    assert processed['redshift'] == 0.4215, f"Redshift mismatch: {processed['redshift']} != 0.4215"
    assert processed['redshift_error'] == 0.0005, f"Redshift error mismatch: {processed['redshift_error']} != 0.0005"
    
    # Updated host info check: be more flexible in what we accept
    assert processed['host_info'] is not None, "Host info not extracted"
    assert "host galaxy" in processed['host_info'].lower() or \
           "bright galaxy" in processed['host_info'].lower() or \
           "supporting it as a likely host galaxy" in processed['host_info'].lower(), \
           f"Host info doesn't contain expected text: {processed['host_info']}"
    
    # Test on false trigger (circular4)
    with open('sample_circulars/circular4.json', 'r') as f:
        circular_data = json.load(f)
    
    # Print the content of circular4 for debugging
    print(f"DEBUG - False trigger circular subject: {circular_data['subject']}")
    print(f"DEBUG - False trigger circular body: {circular_data['body']}")
    
    processed = handler.process_circular(circular_data)
    
    # Print the processed false_trigger flag for debugging
    print(f"DEBUG - Processed false_trigger flag: {processed['false_trigger']}")
    
    # Test false trigger manually on the text
    test_body = circular_data['body']
    manual_result = "not due to a GRB" in test_body
    print(f"DEBUG - Manual check for 'not due to a GRB': {manual_result}")
    
    # Check false trigger detection with more detailed error message
    assert processed['false_trigger'] == True, \
           f"False trigger not detected: {processed['false_trigger']} != True\n" \
           f"Circular text: '{test_body}'"
    
    logger.info("Extraction accuracy tests passed successfully!")

def visualize_results():
    """Display the results of the processing in a readable format."""
    import pandas as pd
    
    # Check if CSV database exists
    if os.path.exists('test/gcn_circulars_test.csv'):
        logger.info("CSV Database Summary:")
        df = pd.read_csv('test/gcn_circulars_test.csv')
        
        # Display key columns
        display_cols = ['circular_id', 'event_name', 'facility', 'trigger_num', 
                       'ra', 'dec', 'error', 'redshift', 'false_trigger']
        print(df[display_cols].to_string(index=False))
        
        # Print statistics
        print("\nDatabase Statistics:")
        print(f"Total entries: {len(df)}")
        print(f"Unique events: {df['event_name'].nunique()}")
        print(f"Entries with coordinates: {df['ra'].notna().sum()}")
        print(f"Entries with redshift: {df['redshift'].notna().sum()}")
        print(f"False triggers: {df['false_trigger'].sum()}")
    
    # Check if ASCII database exists
    if os.path.exists('test/gcn_notices_test_ascii.ascii'):
        logger.info("\nASCII Database Summary:")
        try:
            df = pd.read_csv('test/gcn_notices_test_ascii.ascii', sep=' ')
            print(df.to_string(index=False))
        except Exception:
            # If there are issues with pandas reading the file, just print raw content
            with open('test/gcn_notices_test_ascii.ascii', 'r') as f:
                print(f.read())

def main():
    """Run the GCN Circular Handler test with enhanced functionality."""
    # Setup test environment
    setup_test_environment()
    
    # Create sample files
    create_sample_circulars()
    
    # Define output paths
    output_csv = 'test/gcn_circulars_test.csv'
    output_ascii = 'test/gcn_notices_test_ascii.ascii'
    
    # Initialize the handler
    handler = GCNCircularHandler(
        output_csv=output_csv,
        output_ascii=output_ascii
    )
    
    # Run unit tests
    run_unit_tests()
    
    # Test extraction accuracy
    test_extraction_accuracy(handler)
    
    # Process the batch of circulars
    logger.info("Processing batch of sample circulars...")
    handler.process_batch_from_directory('sample_circulars')
    
    # Display results
    visualize_results()
    
    # Clean up test environment (uncomment to enable cleanup)
    # cleanup_test_environment()
    
    logger.info("Test completed successfully")

if __name__ == "__main__":
    main()