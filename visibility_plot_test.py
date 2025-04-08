#!/usr/bin/env python3
"""
Simple Visibility Plotter
=========================
Create a visibility plot for astronomical targets based on RA and Dec coordinates.

Usage:
    python simple_visibility.py --ra 180.0 --dec -30.0 [--name "GRB 250322A"] [--save "output.png"]
"""

import argparse
import matplotlib.pyplot as plt
from mainobserver import mainObserver
from staralt import Staralt
import sys
import os

def create_visibility_plot(ra, dec, name=None, save_path=None, min_alt=30, min_moonsep=30):
    """
    Create a visibility plot for the given coordinates.
    
    Args:
        ra (float): Right Ascension in degrees
        dec (float): Declination in degrees
        name (str, optional): Name of the target to display on the plot
        save_path (str, optional): Path to save the plot. If None, shows the plot
        min_alt (float): Minimum altitude for observability in degrees
        min_moonsep (float): Minimum moon separation in degrees
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Initialize observer and staralt objects
        observer = mainObserver()  # Uses default location (7DT telescope in Chile)
        staralt = Staralt(observer)
        
        # Get visibility data
        data = staralt.staralt_data(
            ra=ra,
            dec=dec,
            objname=name if name else f"Target RA={ra:.2f}, Dec={dec:.2f}\n",
            target_minalt=min_alt,
            target_minmoonsep=min_moonsep
        )
        
        # Create the plot
        staralt.plot_staralt(data)
        
        # Save or show plot
        if save_path:
            plt.savefig(save_path, bbox_inches='tight', dpi=300)
            print(f"Plot saved to: {save_path}")
        else:
            plt.show()
            
        return True
    
    except Exception as e:
        print(f"Error creating visibility plot: {e}")
        return False

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Create visibility plots for astronomical targets")
    
    # Add arguments
    parser.add_argument('--ra', type=float, required=True, help="Right Ascension in degrees")
    parser.add_argument('--dec', type=float, required=True, help="Declination in degrees")
    parser.add_argument('--name', type=str, help="Name of the target")
    parser.add_argument('--save', type=str, help="Path to save the plot (if not specified, shows plot)")
    parser.add_argument('--min-alt', type=float, default=30, help="Minimum altitude for visibility (degrees)")
    parser.add_argument('--min-moonsep', type=float, default=30, help="Minimum moon separation (degrees)")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Create the plot
    success = create_visibility_plot(
        ra=args.ra,
        dec=args.dec,
        name=args.name,
        save_path=args.save,
        min_alt=args.min_alt,
        min_moonsep=args.min_moonsep
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()