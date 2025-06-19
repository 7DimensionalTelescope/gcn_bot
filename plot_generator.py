#!/usr/bin/env python3
"""
Fixed Visibility Plot Generator for GRB observations

Uses direct import of VisibilityPlotter class.
The key fix: VisibilityPlotter.create_visibility_plot() returns the plot file path,
we don't need to save it ourselves.
"""

import shutil
from datetime import date
from pathlib import Path

def save_visibility_plot(ra, dec, object_name, output_dir="plots"):
    """
    Generate and save visibility plot using VisibilityPlotter
    
    Args:
        ra (float): Right Ascension in degrees
        dec (float): Declination in degrees
        object_name (str): Name of the object
        output_dir (str): Directory to save plots
    
    Returns:
        str: Path to saved file or None if error
    """
    try:
        from supy.supy.observer.visibility_plotter import VisibilityPlotter
        
        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)
        
        # Create plotter and generate plot
        plotter = VisibilityPlotter()
        
        # FIXED: create_visibility_plot() returns (plot_path, visibility_info)
        result = plotter.create_visibility_plot(
            ra=ra, 
            dec=dec, 
            grb_name=object_name,
            test_mode=False,  # Set to False for production use
            minalt=30,
            minmoonsep=30,
            savefig=True
        )
        
        # Check if we got a valid result
        if result is None or len(result) != 2:
            print(f"✗ No plot generated for {object_name} (may not be observable)")
            return None
            
        temp_plot_path, visibility_info = result
        
        # Check if a plot was actually created
        if temp_plot_path is None:
            status = visibility_info.get('status', 'unknown') if visibility_info else 'unknown'
            print(f"✗ No plot for {object_name} - Status: {status}")
            return None
        
        # Create our desired filename
        safe_name = "".join(c for c in object_name if c.isalnum() or c in ('-', '_'))
        date_str = date.today().strftime("%Y%m%d")
        filename = f"{safe_name}_{date_str}.png"
        final_filepath = Path(output_dir) / filename
        
        # Copy the temporary file to our desired location
        shutil.copy2(temp_plot_path, final_filepath)
        
        # Log visibility information
        if visibility_info:
            status = visibility_info.get('status', 'unknown')
            condition = visibility_info.get('condition', 'No condition info')
            print(f"✓ {object_name}: {status} - {condition}")
        
        print(f"✓ Saved plot: {final_filepath}")
        return str(final_filepath)
        
    except Exception as e:
        print(f"✗ Error generating plot for {object_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def batch_save_plots(coordinates_list, output_dir="plots"):
    """
    Save multiple visibility plots
    
    Args:
        coordinates_list (list): List of tuples (ra, dec, name)
        output_dir (str): Directory to save plots
    
    Returns:
        list: List of successfully saved file paths
    """
    saved_files = []
    
    print(f"Generating {len(coordinates_list)} visibility plots...")
    
    for ra, dec, name in coordinates_list:
        filepath = save_visibility_plot(ra, dec, name, output_dir)
        if filepath:
            saved_files.append(filepath)
    
    print(f"Successfully generated {len(saved_files)}/{len(coordinates_list)} plots")
    return saved_files

# Example usage
if __name__ == "__main__":
    # Your GRB coordinates - modify this list as needed
    grb_coordinates = [
        (247.6286, 15.1842, "IceCube-250613B"),
        # Add more coordinates here:
        # (ra, dec, "GRB_Name"),
    ]
    
    # Output directory
    output_dir = "./test_plots"

    # Generate plots
    saved_plots = batch_save_plots(grb_coordinates, output_dir)
    
    # Print results
    if saved_plots:
        print(f"\n✓ Successfully generated {len(saved_plots)} plots:")
        for plot in saved_plots:
            print(f"  - {plot}")
    else:
        print("\n✗ No plots were generated successfully")