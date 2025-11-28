"""
Fermi GBM Tile Coverage Analysis
Analyzes how many 7DT tiles are needed to cover Fermi GBM error regions
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
import json
import sys
from tqdm import tqdm

# Add supy path
from supy.supy.tiles import Tiles


class FermiGBMTileAnalyzer:
    """
    Analyzer for determining tile coverage requirements for Fermi GBM observations.
    """
    
    def __init__(self, data_path, output_dir="fermi_gbm_tile_analysis"):
        """
        Initialize analyzer.
        
        Parameters
        ----------
        data_path : str
            Path to GCN notices CSV file
        output_dir : str
            Directory for output files
        """
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.tiles = Tiles()
        self.data = None
        self.fermi_gbm_data = None
        self.total_fermi_gbm_events = 0
        self.results = None
        self.statistics = {}
        
        print(f"Fermi GBM Tile Coverage Analyzer initialized")
        print(f"Output directory: {self.output_dir}")
        print("-" * 70)
    
    def load_fermi_gbm_data(self):
        """Load and filter Fermi GBM data."""
        print("\n[Phase 1] Loading Fermi GBM data...")
        
        self.data = pd.read_csv(self.data_path)
        print(f"Total events loaded: {len(self.data)}")
        
        # Filter for Fermi GBM only
        self.fermi_gbm_data = self.data[
            self.data['Facility'] == 'FermiGBM'
        ].copy()
        self.total_fermi_gbm_events = len(self.fermi_gbm_data)
        print(f"Fermi GBM events: {self.total_fermi_gbm_events}")
        
        # Debug: Check error range before filtering
        print(f"\nBefore filtering - Error range: {self.fermi_gbm_data['Error'].min():.2f} to {self.fermi_gbm_data['Error'].max():.2f}")
        print(f"Events with Error > 5: {(self.fermi_gbm_data['Error'] > 5).sum()}")
        
        # Validate coordinates and errors (Error <= 5 only, no Dec filter yet)
        valid_mask = (
            self.fermi_gbm_data['RA'].notna() & 
            self.fermi_gbm_data['DEC'].notna() &
            self.fermi_gbm_data['Error'].notna() &
            (self.fermi_gbm_data['RA'] >= 0) & 
            (self.fermi_gbm_data['RA'] <= 360) &
            (self.fermi_gbm_data['DEC'] >= -90) & 
            (self.fermi_gbm_data['DEC'] <= 90) &
            (self.fermi_gbm_data['Error'] > 0) &
            (self.fermi_gbm_data['Error'] <= 5)
        )
        
        self.fermi_gbm_data = self.fermi_gbm_data[valid_mask].reset_index(drop=True)
        print(f"Valid Fermi GBM events (Error <= 5°): {len(self.fermi_gbm_data)}")
        print(f"Excluded (Error > 5°): {self.total_fermi_gbm_events - len(self.fermi_gbm_data)}")
        
        # Display error statistics
        print(f"\nError statistics (Error <= 5°):")
        print(f"  Min: {self.fermi_gbm_data['Error'].min():.2f}°")
        print(f"  Max: {self.fermi_gbm_data['Error'].max():.2f}°")
        print(f"  Median: {self.fermi_gbm_data['Error'].median():.2f}°")
        print(f"  Mean: {self.fermi_gbm_data['Error'].mean():.2f}°")
        
        return self.fermi_gbm_data
    
    def analyze_tile_coverage(self):
        """
        Analyze tile coverage for each Fermi GBM event.
        """
        print("\n[Phase 2] Analyzing tile coverage...")
        
        # Prepare all coordinates at once
        all_ras = self.fermi_gbm_data['RA'].tolist()
        all_decs = self.fermi_gbm_data['DEC'].tolist()
        all_errors = self.fermi_gbm_data['Error'].tolist()
        
        print(f"Processing {len(all_ras)} events at once...")
        print(f"RA range: {min(all_ras):.2f} to {max(all_ras):.2f}")
        print(f"DEC range: {min(all_decs):.2f} to {max(all_decs):.2f}")
        print(f"Error range: {min(all_errors):.2f} to {max(all_errors):.2f}")
        
        # Filter out problematic coordinates (extreme declinations or huge errors)
        valid_indices = []
        filtered_ras = []
        filtered_decs = []
        filtered_errors = []
        
        for i, (ra, dec, error) in enumerate(zip(all_ras, all_decs, all_errors)):
            # Skip extreme declinations (near poles) or unreasonably large errors
            if abs(dec) > 80 or error > 20:
                print(f"Skipping problematic coordinate: RA={ra:.2f}, DEC={dec:.2f}, Error={error:.2f}")
                continue
            valid_indices.append(i)
            filtered_ras.append(ra)
            filtered_decs.append(dec)
            filtered_errors.append(error)
        
        print(f"Processing {len(filtered_ras)} valid events (skipped {len(all_ras) - len(filtered_ras)} problematic events)...")
        
        try:
            # Process valid events in one call
            self.tiles.find_overlapping_tiles(
                ra=filtered_ras,
                dec=filtered_decs,
                aperture=filtered_errors,
                fraction_overlap_lower=0.2,
                visualize=False
            )
            
            target_table = self.tiles.target_table
            tile_table = self.tiles.tile_table
            
        except Exception as e:
            print(f"Error during tile analysis: {e}")
            import traceback
            traceback.print_exc()
            print("Creating empty results...")
            self.results = pd.DataFrame()
            return self.results
        
        # Now extract results for ALL events (including skipped ones)
        results_list = []
        
        for idx, row in self.fermi_gbm_data.iterrows():
            gcn_id = row['GCN_ID']
            ra = row['RA']
            dec = row['DEC']
            error = row['Error']
            
            # Check if this coordinate was processed
            if idx not in valid_indices:
                # Skipped coordinate - mark as no tiles
                result = {
                    'GCN_ID': gcn_id,
                    'Name': row['Name'],
                    'RA': ra,
                    'DEC': dec,
                    'Error': error,
                    'Discovery_UTC': row['Discovery_UTC'],
                    'Trigger_num': row['Trigger_num'],
                    'n_tiles': 0,
                    'tile_ids': [],
                    'tile_ras': [],
                    'tile_decs': [],
                    'overlap_fractions': [],
                    'distance_to_boundaries': [],
                    'is_within_boundaries': []
                }
                results_list.append(result)
                continue
            
            # Find the remapped index in the filtered lists
            filtered_idx = valid_indices.index(idx)
            
            # Find corresponding entry in target_table
            target_match = target_table[target_table['matched_idx'] == filtered_idx]
            
            if len(target_match) > 0:
                matched_tiles = target_match['matched_tile'][0]
                n_tiles = len(matched_tiles)
                
                # Get detailed tile information
                tile_match = tile_table[tile_table['matched_idx'] == filtered_idx]
                
                if len(tile_match) > 0:
                    tile_ids = tile_match['id'].tolist()
                    tile_ras = tile_match['ra'].tolist()
                    tile_decs = tile_match['dec'].tolist()
                    overlap_areas = tile_match['overlapped_area'].tolist()
                    distance_to_boundaries = tile_match['distance_to_boundary'].tolist()
                    is_within_boundaries = tile_match['is_within_boundary'].tolist()
                else:
                    tile_ids = []
                    tile_ras = []
                    tile_decs = []
                    overlap_areas = []
                    distance_to_boundaries = []
                    is_within_boundaries = []
            else:
                # No match found for this coordinate
                n_tiles = 0
                matched_tiles = []
                tile_ids = []
                tile_ras = []
                tile_decs = []
                overlap_areas = []
                distance_to_boundaries = []
                is_within_boundaries = []
            
            # Store results
            result = {
                'GCN_ID': gcn_id,
                'Name': row['Name'],
                'RA': ra,
                'DEC': dec,
                'Error': error,
                'Discovery_UTC': row['Discovery_UTC'],
                'Trigger_num': row['Trigger_num'],
                'n_tiles': n_tiles,
                'tile_ids': tile_ids,
                'tile_ras': tile_ras,
                'tile_decs': tile_decs,
                'overlap_fractions': overlap_areas,
                'distance_to_boundaries': distance_to_boundaries,
                'is_within_boundaries': is_within_boundaries
            }
            
            results_list.append(result)
        
        self.results = pd.DataFrame(results_list)
        
        # Count Chile-visible events (events with tiles > 0)
        self.total_chile_visible = (self.results['n_tiles'] > 0).sum()
        
        print(f"\nTile coverage analysis complete!")
        print(f"Events analyzed: {len(self.results)}")
        print(f"Events with tiles (Chile-visible): {self.total_chile_visible}")
        print(f"Events without tiles (outside Chile coverage): {(self.results['n_tiles'] == 0).sum()}")
        
        return self.results
    
    def calculate_statistics(self):
        """Calculate comprehensive statistics."""
        print("\n[Phase 3] Calculating statistics...")
        
        # Overall tile statistics
        tile_counts = self.results['n_tiles']
        
        self.statistics['overall'] = {
            'total_events': len(self.results),
            'events_with_tiles': (tile_counts > 0).sum(),
            'events_without_tiles': (tile_counts == 0).sum(),
            'min_tiles': int(tile_counts.min()),
            'max_tiles': int(tile_counts.max()),
            'mean_tiles': float(tile_counts.mean()),
            'median_tiles': float(tile_counts.median()),
            'std_tiles': float(tile_counts.std()),
            'total_tiles_needed': int(tile_counts.sum())
        }
        
        # Error-based categorization (change bins and labels)
        error_bins = [0, 1, 2, 3, 5]
        error_labels = ['<1°', '1-2°', '2-3°', '3-5°'] 
        self.results['error_category'] = pd.cut(
            self.results['Error'], 
            bins=error_bins, 
            labels=error_labels
        )
        
        category_stats = {}
        for category in error_labels:
            mask = self.results['error_category'] == category
            if mask.sum() > 0:
                cat_tiles = self.results[mask]['n_tiles']
                category_stats[category] = {
                    'count': int(mask.sum()),
                    'mean_tiles': float(cat_tiles.mean()),
                    'median_tiles': float(cat_tiles.median()),
                    'min_tiles': int(cat_tiles.min()),
                    'max_tiles': int(cat_tiles.max())
                }
        
        self.statistics['by_error_category'] = category_stats
        
        # Correlation analysis
        correlation = self.results[['Error', 'n_tiles']].corr().iloc[0, 1]
        self.statistics['correlation'] = {
            'error_vs_tiles': float(correlation)
        }
        
        # Workload estimation
        mean_tiles_per_event = tile_counts.mean()
        self.statistics['workload'] = {
            'mean_tiles_per_event': float(mean_tiles_per_event),
            'total_tiles_all_events': int(tile_counts.sum())
        }
        
        # Percentiles
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        percentile_values = np.percentile(tile_counts, percentiles)
        self.statistics['percentiles'] = {
            f'p{p}': float(v) for p, v in zip(percentiles, percentile_values)
        }
        
        print("Statistics calculated successfully!")
        
        return self.statistics
    
    def create_visualizations(self):
        """Create comprehensive visualizations."""
        print("\n[Phase 4] Creating visualizations...")
        
        sns.set_style("whitegrid")
        
        # 1. Histogram of tile counts
        self._plot_tile_histogram()
        
        # 2. Scatter plot: Error vs. Tiles
        self._plot_error_vs_tiles()
        
        # 3. Box plot by error category
        self._plot_boxplot_by_category()
        
        # 4. Cumulative distribution
        self._plot_cumulative_distribution()
        
        # 5. Sample tile visualizations
        self._plot_sample_tiles()

        # 6. Practical visualizations
        self._plot_cumulative_distribution_practical()
        self._plot_practical_histogram()
        self._create_observability_matrix()
        
        print("All visualizations created!")
    
    def _plot_tile_histogram(self):
        """Plot histogram of tile counts."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        tile_counts = self.results['n_tiles']
        ax.hist(tile_counts, bins=range(0, int(tile_counts.max()) + 2), 
                edgecolor='black', alpha=0.7)
        
        ax.axvline(tile_counts.mean(), color='red', linestyle='--', 
                   linewidth=2, label=f'Mean: {tile_counts.mean():.1f}')
        ax.axvline(tile_counts.median(), color='blue', linestyle='--', 
                   linewidth=2, label=f'Median: {tile_counts.median():.1f}')
        
        ax.set_xlabel('Number of Tiles', fontsize=12)
        ax.set_ylabel('Number of Events', fontsize=12)
        ax.set_title('Distribution of Tile Counts for Fermi GBM Events', fontsize=14)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'tile_count_histogram.png', dpi=300)
        plt.close()
    
    def _plot_error_vs_tiles(self):
        """Plot scatter of error vs. tile count with trend line."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        x = self.results['Error']
        y = self.results['n_tiles']
        
        ax.scatter(x, y, alpha=0.5, s=50)
        
        # Fit and plot trend line
        z = np.polyfit(x, y, 2)
        p = np.poly1d(z)
        x_trend = np.linspace(x.min(), x.max(), 100)
        ax.plot(x_trend, p(x_trend), 'r-', linewidth=2, 
                label=f'Trend (R={self.statistics["correlation"]["error_vs_tiles"]:.3f})')
        
        ax.set_xlabel('Position Error (degrees)', fontsize=12)
        ax.set_ylabel('Number of Tiles', fontsize=12)
        ax.set_title('Position Error vs. Number of Tiles Required', fontsize=14)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'error_vs_tiles_scatter.png', dpi=300)
        plt.close()
    
    def _plot_boxplot_by_category(self):
        """Plot box plot of tile counts by error category."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Updated categories for Error <= 5°
        categories = ['<1°', '1-2°', '2-3°', '3-5°']
        
        data_for_plot = [
            self.results[self.results['error_category'] == cat]['n_tiles'].values
            for cat in categories
            if (self.results['error_category'] == cat).sum() > 0
        ]
        
        labels = [
            cat for cat in categories
            if (self.results['error_category'] == cat).sum() > 0
        ]
        
        bp = ax.boxplot(data_for_plot, tick_labels=labels, patch_artist=True)
        
        for patch in bp['boxes']:
            patch.set_facecolor('lightblue')
        
        ax.set_xlabel('Error Category', fontsize=12)
        ax.set_ylabel('Number of Tiles', fontsize=12)
        ax.set_title('Tile Count Distribution by Error Category (Error ≤ 5°)', fontsize=14)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'tiles_by_error_category.png', dpi=300)
        plt.close()
    
    def _plot_cumulative_distribution(self):
        """Plot cumulative distribution of tile counts."""
        fig, ax = plt.subplots(figsize=(10, 6))
        
        tile_counts = np.sort(self.results['n_tiles'])
        cumulative = np.arange(1, len(tile_counts) + 1) / len(tile_counts)
        
        ax.plot(tile_counts, cumulative, linewidth=2)
        ax.axhline(0.5, color='red', linestyle='--', alpha=0.5, 
                   label='50th percentile')
        ax.axhline(0.9, color='orange', linestyle='--', alpha=0.5, 
                   label='90th percentile')
        
        ax.set_xlabel('Number of Tiles', fontsize=12)
        ax.set_ylabel('Cumulative Fraction of Events', fontsize=12)
        ax.set_title('Cumulative Distribution: Fraction of Events vs. Tile Count', 
                    fontsize=14)
        ax.legend(fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'cumulative_distribution.png', dpi=300)
        plt.close()
    
    def _plot_sample_tiles(self):
        """Visualize sample events with different error sizes."""
        print("  Creating sample tile visualizations...")
        
        # Select representative samples from each error category
        samples = []
        for category in ['<1°', '1-2°', '2-3°', '3-5°']:
            cat_data = self.results[self.results['error_category'] == category]
            if len(cat_data) > 0:
                sample = cat_data.iloc[len(cat_data)//2]  # Middle sample
                samples.append(sample)
        
        if len(samples) == 0:
            print("  No samples available for visualization")
            return
        
        # Visualize each sample
        for i, sample in enumerate(samples):
            try:
                fig_path = self.tiles.find_overlapping_tiles(
                    ra=sample['RA'],
                    dec=sample['DEC'],
                    aperture=sample['Error'],
                    fraction_overlap_lower=0.2,
                    visualize=True,
                    visualize_ncols=1,
                    visualize_savepath=str(self.output_dir),
                    show=False
                )
                
                # Rename the file
                if fig_path and isinstance(fig_path, str):
                    new_name = self.output_dir / f'sample_tiles_{i+1}_error_{sample["Error"]:.1f}deg.png'
                    Path(fig_path).rename(new_name)
                    
            except Exception as e:
                print(f"  Warning: Could not visualize sample {i+1}: {e}")

    def _plot_cumulative_distribution_practical(self):
        """Plot cumulative distribution focused on practical tile ranges."""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Only include Chile-visible events (n_tiles > 0)
        chile_visible = self.results[self.results['n_tiles'] > 0]
        tile_counts = np.sort(chile_visible['n_tiles'])
        cumulative = np.arange(1, len(tile_counts) + 1) / self.total_chile_visible
        
        # Full range
        ax1.plot(tile_counts, cumulative, linewidth=2)
        ax1.axhline(0.5, color='red', linestyle='--', alpha=0.5, label='50%')
        ax1.axhline(0.75, color='orange', linestyle='--', alpha=0.5, label='75%')
        ax1.axhline(0.9, color='green', linestyle='--', alpha=0.5, label='90%')
        ax1.set_xlabel('Number of Tiles', fontsize=12)
        ax1.set_ylabel('Fraction of Chile-Visible Events', fontsize=12)
        ax1.set_title('Cumulative Coverage (Full Range)', fontsize=14)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Zoomed to practical range
        max_practical = min(50, tile_counts.max())
        mask = tile_counts <= max_practical
        ax2.plot(tile_counts[mask], cumulative[mask], linewidth=2, color='blue')
        ax2.axhline(0.5, color='red', linestyle='--', alpha=0.5, label='50%')
        ax2.axhline(0.75, color='orange', linestyle='--', alpha=0.5, label='75%')
        ax2.axhline(0.9, color='green', linestyle='--', alpha=0.5, label='90%')
        
        # Mark key thresholds
        for threshold in [5, 10, 20, 30]:
            if threshold <= max_practical:
                frac = (tile_counts <= threshold).sum() / self.total_chile_visible
                ax2.axvline(threshold, color='gray', linestyle=':', alpha=0.5)
                ax2.text(threshold, frac + 0.02, f'{threshold} tiles\n{frac*100:.0f}%', 
                        ha='center', fontsize=9)
        
        ax2.set_xlabel('Number of Tiles', fontsize=12)
        ax2.set_ylabel('Fraction of Chile-Visible Events', fontsize=12)
        ax2.set_title(f'Cumulative Coverage (Practical Range: 0-{max_practical} tiles)', fontsize=14)
        ax2.set_xlim(0, max_practical)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add text annotation
        outside_chile = (self.results['n_tiles'] == 0).sum()
        fig.text(0.5, 0.02, 
                f'Note: {outside_chile} events with Error ≤ 5° are outside Chile tile coverage',
                ha='center', fontsize=9, style='italic')
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.08)
        plt.savefig(self.output_dir / 'cumulative_coverage_practical.png', dpi=300)
        plt.close()

    def _plot_practical_histogram(self):
        """Plot histogram focused on practical tile ranges."""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Only include Chile-visible events
        chile_visible = self.results[self.results['n_tiles'] > 0]
        tile_counts = chile_visible['n_tiles']
        max_tiles = tile_counts.max()
        
        # Adjust bins based on actual data range
        if max_tiles <= 30:
            bins = [0, 3, 5, 10, 20, 30]
            labels = ['0-3', '3-5', '5-10', '10-20', '20-30']
            colors = ['darkgreen', 'green', 'lightgreen', 'yellow', 'orange']
        elif max_tiles <= 50:
            bins = [0, 3, 5, 10, 20, 30, 50]
            labels = ['0-3', '3-5', '5-10', '10-20', '20-30', '30-50']
            colors = ['darkgreen', 'green', 'lightgreen', 'yellow', 'orange', 'red']
        elif max_tiles <= 100:
            bins = [0, 3, 5, 10, 20, 30, 50, 100]
            labels = ['0-3', '3-5', '5-10', '10-20', '20-30', '30-50', '50-100']
            colors = ['darkgreen', 'green', 'lightgreen', 'yellow', 'orange', 'red', 'darkred']
        else:
            bins = [0, 3, 5, 10, 20, 30, 50, 100, np.inf]
            labels = ['0-3', '3-5', '5-10', '10-20', '20-30', '30-50', '50-100', '100+']
            colors = ['darkgreen', 'green', 'lightgreen', 'yellow', 'orange', 'red', 'darkred', 'black']
        
        binned = pd.cut(tile_counts, bins=bins, labels=labels, include_lowest=True)
        counts = binned.value_counts().sort_index()
        
        bars = ax.bar(range(len(counts)), counts.values, 
                    color=colors[:len(counts)],
                    alpha=0.7, edgecolor='black')
        
        ax.set_xticks(range(len(counts)))
        ax.set_xticklabels(labels)
        ax.set_xlabel('Number of Tiles', fontsize=12)
        ax.set_ylabel('Number of Events', fontsize=12)
        ax.set_title(f'Distribution by Tile Count Range (Max: {max_tiles} tiles)', fontsize=14)
        
        # Add percentages on bars (relative to Chile-visible events)
        for i, (bar, count) in enumerate(zip(bars, counts.values)):
            height = bar.get_height()
            percentage = count / self.total_chile_visible * 100
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{count}\n({percentage:.1f}%)',
                    ha='center', va='bottom', fontsize=10)
        
        # Add text about event breakdown
        outside_chile = (self.results['n_tiles'] == 0).sum()
        error_gt_5 = self.total_fermi_gbm_events - len(self.results)
        
        ax.text(0.98, 0.98, 
                f'Total Fermi GBM: {self.total_fermi_gbm_events}\n'
                f'Error ≤ 5°: {len(self.results)}\n'
                f'  - Chile-visible: {self.total_chile_visible}\n'
                f'  - Outside Chile: {outside_chile}\n'
                f'Error > 5°: {error_gt_5}',
                transform=ax.transAxes,
                fontsize=10,
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(self.output_dir / 'practical_tile_histogram.png', dpi=300)
        plt.close()

    def _create_observability_matrix(self):
        """Create a matrix showing coverage vs tile limit."""
        tile_limits = [3, 5, 10, 15, 20, 30, 50, 75, 100]
        
        results = []
        for limit in tile_limits:
            observable = (self.results['n_tiles'] <= limit) & (self.results['n_tiles'] > 0)
            n_observable = observable.sum()
            
            # Calculate coverage relative to Chile-visible events
            frac_observable = n_observable / self.total_chile_visible * 100
            
            # Calculate average workload
            workload_events = self.results[self.results['n_tiles'] <= limit]
            avg_tiles = workload_events['n_tiles'].mean() if len(workload_events) > 0 else 0
            
            # Missed = Chile-visible - observable
            missed = self.total_chile_visible - n_observable
            
            results.append({
                'Tile Limit': limit,
                'Observable': n_observable,
                'Coverage (%)': f'{frac_observable:.1f}%',
                'Avg Tiles': f'{avg_tiles:.1f}',
                'Missed': missed
            })
        
        df = pd.DataFrame(results)
        
        # Save as table
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.axis('tight')
        ax.axis('off')
        
        table = ax.table(cellText=df.values, colLabels=df.columns,
                        cellLoc='center', loc='center',
                        colWidths=[0.15, 0.18, 0.18, 0.18, 0.15])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        # Color code by coverage
        for i in range(1, len(results) + 1):
            coverage = float(results[i-1]['Coverage (%)'].rstrip('%'))
            if coverage >= 75:
                color = '#90EE90'
            elif coverage >= 50:
                color = '#FFD700'
            else:
                color = '#FFB6C6'
            table[(i, 2)].set_facecolor(color)
        
        # Add note about calculation
        outside_chile = (self.results['n_tiles'] == 0).sum()
        note_text = (f'Note: Coverage calculated as percentage of Chile-visible events (N={self.total_chile_visible})\n'
                    f'Total Error ≤ 5° events: {len(self.results)} (includes {outside_chile} outside Chile coverage)')
        
        plt.figtext(0.5, 0.05, note_text, 
                    ha='center', fontsize=9, style='italic',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
        
        plt.title('Observability Matrix: Coverage vs. Tile Limit', 
                fontsize=14, pad=20)
        plt.subplots_adjust(bottom=0.15)
        plt.savefig(self.output_dir / 'observability_matrix.png', 
                    dpi=300, bbox_inches='tight')
        plt.close()
        
        # Also save as CSV
        df.to_csv(self.output_dir / 'observability_matrix.csv', index=False)
        print(f"  Saved: observability_matrix.png and observability_matrix.csv")
        
        return df

    def save_results(self):
        """Save all results to files."""
        print("\n[Phase 5] Saving results...")
        
        # Save detailed results CSV
        results_save = self.results.copy()
        
        # Convert list columns to string for CSV
        for col in ['tile_ids', 'tile_ras', 'tile_decs', 'overlap_fractions', 
                    'distance_to_boundaries', 'is_within_boundaries']:
            results_save[col] = results_save[col].apply(lambda x: str(x))
        
        results_save.to_csv(
            self.output_dir / 'fermi_gbm_tile_coverage.csv', 
            index=False
        )
        print(f"  Saved: fermi_gbm_tile_coverage.csv")
        
        # Save statistics JSON
        def convert_to_serializable(obj):
            """Convert numpy/pandas types to native Python types"""
            if isinstance(obj, dict):
                return {key: convert_to_serializable(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_serializable(item) for item in obj]
            elif isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            else:
                return obj

        with open(self.output_dir / 'tile_statistics.json', 'w') as f:
            json.dump(convert_to_serializable(self.statistics), f, indent=2)
        print(f"  Saved: tile_statistics.json")
        
        # Generate text report
        self._generate_text_report()
    
    def _generate_text_report(self):
        """Generate comprehensive text report."""
        report_path = self.output_dir / 'tile_analysis_report.txt'
        
        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("FERMI GBM TILE COVERAGE ANALYSIS REPORT\n")
            f.write("7DT Telescope Tile Requirements\n")
            f.write("=" * 80 + "\n")
            f.write("NOTE: Analysis limited to high-precision events (Error ≤ 5°)\n")  # Added
            f.write("=" * 80 + "\n\n")
            
            # Overall statistics
            f.write("-" * 80 + "\n")
            f.write("OVERALL STATISTICS\n")
            f.write("-" * 80 + "\n")
            overall = self.statistics['overall']
            f.write(f"Total Fermi GBM events analyzed: {overall['total_events']}\n")
            f.write(f"Events with tile coverage: {overall['events_with_tiles']} "
                   f"({overall['events_with_tiles']/overall['total_events']*100:.1f}%)\n")
            f.write(f"Events without tile coverage: {overall['events_without_tiles']} "
                   f"({overall['events_without_tiles']/overall['total_events']*100:.1f}%)\n\n")
            
            f.write(f"Tile count per event:\n")
            f.write(f"  Min: {overall['min_tiles']}\n")
            f.write(f"  Max: {overall['max_tiles']}\n")
            f.write(f"  Mean: {overall['mean_tiles']:.2f}\n")
            f.write(f"  Median: {overall['median_tiles']:.1f}\n")
            f.write(f"  Std: {overall['std_tiles']:.2f}\n\n")
            
            f.write(f"Total tiles needed (sum): {overall['total_tiles_needed']}\n\n")
            
            # By error category
            f.write("-" * 80 + "\n")
            f.write("STATISTICS BY ERROR CATEGORY\n")
            f.write("-" * 80 + "\n")
            for category, stats in self.statistics['by_error_category'].items():
                f.write(f"\n{category}:\n")
                f.write(f"  Number of events: {stats['count']}\n")
                f.write(f"  Mean tiles: {stats['mean_tiles']:.2f}\n")
                f.write(f"  Median tiles: {stats['median_tiles']:.1f}\n")
                f.write(f"  Range: {stats['min_tiles']} - {stats['max_tiles']}\n")
            
            # Percentiles
            f.write("\n" + "-" * 80 + "\n")
            f.write("TILE COUNT PERCENTILES\n")
            f.write("-" * 80 + "\n")
            for percentile, value in self.statistics['percentiles'].items():
                f.write(f"  {percentile}: {value:.1f}\n")
            
            # Correlation
            f.write("\n" + "-" * 80 + "\n")
            f.write("CORRELATION ANALYSIS\n")
            f.write("-" * 80 + "\n")
            corr = self.statistics['correlation']['error_vs_tiles']
            f.write(f"Error vs. Tiles correlation: {corr:.3f}\n")
            
            # Workload estimation
            f.write("\n" + "-" * 80 + "\n")
            f.write("WORKLOAD ESTIMATION\n")
            f.write("-" * 80 + "\n")
            workload = self.statistics['workload']
            f.write(f"Mean tiles per event: {workload['mean_tiles_per_event']:.2f}\n")
            f.write(f"Total tiles for all events: {workload['total_tiles_all_events']}\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        print(f"  Saved: tile_analysis_report.txt")
    
    def run_analysis(self):
        """Run complete tile coverage analysis."""
        print("\n" + "=" * 70)
        print("STARTING FERMI GBM TILE COVERAGE ANALYSIS")
        print("=" * 70)
        
        # Phase 1: Load data
        self.load_fermi_gbm_data()
        
        # Phase 2: Analyze tile coverage
        self.analyze_tile_coverage()

        # Check if analysis succeeded
        if self.results is None or len(self.results) == 0:
            print("\n" + "=" * 70)
            print("ANALYSIS FAILED - No results generated")
            print("=" * 70)
            print("The 'float division by zero' error occurred in tiles.py")
            print("This is likely due to extreme declinations or polygon area calculations")
            return

        # Phase 3: Calculate statistics
        self.calculate_statistics()
        
        # Phase 4: Create visualizations
        self.create_visualizations()
        
        # Phase 5: Save results
        self.save_results()
        
        print("\n" + "=" * 70)
        print("ANALYSIS COMPLETE")
        print("=" * 70)
        print(f"\nAll results saved to: {self.output_dir}/")
        print("\nGenerated files:")
        print("  - fermi_gbm_tile_coverage.csv (detailed results)")
        print("  - tile_statistics.json (summary statistics)")
        print("  - tile_analysis_report.txt (comprehensive report)")
        print("  - tile_count_histogram.png")
        print("  - error_vs_tiles_scatter.png")
        print("  - tiles_by_error_category.png")
        print("  - cumulative_distribution.png")
        print("  - sample_tiles_*.png (example visualizations)")


if __name__ == "__main__":
    # Configuration
    DATA_PATH = "gcn_notices_cleaned.csv"
    OUTPUT_DIR = "fermi_gbm_tile_analysis"
    
    # Check if data file exists
    if not Path(DATA_PATH).exists():
        print(f"ERROR: Data file not found: {DATA_PATH}")
        sys.exit(1)
    
    # Run analysis
    print("Note: Using default tile path from REFDATA_DIR")
    analyzer = FermiGBMTileAnalyzer(DATA_PATH, OUTPUT_DIR)
    analyzer.run_analysis()