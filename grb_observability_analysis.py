"""
GRB Observability Analysis for Chilean Observatory
Analyzes GRB events to determine feasibility of automatic observations
Version 2: Deduplicated data only with comprehensive analysis
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from pathlib import Path
from astropy.time import Time
from astropy import units as u
from astropy.coordinates import SkyCoord
import warnings
warnings.filterwarnings('ignore')

import sys
sys.path.append(str(Path(__file__).parent))
from supy.supy.observer import Observer, VisibilityPlotter


class GRBObservabilityAnalyzer:
    """
    Comprehensive GRB observability analyzer for automatic observation decisions.
    Uses deduplicated data only for accurate workload estimation.
    """
    
    def __init__(self, data_path, output_dir="grb_analysis_results"):
        """
        Initialize analyzer.
        
        Parameters
        ----------
        data_path : str
            Path to GRB CSV file
        output_dir : str
            Directory for output plots and reports
        """
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.observer = Observer()
        self.plotter = VisibilityPlotter(self.observer)
        
        self.data = None
        self.data_deduplicated = None
        self.results_deduplicated = None
        self.deduplication_info = {}
        self.statistics = {}
        
        print(f"GRB Observability Analyzer initialized")
        print(f"Observatory: {self.observer.name}")
        print(f"Location: {self.observer.latitude:.2f}, {self.observer.longitude:.2f}")
        print(f"Output directory: {self.output_dir}")
        print("-" * 70)
    
    def load_data(self):
        """Load and preprocess GRB data."""
        print("\n[Phase 1] Loading GRB data...")
        
        self.data = pd.read_csv(self.data_path)
        print(f"Loaded {len(self.data)} GRB events")
        
        # Parse timestamps with timezone handling
        self.data['Discovery_UTC'] = pd.to_datetime(self.data['Discovery_UTC'], utc=True, format='mixed')
        self.data['Notice_date'] = pd.to_datetime(self.data['Notice_date'], utc=True, format='mixed')
        
        # Calculate response time
        self.data['Response_time_hours'] = (
            (self.data['Notice_date'] - self.data['Discovery_UTC']).dt.total_seconds() / 3600
        )
        
        # Remove invalid coordinates
        valid_mask = (
            self.data['RA'].notna() & 
            self.data['DEC'].notna() &
            (self.data['RA'] >= 0) & (self.data['RA'] <= 360) &
            (self.data['DEC'] >= -90) & (self.data['DEC'] <= 90)
        )
        self.data = self.data[valid_mask].reset_index(drop=True)
        print(f"Valid coordinates: {len(self.data)} events")
        
        return self.data
    
    def deduplicate_events(self):
        """
        Remove duplicate detections of the same GRB.
        Groups events by time (±3 hours) and position (< 5 degrees).
        Keeps the most accurate localization from each group.
        """
        print("\n[Deduplication] Identifying duplicate detections...")
        
        # Facility priority (higher = better localization)
        facility_priority = {
            'SwiftXRT': 6,
            'EinsteinProbe': 5,
            'SwiftBAT': 4,
            'FermiLAT': 3,
            'FermiGBM': 2,
            'IceCubeCASCADE': 1,
            'IceCubeBRONZE': 1,
        }
        
        # Default priority for unknown facilities
        def get_priority(facility):
            for key in facility_priority:
                if key in facility:
                    return facility_priority[key]
            return 0
        
        self.data['priority'] = self.data['Facility'].apply(get_priority)
        
        # Sort by time
        data_sorted = self.data.sort_values('Discovery_UTC').reset_index(drop=True)
        
        # Track which events to keep
        keep_indices = []
        grouped_events = []
        used = set()
        
        for i in range(len(data_sorted)):
            if i in used:
                continue
            
            current = data_sorted.iloc[i]
            current_coord = SkyCoord(ra=current['RA']*u.deg, dec=current['DEC']*u.deg)
            current_time = current['Discovery_UTC']
            
            # Find all events within time and position window
            group = [i]
            
            for j in range(i+1, len(data_sorted)):
                if j in used:
                    continue
                
                candidate = data_sorted.iloc[j]
                candidate_time = candidate['Discovery_UTC']
                
                # Check time window (±3 hours)
                time_diff = abs((candidate_time - current_time).total_seconds() / 3600)
                if time_diff > 3:
                    break  # No more candidates in time window
                
                # Check position
                candidate_coord = SkyCoord(ra=candidate['RA']*u.deg, dec=candidate['DEC']*u.deg)
                separation = current_coord.separation(candidate_coord).deg
                
                if separation < 5:
                    group.append(j)
                    used.add(j)
            
            # From group, select the one with best localization
            group_data = data_sorted.iloc[group]
            
            # Priority: lowest error, then highest facility priority
            best_idx = group_data.sort_values(
                ['Error', 'priority'], 
                ascending=[True, False]
            ).index[0]
            
            keep_indices.append(best_idx)
            grouped_events.append({
                'kept_index': best_idx,
                'group_size': len(group),
                'group_indices': group
            })
            used.add(i)
        
        # Create deduplicated dataset
        self.data_deduplicated = data_sorted.loc[keep_indices].reset_index(drop=True)
        self.deduplication_info = {
            'original_count': len(self.data),
            'deduplicated_count': len(self.data_deduplicated),
            'removed_count': len(self.data) - len(self.data_deduplicated),
            'duplication_rate': (len(self.data) - len(self.data_deduplicated)) / len(self.data),
            'grouped_events': grouped_events
        }
        
        # Calculate duplication rate by facility
        duplicates = data_sorted[~data_sorted.index.isin(keep_indices)]
        if len(duplicates) > 0:
            dup_by_facility = duplicates['Facility'].value_counts()
            self.deduplication_info['duplicates_by_facility'] = dup_by_facility.to_dict()
        
        print(f"  Original events: {self.deduplication_info['original_count']}")
        print(f"  Unique events: {self.deduplication_info['deduplicated_count']}")
        print(f"  Duplicates removed: {self.deduplication_info['removed_count']} "
              f"({self.deduplication_info['duplication_rate']*100:.1f}%)")
        
        # Show grouping examples
        multi_detections = [g for g in grouped_events if g['group_size'] > 1]
        if multi_detections:
            print(f"  Events with multiple detections: {len(multi_detections)}")
            print(f"  Largest group: {max(g['group_size'] for g in multi_detections)} detections")
        
        return self.data_deduplicated
    
    def temporal_statistics(self):
        """Analyze temporal distribution of GRB events (deduplicated)."""
        print("\n[Phase 1] Computing temporal statistics...")
        
        stats = {}
        data_source = self.data_deduplicated
        
        # Date range
        date_range = (data_source['Discovery_UTC'].max() - 
                     data_source['Discovery_UTC'].min()).days
        stats['date_range_days'] = date_range
        stats['start_date'] = data_source['Discovery_UTC'].min()
        stats['end_date'] = data_source['Discovery_UTC'].max()
        
        # Event rates
        stats['total_events'] = len(data_source)
        stats['events_per_day_mean'] = len(data_source) / date_range
        
        # Daily grouping
        daily_counts = data_source.groupby(
            data_source['Discovery_UTC'].dt.date
        ).size()
        stats['events_per_day_median'] = daily_counts.median()
        stats['events_per_day_std'] = daily_counts.std()
        stats['max_events_per_day'] = daily_counts.max()
        
        # Time gaps
        sorted_times = data_source['Discovery_UTC'].sort_values()
        time_gaps = sorted_times.diff().dt.total_seconds() / 3600  # hours
        stats['median_gap_hours'] = time_gaps.median()
        stats['min_gap_hours'] = time_gaps.min()
        
        # Facility breakdown
        facility_counts = data_source['Facility'].value_counts()
        stats['facility_distribution'] = facility_counts.to_dict()
        
        # Response time
        stats['median_response_hours'] = data_source['Response_time_hours'].median()
        stats['mean_response_hours'] = data_source['Response_time_hours'].mean()
        
        self.statistics['temporal'] = stats
        
        print(f"  Date range: {stats['start_date'].date()} to {stats['end_date'].date()}")
        print(f"  Events per day: {stats['events_per_day_mean']:.2f} (mean), "
              f"{stats['events_per_day_median']:.1f} (median)")
        print(f"  Median gap between events: {stats['median_gap_hours']:.1f} hours")
        print(f"  Response time: {stats['median_response_hours']:.2f} hours (median)")
        
        return stats
    
    def spatial_analysis(self):
        """Analyze spatial distribution of GRB events (deduplicated)."""
        print("\n[Phase 1] Analyzing spatial distribution...")
        
        stats = {}
        data_source = self.data_deduplicated
        
        # Hemisphere distribution
        northern = (data_source['DEC'] > 0).sum()
        southern = (data_source['DEC'] <= 0).sum()
        stats['northern_hemisphere'] = northern
        stats['southern_hemisphere'] = southern
        stats['southern_fraction'] = southern / len(data_source)
        
        # Position error statistics
        stats['median_error_deg'] = data_source['Error'].median()
        stats['mean_error_deg'] = data_source['Error'].mean()
        
        # Error by facility
        error_by_facility = data_source.groupby('Facility')['Error'].median().to_dict()
        stats['error_by_facility'] = error_by_facility
        
        self.statistics['spatial'] = stats
        
        print(f"  Northern hemisphere: {northern} ({northern/len(data_source)*100:.1f}%)")
        print(f"  Southern hemisphere: {southern} ({southern/len(data_source)*100:.1f}%)")
        print(f"  Median position error: {stats['median_error_deg']:.2f}°")
        
        return stats
    
    def observability_analysis(self):
        """
        Analyze observability of all GRB events (deduplicated).
        Uses current night only since GRBs fade in 1-3 hours.
        """
        print("\n[Phase 2] Analyzing observability...")
        print("  Constraints: altitude > 30°, moon separation > 30°, astronomical night")
        print("  Time window: Current night only (GRBs fade in 1-3 hours)")
        
        results = []
        data_source = self.data_deduplicated
        
        for idx, row in data_source.iterrows():
            if (idx + 1) % 50 == 0:
                print(f"  Processing: {idx+1}/{len(data_source)}")
            
            ra = row['RA']
            dec = row['DEC']
            discovery_time = Time(row['Discovery_UTC'])
            
            try:
                # Calculate visibility at discovery time
                visibility_result, data_dict = self.plotter.staralt.calculate_visibility(
                    ra, dec, time=discovery_time,
                    min_altitude=30,
                    min_moon_separation=30
                )
                
                result_entry = {
                    'GCN_ID': row['GCN_ID'],
                    'RA': ra,
                    'DEC': dec,
                    'Discovery_UTC': row['Discovery_UTC'],
                    'Facility': row['Facility'],
                    'Error': row['Error'],
                    'is_observable': visibility_result.is_observable,
                    'when': visibility_result.when,
                    'status': visibility_result.status,
                    'reason': visibility_result.reason,
                    'window_duration_hours': None,
                    'max_altitude': None,
                    'time_to_window_hours': None,
                    'urgency': None
                }
                
                if visibility_result.window:
                    result_entry['window_duration_hours'] = visibility_result.window.duration_hours
                    result_entry['max_altitude'] = visibility_result.window.max_altitude
                    
                    if visibility_result.when == "now":
                        result_entry['time_to_window_hours'] = 0
                        remaining = visibility_result.window.time_remaining(discovery_time)
                        if remaining < 0.5:
                            result_entry['urgency'] = 'critical'
                        elif remaining < 1:
                            result_entry['urgency'] = 'high'
                        elif remaining < 2:
                            result_entry['urgency'] = 'medium'
                        else:
                            result_entry['urgency'] = 'low'
                    
                    elif visibility_result.when == "later":
                        wait_time = visibility_result.window.time_until_start(discovery_time)
                        result_entry['time_to_window_hours'] = wait_time
                        if wait_time < 0.5:
                            result_entry['urgency'] = 'high'
                        elif wait_time < 1:
                            result_entry['urgency'] = 'medium'
                        else:
                            result_entry['urgency'] = 'low'
                
                results.append(result_entry)
                
            except Exception as e:
                print(f"  Warning: Failed to process {row['GCN_ID']}: {e}")
                results.append({
                    'GCN_ID': row['GCN_ID'],
                    'RA': ra,
                    'DEC': dec,
                    'Discovery_UTC': row['Discovery_UTC'],
                    'Facility': row['Facility'],
                    'Error': row['Error'],
                    'is_observable': False,
                    'status': 'ERROR',
                    'reason': str(e)
                })
        
        self.results_deduplicated = pd.DataFrame(results)
        
        # Calculate observability statistics
        obs_stats = {}
        obs_stats['total_analyzed'] = len(self.results_deduplicated)
        obs_stats['observable'] = self.results_deduplicated['is_observable'].sum()
        obs_stats['observable_fraction'] = obs_stats['observable'] / obs_stats['total_analyzed']
        obs_stats['observable_now'] = (self.results_deduplicated['when'] == 'now').sum()
        obs_stats['observable_later'] = (self.results_deduplicated['when'] == 'later').sum()
        
        observable = self.results_deduplicated[self.results_deduplicated['is_observable']]
        if len(observable) > 0:
            obs_stats['mean_window_duration'] = observable['window_duration_hours'].mean()
            obs_stats['median_window_duration'] = observable['window_duration_hours'].median()
            obs_stats['mean_max_altitude'] = observable['max_altitude'].mean()
            
            # Urgency distribution
            urgency_counts = observable['urgency'].value_counts().to_dict()
            obs_stats['urgency_distribution'] = urgency_counts
        
        # Constraint failure analysis
        not_observable = self.results_deduplicated[~self.results_deduplicated['is_observable']]
        if len(not_observable) > 0:
            failure_reasons = not_observable['reason'].value_counts().to_dict()
            obs_stats['failure_reasons'] = failure_reasons
        
        self.statistics['observability'] = obs_stats
        
        print(f"\n  Results:")
        print(f"    Observable: {obs_stats['observable']} / {obs_stats['total_analyzed']} "
              f"({obs_stats['observable_fraction']*100:.1f}%)")
        print(f"    Observable NOW: {obs_stats['observable_now']}")
        print(f"    Observable LATER: {obs_stats['observable_later']}")
        
        if len(observable) > 0:
            print(f"    Average window duration: {obs_stats['mean_window_duration']:.2f} hours")
            print(f"    Average max altitude: {obs_stats['mean_max_altitude']:.1f}°")
        
        return obs_stats
    
    def decision_framework(self):
        """
        Establish decision framework for automatic observations.
        """
        print("\n[Phase 3] Building decision framework...")
        
        framework = {}
        
        # Workload estimation
        observable_events = self.results_deduplicated[self.results_deduplicated['is_observable']].copy()
        observable_events['date'] = pd.to_datetime(
            observable_events['Discovery_UTC']
        ).dt.date
        
        daily_observable = observable_events.groupby('date').size()
        
        framework['events_per_night_mean'] = daily_observable.mean()
        framework['events_per_night_median'] = daily_observable.median()
        framework['events_per_night_max'] = daily_observable.max()
        framework['events_per_night_std'] = daily_observable.std()
        
        # Nights distribution
        night_distribution = daily_observable.value_counts().sort_index().to_dict()
        framework['night_distribution'] = night_distribution
        
        # Response timing
        observable_now = self.results_deduplicated[self.results_deduplicated['when'] == 'now']
        observable_later = self.results_deduplicated[self.results_deduplicated['when'] == 'later']
        
        framework['immediate_response_fraction'] = len(observable_now) / len(observable_events) if len(observable_events) > 0 else 0
        
        if len(observable_later) > 0:
            wait_times = observable_later['time_to_window_hours'].dropna()
            framework['mean_wait_time'] = wait_times.mean()
            framework['median_wait_time'] = wait_times.median()
        
        # Priority scoring
        if len(observable_events) > 0:
            observable_events['priority_score'] = (
                100 / (1 + observable_events['Error']) * 
                (observable_events['max_altitude'] / 90)
            )
            framework['priority_score_stats'] = {
                'mean': observable_events['priority_score'].mean(),
                'median': observable_events['priority_score'].median(),
                'std': observable_events['priority_score'].std()
            }
        
        self.statistics['decision_framework'] = framework
        
        print(f"  Observable events per night: {framework['events_per_night_mean']:.2f} (mean), "
              f"{framework['events_per_night_median']:.1f} (median)")
        print(f"  Max events in a single night: {framework['events_per_night_max']}")
        print(f"  Immediate response needed: {framework['immediate_response_fraction']*100:.1f}%")
        
        return framework
    
    def generate_recommendation(self):
        """Generate automatic observation recommendation."""
        print("\n[Phase 4] Generating recommendation...")
        
        obs_stats = self.statistics['observability']
        framework = self.statistics['decision_framework']
        
        recommendation = {}
        
        # Determine feasibility
        avg_per_night = framework['events_per_night_mean']
        max_per_night = framework['events_per_night_max']
        obs_fraction = obs_stats['observable_fraction']
        
        if avg_per_night < 1 and max_per_night <= 3:
            recommendation['feasibility'] = 'HIGH'
            recommendation['reason'] = (
                f"Low event rate ({avg_per_night:.1f} per night on average) "
                f"makes automatic observations feasible"
            )
        elif avg_per_night < 2 and max_per_night <= 5:
            recommendation['feasibility'] = 'MODERATE'
            recommendation['reason'] = (
                f"Moderate event rate ({avg_per_night:.1f} per night) "
                f"requires careful scheduling but automation is possible"
            )
        else:
            recommendation['feasibility'] = 'LOW'
            recommendation['reason'] = (
                f"High event rate ({avg_per_night:.1f} per night, max {max_per_night}) "
                f"may overwhelm automatic observation system"
            )
        
        # Suggested thresholds
        if len(self.results_deduplicated[self.results_deduplicated['is_observable']]) > 0:
            priority_median = framework['priority_score_stats']['median']
            recommendation['suggested_priority_threshold'] = priority_median * 0.7
        
        recommendation['suggested_constraints'] = {
            'max_events_per_night': 3,
            'min_window_duration_hours': 1.0,
            'min_altitude': 30,
            'min_moon_separation': 30,
            'urgency_filter': ['critical', 'high']
        }
        
        # Estimated resource usage
        total_nights = self.statistics['temporal']['date_range_days']
        nights_with_obs = len(framework['night_distribution'])
        recommendation['estimated_active_nights_fraction'] = nights_with_obs / total_nights
        
        self.statistics['recommendation'] = recommendation
        
        print(f"\n  Feasibility: {recommendation['feasibility']}")
        print(f"  Reason: {recommendation['reason']}")
        print(f"  Active observation nights: {recommendation['estimated_active_nights_fraction']*100:.1f}%")
        
        return recommendation
    
    def _plot_observability_summary(self):
        """Comprehensive observability summary plot."""
        fig = plt.figure(figsize=(14, 8))
        gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.3)
        
        results_source = self.results_deduplicated
        
        # Calculate timing categories
        observable = results_source[results_source['is_observable']].copy()
        
        timing_categories = {
            'now': 0,
            '<1h later': 0,
            '<2h later': 0,
            '>2h later': 0
        }
        
        for idx, row in observable.iterrows():
            if row['when'] == 'now':
                timing_categories['now'] += 1
            elif row['when'] == 'later':
                wait_time = row.get('time_to_window_hours', 0)
                if wait_time < 1:
                    timing_categories['<1h later'] += 1
                elif wait_time < 2:
                    timing_categories['<2h later'] += 1
                else:
                    timing_categories['>2h later'] += 1
        
        total_events = len(results_source)
        total_observable = len(observable)
        total_not_observable = total_events - total_observable
        
        # Calculate daily statistics
        date_range_days = (pd.to_datetime(results_source['Discovery_UTC']).max() - 
                          pd.to_datetime(results_source['Discovery_UTC']).min()).days
        
        daily_total = total_events / date_range_days
        daily_observable = total_observable / date_range_days
        daily_not_observable = total_not_observable / date_range_days
        
        weekly_total = daily_total * 7
        weekly_observable = daily_observable * 7
        
        # Calculate daily/weekly for each timing category
        timing_daily = {k: v / date_range_days for k, v in timing_categories.items()}
        timing_weekly = {k: v * 7 for k, v in timing_daily.items()}
        
        # 1. Merged bar chart: Total, Observable breakdown, Not Observable
        ax1 = fig.add_subplot(gs[0, :])
        
        categories = ['Total\nEvents', 'Observable\n(NOW)', 'Observable\n(<1h later)', 
                     'Observable\n(<2h later)', 'Observable\n(>2h later)', 'Not\nObservable']
        values = [total_events, timing_categories['now'], timing_categories['<1h later'],
                 timing_categories['<2h later'], timing_categories['>2h later'], 
                 total_not_observable]
        colors_main = ['steelblue', 'darkgreen', 'limegreen', 'yellow', 'orange', 'red']
        
        bars = ax1.bar(categories, values, color=colors_main, edgecolor='black', alpha=0.7)
        
        # Add value labels and daily/weekly stats
        for i, (bar, val, cat) in enumerate(zip(bars, values, categories)):
            height = bar.get_height()
            # Main count
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(val)}',
                    ha='center', va='bottom', fontsize=13, fontweight='bold')
            
            # Percentage
            pct = (val / total_events) * 100
            ax1.text(bar.get_x() + bar.get_width()/2., height/2,
                    f'{pct:.1f}%',
                    ha='center', va='center', fontsize=11, 
                    fontweight='bold', color='white' if pct > 10 else 'black')
            
            # Daily and weekly statistics (bold)
            if i == 0:  # Total
                daily_stat = f'Daily: {daily_total:.2f}\nWeekly: {weekly_total:.1f}'
            elif i == 1:  # NOW
                daily_stat = f'Daily: {timing_daily["now"]:.2f}\nWeekly: {timing_weekly["now"]:.1f}'
            elif i == 2:  # <1h
                daily_stat = f'Daily: {timing_daily["<1h later"]:.2f}\nWeekly: {timing_weekly["<1h later"]:.1f}'
            elif i == 3:  # <2h
                daily_stat = f'Daily: {timing_daily["<2h later"]:.2f}\nWeekly: {timing_weekly["<2h later"]:.1f}'
            elif i == 4:  # >2h
                daily_stat = f'Daily: {timing_daily[">2h later"]:.2f}\nWeekly: {timing_weekly[">2h later"]:.1f}'
            elif i == 5:  # Not Observable
                daily_stat = f'Daily: {daily_not_observable:.2f}\nWeekly: {daily_not_observable*7:.1f}'
            
            ax1.text(bar.get_x() + bar.get_width()/2., -max(values)*0.18,
                    daily_stat,
                    ha='center', va='top', fontsize=9, fontweight='bold')
        
        ax1.set_ylabel('Count', fontsize=12)
        ax1.set_title('Event Overview with Observable Timing (Deduplicated Data)', 
                     fontsize=14, fontweight='bold')
        ax1.set_ylim(0, max(values) * 1.2)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # 2. Overall fraction pie chart
        ax2 = fig.add_subplot(gs[1, 0])
        
        pie_labels = ['NOW', '<1h later', '<2h later', '>2h later', 'Not Observable']
        pie_values = [timing_categories['now'], timing_categories['<1h later'],
                     timing_categories['<2h later'], timing_categories['>2h later'],
                     total_not_observable]
        pie_colors = ['darkgreen', 'limegreen', 'yellow', 'orange', 'red']
        
        wedges, texts, autotexts = ax2.pie(pie_values, labels=pie_labels, autopct='%1.1f%%',
                                           colors=pie_colors, startangle=90)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(9)
        ax2.set_title('Overall Observability Fractions', fontsize=12, fontweight='bold')
        
        # 3. Summary statistics text
        ax3 = fig.add_subplot(gs[1, 1])
        ax3.axis('off')
        
        summary_text = f"""
SUMMARY STATISTICS (Deduplicated)

Total Events: {total_events}
  Daily: {daily_total:.2f} | Weekly: {weekly_total:.1f}

Observable: {total_observable} ({total_observable/total_events*100:.1f}%)
  Daily: {daily_observable:.2f} | Weekly: {weekly_observable:.1f}
  
  • NOW: {timing_categories['now']} ({timing_categories['now']/total_events*100:.1f}%)
  • <1h later: {timing_categories['<1h later']} ({timing_categories['<1h later']/total_events*100:.1f}%)
  • <2h later: {timing_categories['<2h later']} ({timing_categories['<2h later']/total_events*100:.1f}%)
  • >2h later: {timing_categories['>2h later']} ({timing_categories['>2h later']/total_events*100:.1f}%)

Not Observable: {total_not_observable} ({total_not_observable/total_events*100:.1f}%)
  Daily: {daily_not_observable:.2f} | Weekly: {daily_not_observable*7:.1f}

Analysis Period: {date_range_days} days
        """
        
        ax3.text(0.1, 0.95, summary_text, transform=ax3.transAxes,
                fontsize=10, verticalalignment='top', family='monospace',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
        
        plt.savefig(self.output_dir / 'observability_summary.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: observability_summary.png")
        
        # Create separate non-observability reasons plot
        self._plot_non_observability_reasons()
    
    def _plot_non_observability_reasons(self):
        """Create separate plot for non-observability reasons."""
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        
        results_source = self.results_deduplicated
        not_obs = results_source[~results_source['is_observable']]
        
        if len(not_obs) > 0 and 'reason' in not_obs.columns:
            # Clean reasons: remove text after '(' to group similar reasons
            cleaned_reasons = not_obs['reason'].apply(lambda x: x.split('(')[0].strip() if pd.notna(x) else x)
            reasons = cleaned_reasons.value_counts().head(8)
            
            bars = ax.barh(range(len(reasons)), reasons.values, 
                          color='coral', edgecolor='black', alpha=0.7)
            ax.set_yticks(range(len(reasons)))
            ax.set_yticklabels(reasons.index, fontsize=10)
            ax.set_xlabel('Count', fontsize=12)
            ax.set_title('Non-Observability Reasons (Deduplicated Data)', 
                        fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x')
            
            total_not_observable = len(not_obs)
            
            # Add value labels
            for i, (bar, val) in enumerate(zip(bars, reasons.values)):
                width = bar.get_width()
                pct = (val / total_not_observable) * 100
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f' {int(val)} ({pct:.1f}%)',
                       ha='left', va='center', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'non_observability_reasons.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: non_observability_reasons.png")
    
    def _plot_event_timeline(self):
        """Plot event rate timeline (deduplicated data only, no weekly average subplot)."""
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        
        # Filter out 2024-03 outlier
        data_filtered = self.data_deduplicated[
            ~((self.data_deduplicated['Discovery_UTC'].dt.year == 2024) & 
              (self.data_deduplicated['Discovery_UTC'].dt.month == 3))
        ].copy()
        
        # Daily event count (all events)
        daily_counts = data_filtered.groupby(
            data_filtered['Discovery_UTC'].dt.date
        ).size()
        
        ax1 = axes[0]
        ax1.plot(daily_counts.index, daily_counts.values, 'o-', alpha=0.6, markersize=4)
        ax1.axhline(y=daily_counts.mean(), color='r', linestyle='--', 
                   label=f'Mean: {daily_counts.mean():.2f} events/day')
        
        # Calculate weekly average for annotation
        total_events = len(data_filtered)
        total_days = (data_filtered['Discovery_UTC'].max() - 
                     data_filtered['Discovery_UTC'].min()).days
        weekly_rate = (total_events / total_days) * 7
        ax1.text(0.02, 0.98, f'Weekly average: {weekly_rate:.2f} events/week', 
                transform=ax1.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8),
                fontsize=10)
        
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Events per Day')
        ax1.set_title('All GRB Events - Daily Rate Timeline (Deduplicated)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Observable events timeline
        ax2 = axes[1]
        if len(self.results_deduplicated) > 0:
            results_filtered = self.results_deduplicated[
                ~((pd.to_datetime(self.results_deduplicated['Discovery_UTC']).dt.year == 2024) & 
                  (pd.to_datetime(self.results_deduplicated['Discovery_UTC']).dt.month == 3))
            ].copy()
            
            observable_data = results_filtered[results_filtered['is_observable']].copy()
            if len(observable_data) > 0:
                observable_data['date'] = pd.to_datetime(observable_data['Discovery_UTC']).dt.date
                daily_obs_counts = observable_data.groupby('date').size()
                
                ax2.plot(daily_obs_counts.index, daily_obs_counts.values, 'o-', 
                        color='green', alpha=0.6, markersize=4)
                ax2.axhline(y=daily_obs_counts.mean(), color='darkgreen', linestyle='--',
                           label=f'Mean: {daily_obs_counts.mean():.2f} events/day')
                
                # Calculate weekly average for observable
                total_obs = len(observable_data)
                total_days_obs = (pd.to_datetime(observable_data['Discovery_UTC']).max() - 
                                 pd.to_datetime(observable_data['Discovery_UTC']).min()).days
                weekly_obs_rate = (total_obs / total_days_obs) * 7 if total_days_obs > 0 else 0
                ax2.text(0.02, 0.98, f'Weekly average: {weekly_obs_rate:.2f} observable events/week', 
                        transform=ax2.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8),
                        fontsize=10)
                
                ax2.set_xlabel('Date')
                ax2.set_ylabel('Observable Events per Day')
                ax2.set_title('Observable GRB Events - Daily Rate Timeline')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'event_timeline.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: event_timeline.png")
    
    def _plot_sky_distribution(self):
        """Plot spatial distribution on sky (using deduplicated data)."""
        fig = plt.figure(figsize=(14, 6))
        
        # Use deduplicated data
        data_source = self.data_deduplicated
        results_source = self.results_deduplicated
        
        # Mollweide projection
        ax1 = fig.add_subplot(121, projection='mollweide')
        
        # Convert RA to [-180, 180] for Mollweide
        ra_rad = np.deg2rad(data_source['RA'] - 180)
        dec_rad = np.deg2rad(data_source['DEC'])
        
        # Color by observability
        if len(results_source) > 0:
            colors = results_source['is_observable'].map({True: 'green', False: 'red'})
            
            # Plot observable and not observable separately for legend
            observable_mask = results_source['is_observable']
            ax1.scatter(ra_rad[observable_mask], dec_rad[observable_mask], 
                       c='green', alpha=0.6, s=20, label='Observable')
            ax1.scatter(ra_rad[~observable_mask], dec_rad[~observable_mask], 
                       c='red', alpha=0.5, s=20, label='Not Observable')
            ax1.legend(loc='upper left', fontsize=9)
        else:
            ax1.scatter(ra_rad, dec_rad, alpha=0.5, s=20)
        
        ax1.set_xlabel('RA')
        ax1.set_ylabel('Dec')
        ax1.set_title('GRB Sky Distribution (Mollweide, Deduplicated)')
        ax1.grid(True, alpha=0.3)
        
        # RA/Dec histogram
        ax2 = fig.add_subplot(122)
        ax2.hist2d(data_source['RA'], data_source['DEC'], bins=30, cmap='YlOrRd')
        ax2.set_xlabel('RA (degrees)')
        ax2.set_ylabel('Dec (degrees)')
        ax2.set_title('GRB Density Distribution (Deduplicated)')
        ax2.grid(True, alpha=0.3)
        plt.colorbar(ax2.collections[0], ax=ax2, label='Count')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'sky_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: sky_distribution.png")
    
    def _plot_window_characteristics(self):
        """Plot observable window characteristics (using deduplicated data)."""
        observable = self.results_deduplicated[self.results_deduplicated['is_observable']]
        
        if len(observable) == 0:
            print(f"    Skipped: window_characteristics.png (no observable events)")
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Window duration histogram
        ax1 = axes[0, 0]
        durations = observable['window_duration_hours'].dropna()
        if len(durations) > 0:
            ax1.hist(durations, bins=20, color='skyblue', edgecolor='black', alpha=0.7)
            ax1.axvline(durations.median(), color='red', linestyle='--', 
                       label=f'Median: {durations.median():.2f}h')
            ax1.set_xlabel('Window Duration (hours)')
            ax1.set_ylabel('Count')
            ax1.set_title('Observable Window Duration Distribution (Deduplicated)')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
        # Max altitude histogram
        ax2 = axes[0, 1]
        altitudes = observable['max_altitude'].dropna()
        if len(altitudes) > 0:
            ax2.hist(altitudes, bins=20, color='lightgreen', edgecolor='black', alpha=0.7)
            ax2.axvline(altitudes.median(), color='red', linestyle='--',
                       label=f'Median: {altitudes.median():.1f}°')
            ax2.set_xlabel('Maximum Altitude (degrees)')
            ax2.set_ylabel('Count')
            ax2.set_title('Maximum Altitude Distribution')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        # Time to window
        ax3 = axes[1, 0]
        wait_times = observable[observable['when'] == 'later']['time_to_window_hours'].dropna()
        if len(wait_times) > 0:
            ax3.hist(wait_times, bins=20, color='orange', edgecolor='black', alpha=0.7)
            ax3.axvline(wait_times.median(), color='red', linestyle='--',
                       label=f'Median: {wait_times.median():.2f}h')
            ax3.set_xlabel('Time to Window Start (hours)')
            ax3.set_ylabel('Count')
            ax3.set_title('Wait Time for "Observable Later" Events')
            ax3.legend()
            ax3.grid(True, alpha=0.3)
        
        # Window duration vs altitude
        ax4 = axes[1, 1]
        mask = observable['window_duration_hours'].notna() & observable['max_altitude'].notna()
        if mask.sum() > 0:
            sc = ax4.scatter(observable[mask]['window_duration_hours'], 
                           observable[mask]['max_altitude'],
                           c=observable[mask]['Error'], cmap='coolwarm', 
                           s=50, alpha=0.6)
            ax4.set_xlabel('Window Duration (hours)')
            ax4.set_ylabel('Max Altitude (degrees)')
            ax4.set_title('Window Duration vs Max Altitude')
            ax4.grid(True, alpha=0.3)
            plt.colorbar(sc, ax=ax4, label='Position Error (deg)')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'window_characteristics.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: window_characteristics.png")
    
    def _plot_decision_metrics(self):
        """Plot decision framework metrics (Fermi GBM data only)."""
        
        # Create separate plots for events per night and facility
        self._plot_events_per_night()
        self._plot_events_by_facility()
        
        # Main decision metrics plot with position error only - FERMI GBM ONLY
        fig, ax = plt.subplots(1, 1, figsize=(12, 6))
        
        # Filter for Fermi GBM only
        fermi_gbm_mask = self.results_deduplicated['Facility'].str.contains('FermiGBM', na=False)
        fermi_gbm_results = self.results_deduplicated[fermi_gbm_mask]
        
        # Position error for all Fermi GBM (no distinction)
        all_errors = fermi_gbm_results['Error'].dropna()
        
        if len(all_errors) > 0:
            # Remove outliers (e.g., > 30 degrees)
            all_errors_filtered = all_errors[all_errors <= 30]
            
            # Fine binning (0.5 degree bins)
            bins = np.arange(0, 30.5, 0.5)
            
            ax.hist(all_errors_filtered, bins=bins, 
                    color='steelblue', alpha=0.7, edgecolor='black')
            
            # Add vertical line at 1.34 degrees (telescope FoV)
            ax.axvline(x=1.34, color='red', linestyle='--', linewidth=2,
                      label='Telescope FoV (1.34°)')
            
            ax.set_xlabel('Position Error (degrees)')
            ax.set_ylabel('Count')
            ax.set_title(f'Position Error Distribution - Fermi GBM Only (N={len(fermi_gbm_results)})\n'
                        f'Outliers > 30° removed, Telescope FoV: 1.34 × 0.9 degrees')
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'Insufficient Fermi GBM data', 
                   ha='center', va='center', fontsize=14)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'decision_metrics.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: decision_metrics.png")
    
    def _plot_events_per_night(self):
        """Plot observable events per night distribution separately (using deduplicated data)."""
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        
        observable_events = self.results_deduplicated[self.results_deduplicated['is_observable']].copy()
        observable_events['date'] = pd.to_datetime(
            observable_events['Discovery_UTC']
        ).dt.date
        daily_obs = observable_events.groupby('date').size()
        
        if len(daily_obs) > 0:
            # Create histogram
            counts, bins, patches = ax.hist(daily_obs.values, 
                                           bins=range(0, daily_obs.max()+2), 
                                           color='teal', edgecolor='black', alpha=0.7)
            
            # Add value labels on top of each bar
            for i, (count, patch) in enumerate(zip(counts, patches)):
                if count > 0:
                    height = patch.get_height()
                    ax.text(patch.get_x() + patch.get_width()/2., height,
                           f'{int(count)}',
                           ha='center', va='bottom', fontsize=11, fontweight='bold')
            
            # Add median line
            ax.axvline(daily_obs.median(), color='red', linestyle='--', linewidth=2,
                      label=f'Median: {daily_obs.median():.1f}')
            
            ax.set_xlabel('Observable Events per Night', fontsize=12)
            ax.set_ylabel('Number of Nights', fontsize=12)
            ax.set_title('Observable Events per Night Distribution (Deduplicated Data)', 
                        fontsize=14, fontweight='bold')
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'events_per_night.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: events_per_night.png")
    
    def _plot_events_by_facility(self):
        """Plot observable events by facility separately (using deduplicated data)."""
        fig, ax = plt.subplots(1, 1, figsize=(10, 8))
        
        observable_events = self.results_deduplicated[self.results_deduplicated['is_observable']].copy()
        
        if len(observable_events) > 0:
            facility_obs = observable_events['Facility'].value_counts().head(10)
            
            # Create horizontal bar plot
            bars = ax.barh(range(len(facility_obs)), facility_obs.values, 
                          color='steelblue', edgecolor='black', alpha=0.7)
            ax.set_yticks(range(len(facility_obs)))
            ax.set_yticklabels(facility_obs.index, fontsize=10)
            ax.set_xlabel('Observable Events', fontsize=12)
            ax.set_title('Observable Events by Facility (Top 10, Deduplicated Data)', 
                        fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x')
            
            # Add value labels at the end of each bar
            for i, (bar, val) in enumerate(zip(bars, facility_obs.values)):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f' {int(val)}',
                       ha='left', va='center', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'events_by_facility.png', dpi=150, bbox_inches='tight')
        plt.close()
        print(f"    Saved: events_by_facility.png")
    
    def create_visualizations(self):
        """Create comprehensive visualization suite."""
        print("\n[Phase 5] Creating visualizations...")
        
        # Set style
        plt.style.use('default')
        
        # 1. Comprehensive observability summary
        self._plot_observability_summary()
        
        # 2. Event rate timeline
        self._plot_event_timeline()
        
        # 3. Sky distribution
        self._plot_sky_distribution()
        
        # 4. Window characteristics
        self._plot_window_characteristics()
        
        # 5. Decision metrics (includes separate plots)
        self._plot_decision_metrics()
        
        print(f"  All plots saved to {self.output_dir}/")
    
    def save_results(self):
        """Save analysis results to files."""
        print("\n[Phase 6] Saving results...")
        
        # Save deduplicated results CSV only
        self.results_deduplicated.to_csv(self.output_dir / 'observability_results.csv', index=False)
        print(f"  Saved: observability_results.csv")
        
        # Save statistics summary
        import json
        
        stats_serializable = {}
        stats_serializable['deduplication'] = self._make_serializable(self.deduplication_info)
        for key, value in self.statistics.items():
            stats_serializable[key] = self._make_serializable(value)
        
        with open(self.output_dir / 'statistics_summary.json', 'w') as f:
            json.dump(stats_serializable, f, indent=2)
        print(f"  Saved: statistics_summary.json")
        
        # Generate text report
        self._generate_text_report()
    
    def _make_serializable(self, obj):
        """Convert objects to JSON-serializable format."""
        if isinstance(obj, (np.integer, np.floating)):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(x) for x in obj]
        else:
            return obj
    
    def _generate_text_report(self):
        """Generate comprehensive text report."""
        report_path = self.output_dir / 'analysis_report.txt'
        
        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("GRB OBSERVABILITY ANALYSIS REPORT\n")
            f.write("Chilean Observatory (7DT)\n")
            f.write("Deduplicated Data Analysis\n")
            f.write("=" * 80 + "\n\n")
            
            # Deduplication summary
            f.write("-" * 80 + "\n")
            f.write("DEDUPLICATION SUMMARY\n")
            f.write("-" * 80 + "\n")
            dup_info = self.deduplication_info
            f.write(f"Original detections: {dup_info['original_count']}\n")
            f.write(f"Unique GRBs: {dup_info['deduplicated_count']}\n")
            f.write(f"Duplicates removed: {dup_info['removed_count']} "
                   f"({dup_info['duplication_rate']*100:.1f}%)\n\n")
            f.write(f"Deduplication criteria:\n")
            f.write(f"  - Time window: ±3 hours\n")
            f.write(f"  - Position threshold: 5 degrees\n")
            f.write(f"  - Selection: Best localization (lowest error)\n\n")
            
            multi_detections = [g for g in dup_info['grouped_events'] if g['group_size'] > 1]
            if multi_detections:
                f.write(f"Events with multiple detections: {len(multi_detections)}\n")
                f.write(f"Largest group size: {max(g['group_size'] for g in multi_detections)}\n\n")
            
            # Temporal statistics
            f.write("-" * 80 + "\n")
            f.write("TEMPORAL STATISTICS (Deduplicated)\n")
            f.write("-" * 80 + "\n")
            temp = self.statistics['temporal']
            f.write(f"Analysis period: {temp['start_date'].date()} to {temp['end_date'].date()}\n")
            f.write(f"Total events: {temp['total_events']}\n")
            f.write(f"Events per day (mean): {temp['events_per_day_mean']:.2f}\n")
            f.write(f"Events per day (median): {temp['events_per_day_median']:.1f}\n")
            f.write(f"Max events per day: {temp['max_events_per_day']}\n\n")
            
            # Spatial statistics
            f.write("-" * 80 + "\n")
            f.write("SPATIAL STATISTICS (Deduplicated)\n")
            f.write("-" * 80 + "\n")
            spatial = self.statistics['spatial']
            f.write(f"Northern hemisphere: {spatial['northern_hemisphere']} "
                   f"({(1-spatial['southern_fraction'])*100:.1f}%)\n")
            f.write(f"Southern hemisphere: {spatial['southern_hemisphere']} "
                   f"({spatial['southern_fraction']*100:.1f}%)\n\n")
            f.write(f"Position error:\n")
            f.write(f"  Median: {spatial['median_error_deg']:.2f}°\n")
            f.write(f"  Mean: {spatial['mean_error_deg']:.2f}°\n\n")
            
            # Observability statistics
            f.write("-" * 80 + "\n")
            f.write("OBSERVABILITY ANALYSIS (Deduplicated)\n")
            f.write("-" * 80 + "\n")
            obs = self.statistics['observability']
            f.write(f"Total analyzed: {obs['total_analyzed']}\n")
            f.write(f"Observable: {obs['observable']} ({obs['observable_fraction']*100:.1f}%)\n")
            f.write(f"  - Observable NOW: {obs['observable_now']}\n")
            f.write(f"  - Observable LATER: {obs['observable_later']}\n\n")
            
            if 'mean_window_duration' in obs:
                f.write(f"Window characteristics:\n")
                f.write(f"  Mean duration: {obs['mean_window_duration']:.2f} hours\n")
                f.write(f"  Median duration: {obs['median_window_duration']:.2f} hours\n")
                f.write(f"  Mean max altitude: {obs['mean_max_altitude']:.1f}°\n\n")
            
            # Decision framework
            f.write("-" * 80 + "\n")
            f.write("DECISION FRAMEWORK (Deduplicated)\n")
            f.write("-" * 80 + "\n")
            framework = self.statistics['decision_framework']
            f.write(f"Workload estimation:\n")
            f.write(f"  Mean observable events per night: {framework['events_per_night_mean']:.2f}\n")
            f.write(f"  Median: {framework['events_per_night_median']:.1f}\n")
            f.write(f"  Max: {framework['events_per_night_max']}\n")
            f.write(f"  Std: {framework['events_per_night_std']:.2f}\n\n")
            f.write(f"Response timing:\n")
            f.write(f"  Immediate response needed: {framework['immediate_response_fraction']*100:.1f}%\n")
            
            if 'median_wait_time' in framework:
                f.write(f"  Median wait time (for 'later' events): {framework['median_wait_time']:.2f} hours\n\n")
            
            # Recommendation
            f.write("-" * 80 + "\n")
            f.write("RECOMMENDATION\n")
            f.write("-" * 80 + "\n")
            rec = self.statistics['recommendation']
            f.write(f"Feasibility: {rec['feasibility']}\n")
            f.write(f"Reason: {rec['reason']}\n\n")
            f.write(f"Suggested constraints for automatic observations:\n")
            for key, value in rec['suggested_constraints'].items():
                f.write(f"  - {key}: {value}\n")
            f.write(f"\nEstimated resource usage:\n")
            f.write(f"  Active observation nights: {rec['estimated_active_nights_fraction']*100:.1f}%\n")
            
            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        print(f"  Saved: analysis_report.txt")
    
    def run_full_analysis(self):
        """Run complete analysis pipeline using deduplicated data only."""
        print("\n" + "=" * 70)
        print("STARTING GRB OBSERVABILITY ANALYSIS")
        print("=" * 70)
        
        # Load data
        self.load_data()
        
        # Deduplicate events
        self.deduplicate_events()
        
        print("\n" + "=" * 70)
        print("ANALYSIS: DEDUPLICATED DATA (Unique GRBs)")
        print("=" * 70)
        
        # Phase 1: Statistics
        self.temporal_statistics()
        self.spatial_analysis()
        
        # Phase 2: Observability
        self.observability_analysis()
        
        # Phase 3: Decision framework
        self.decision_framework()
        
        # Phase 4: Recommendation
        self.generate_recommendation()
        
        # Phase 5: Visualizations
        self.create_visualizations()
        
        # Phase 6: Save results
        self.save_results()
        
        print("\n" + "=" * 70)
        print("ANALYSIS COMPLETE")
        print("=" * 70)
        print(f"\nAll results saved to: {self.output_dir}/")
        print("\nGenerated files:")
        print("  - observability_results.csv (deduplicated)")
        print("  - statistics_summary.json")
        print("  - analysis_report.txt")
        print("  - observability_summary.png (comprehensive overview)")
        print("  - non_observability_reasons.png")
        print("  - event_timeline.png")
        print("  - sky_distribution.png")
        print("  - window_characteristics.png")
        print("  - decision_metrics.png (Fermi GBM only)")
        print("  - events_per_night.png")
        print("  - events_by_facility.png")


if __name__ == "__main__":
    # Configuration
    DATA_PATH = "gcn_notices_cleaned.csv"
    OUTPUT_DIR = "grb_analysis_results"
    
    # Run analysis
    analyzer = GRBObservabilityAnalyzer(DATA_PATH, OUTPUT_DIR)
    analyzer.run_full_analysis()