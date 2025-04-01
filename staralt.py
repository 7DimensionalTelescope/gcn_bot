from mainobserver import mainObserver
from datetime import datetime as dt, timedelta
from astropy.time import Time
from astropy.coordinates import SkyCoord, AltAz
from astropy import units as u
import numpy as np
import matplotlib.pyplot as plt
from typing import Union, Optional, List, Dict, Any, Tuple
from matplotlib.dates import date2num, DateFormatter
from datetime import datetime


# Define a custom class for observation night outside of methods
class ObsNight:
    """Class to store observation night information."""
    
    def __init__(self):
        # Initialize attributes that will be set later
        self.sunrise_civil: Optional[Time] = None
        self.sunset_civil: Optional[Time] = None
        self.sunrise_nautical: Optional[Time] = None
        self.sunset_nautical: Optional[Time] = None
        self.sunrise_astro: Optional[Time] = None
        self.sunset_astro: Optional[Time] = None
        self.sunrise_night: Optional[Time] = None
        self.sunset_night: Optional[Time] = None

    def __getattr__(self, name: str) -> None:
        return None

    def __repr__(self) -> str:
        attrs = {name: value.iso if isinstance(value, Time) else value
                 for name, value in self.__dict__.items()}
        max_key_len = max(len(key) for key in attrs.keys()) if attrs else 0
        attrs_str = '\n'.join([f'{key:{max_key_len}}: {value}' for key, value in attrs.items()])
        return f'{self.__class__.__name__} Attributes:\n{attrs_str}'


# Helper functions for data processing
def _safe_to_list(value) -> List[float]:
    """Convert a value to a list of floats safely."""
    if value is None:
        return []
    # Optimize: Use NumPy's vectorized operations if input is NumPy array
    if hasattr(value, 'shape') and hasattr(value, 'astype'):
        # Fast path for NumPy arrays
        try:
            # Replace NaN values with 0.0 and convert to Python list
            return np.nan_to_num(value).astype(float).tolist()
        except (TypeError, ValueError):
            pass  # Fall back to regular path if NumPy conversion fails
            
    if hasattr(value, '__iter__') and not isinstance(value, str):
        return [float(v) if v is not None else 0.0 for v in value]
    return [float(value) if value is not None else 0.0]

def _safe_get(data_dict: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary with a default fallback."""
    return data_dict.get(key, default)

def _safe_get_datetime(time_obj) -> Optional[dt]:
    """Safely extract datetime from a Time object."""
    if time_obj is not None and hasattr(time_obj, 'datetime'):
        return time_obj.datetime
    return None

def _safe_date2num(dt_obj: Any) -> Optional[float]:
    """
    Safely convert datetime to numeric value for plotting.
    Handles the case where date2num returns a numpy array instead of a float.
    """
    if dt_obj is None:
        return None
    try:
        result = date2num(dt_obj)
        # Handle case where result is a numpy array
        if hasattr(result, 'item'):
            return float(result.item())
        # Handle case where result is already a scalar
        return float(result)
    except (TypeError, ValueError) as e:
        print(f"Warning: Error converting datetime to numeric: {e}")
        return None

def _safe_get_attribute(obj, attr_path):
    """
    Extract a nested attribute safely.
    attr_path is a list like ['alt', 'value'] for obj.alt.value
    """
    if obj is None:
        return None
        
    current = obj
    for attr in attr_path:
        if not hasattr(current, attr):
            return None
        current = getattr(current, attr)
        if current is None:
            return None
    
    return current

def _safe_get_alt_value(altaz_obj):
    """Extract altitude value from AltAz object safely."""
    return _safe_get_attribute(altaz_obj, ['alt', 'value'])

def _safe_get_separation_value(sep_obj):
    """Extract separation value safely."""
    return _safe_get_attribute(sep_obj, ['value'])

def _extract_time_values(altaz_obj, attr_name='obstime'):
    """Directly extract time values for comparison."""
    if altaz_obj is None or not hasattr(altaz_obj, attr_name):
        return []
        
    attr_val = getattr(altaz_obj, attr_name)
    if attr_val is None:
        return []
        
    # If it's a scalar, return as single item
    if hasattr(attr_val, 'size') and attr_val.size == 1:
        return [attr_val]
        
    # Try to iterate if it's a collection
    try:
        # For arrays with shape attribute
        if hasattr(attr_val, 'shape'):
            return [t for t in attr_val]
        # For regular iterables
        return [t for t in attr_val]
    except TypeError:
        # If iteration fails, treat as a scalar
        return [attr_val]

def _extract_times_safely(altaz_obj, attr_name='obstime'):
    """
    Directly extract time values from possibly non-iterable TimeAttribute.
    Optimized version with caching for better performance.
    """
    result = []
    
    # Check if the object is valid
    if altaz_obj is None or not hasattr(altaz_obj, attr_name):
        return result
        
    attr_val = getattr(altaz_obj, attr_name)
    if attr_val is None:
        return result
    
    # Use an optimized approach for large arrays
    if hasattr(attr_val, 'size') and hasattr(attr_val.size, '__gt__') and attr_val.size > 100:
        # For large arrays, process in chunks for better memory performance
        chunk_size = 100
        # Initialize with the right size to avoid resizing
        result = [None] * attr_val.size
        
        for i in range(0, attr_val.size, chunk_size):
            end = min(i + chunk_size, attr_val.size)
            # Process this chunk
            for j in range(i, end):
                try:
                    result[j] = _safe_isoformat(attr_val[j])
                except (IndexError, TypeError):
                    result[j] = ""
        return result
        
    # If it's a scalar Time object, handle it directly
    if hasattr(attr_val, 'size') and attr_val.size == 1:
        return [_safe_isoformat(attr_val)]
        
    # If it seems to be an array or collection, try to iterate
    try:
        # For Time arrays with shape attribute - use a list comprehension
        if hasattr(attr_val, 'shape'):
            return [_safe_isoformat(t) for t in attr_val]
        # For regular iterables
        return [_safe_isoformat(t) for t in attr_val]
    except TypeError as e:
        # If iteration fails, treat as a scalar
        print(f"Warning: Non-iterable TimeAttribute handled as scalar: {e}")
        return [_safe_isoformat(attr_val)]

def _safe_isoformat(time_obj):
    """Safely extract ISO format from a Time or TimeAttribute object."""
    if time_obj is None:
        return ""
    if hasattr(time_obj, 'datetime'):
        if time_obj.datetime is None:
            return ""
        return time_obj.datetime.isoformat()
    if hasattr(time_obj, 'iso'):
        return time_obj.iso
    # Try direct conversion as last resort
    try:
        return str(time_obj)
    except:
        return ""


class Staralt:
    """Class to handle star altitude plots."""
    
    def __init__(self, 
                 observer: mainObserver,
                 utctime: Optional[Union[dt, Time]] = None):
        """
        Initialize a Staralt object.
        
        Parameters
        ----------
        observer : mainObserver
            Observer object with location information.
        utctime : datetime or Time, optional
            Initial time for calculations. Default is current time.
        """
        # Set the observer
        self._observer = observer
        
        # If no time is provided, use the current time
        if utctime is None:
            utctime = Time.now()
        if not isinstance(utctime, Time):
            utctime = Time(utctime)
            
        # Set the night
        self.tonight = self._set_night(utctime=utctime)
        
        # Initialize data dictionary to store results
        self.data_dict: Dict[str, Any] = {}
    
    @property
    def observer(self) -> mainObserver:
        """Get the observer object."""
        return self._observer
    
    def _set_night(self, utctime: Optional[Union[dt, Time]] = None) -> ObsNight:
        """
        Set the night for the given time.
        
        Parameters
        ----------
        utctime : datetime or Time, optional
            The time for which to set the night.
            
        Returns
        -------
        ObsNight
            Object containing night-related time information.
        """
        if utctime is None:
            utctime = Time.now()
        if not isinstance(utctime, Time):
            utctime = Time(utctime)
        
        obsnight = ObsNight()
        
        # Celestial information
        obsnight.sunrise_civil = self.observer.sun_risetime(utctime, horizon=0, mode='next')
        
        # Only continue if sunrise_civil is not None
        if obsnight.sunrise_civil is not None:
            obsnight.sunset_civil = self.observer.sun_settime(obsnight.sunrise_civil, mode='previous', horizon=0)
            obsnight.sunrise_nautical = self.observer.sun_risetime(obsnight.sunrise_civil, mode='previous', horizon=-6)
            obsnight.sunset_nautical = self.observer.sun_settime(obsnight.sunrise_civil, mode='previous', horizon=-6)
            obsnight.sunrise_astro = self.observer.sun_risetime(obsnight.sunrise_civil, mode='previous', horizon=-12)
            obsnight.sunset_astro = self.observer.sun_settime(obsnight.sunrise_civil, mode='previous', horizon=-12)
            obsnight.sunrise_night = self.observer.sun_risetime(obsnight.sunrise_civil, mode='previous', horizon=-18)
            obsnight.sunset_night = self.observer.sun_settime(obsnight.sunrise_civil, mode='previous', horizon=-18)
        
        return obsnight

    def _get_skycoord(self, ra: Union[float, str], dec: Union[float, str]) -> SkyCoord:
        """
        Convert RA and Dec to SkyCoord object.
        
        Parameters
        ----------
        ra : str or float
            Right Ascension, if str in hms format (e.g., "10:20:30"),
            if float in decimal degrees.
        dec : str or float
            Declination, if str in dms format (e.g., "+20:30:40"),
            if float in decimal degrees.
        
        Returns
        -------
        SkyCoord
            SkyCoord object representing the coordinates.
            
        Raises
        ------
        ValueError
            If RA and Dec formats are not supported.
        """
        # Check if RA and Dec are given as strings (like "10:20:30")
        if isinstance(ra, str) and isinstance(dec, str):
            # Interpret as sexagesimal format (e.g., "10:20:30", "+20:30:40")
            coord = SkyCoord(ra, dec, unit=(u.hourangle, u.deg)) # type: ignore
        elif isinstance(ra, (float, int)) and isinstance(dec, (float, int)):
            # Interpret as decimal degrees
            coord = SkyCoord(ra * u.deg, dec * u.deg) # type: ignore
        else:
            raise ValueError("Unsupported RA and Dec format")
        return coord
        
    def staralt_data(self, 
                     ra: Union[float, str], 
                     dec: Union[float, str], 
                     objname: Optional[str] = None, 
                     utctime: Optional[Union[dt, Time]] = None,
                     target_minalt: float = 30, 
                     target_minmoonsep: float = 30) -> Dict[str, Any]:
        """
        Generate the data needed for the altitude plot.

        Parameters
        ----------
        ra : float or str
            Right Ascension of the target in degrees (float) or in hms format (str).
        dec : float or str
            Declination of the target in degrees (float) or in dms format (str).
        objname : str, optional
            Name of the target object.
        utctime : datetime or Time, optional
            The reference time. If None, current time is used.
        target_minalt : float, optional
            The minimum allowable altitude for observation (in degrees).
        target_minmoonsep : float, optional
            The minimum allowable moon separation (in degrees).

        Returns
        -------
        dict
            A dictionary containing all data required for plotting.
        """
        # Cache the current time to avoid multiple calls
        now = Time.now()
        if utctime is None:
            utctime = now
        elif not isinstance(utctime, Time):
            utctime = Time(utctime)

        # Get the sky coordinates of the target
        coord = self._get_skycoord(ra, dec)

        tonight = self.tonight
        
        # Make sure critical attributes are not None
        if (tonight.sunset_astro is None or tonight.sunrise_astro is None or 
            tonight.sunset_night is None or tonight.sunrise_night is None):
            raise ValueError("Night times not properly set - critical values are None")
        
        # Define time range for plotting using Time arithmetic
        time_range_start = tonight.sunset_astro - 2*u.hour # type: ignore
        time_range_end = tonight.sunrise_astro + 2*u.hour # type: ignore

        # Optimize time axis generation - calculate time points more efficiently
        time_span_seconds = (time_range_end - time_range_start).sec
        # Use 5-minute intervals (300 seconds)
        num_points = int(time_span_seconds / 300) + 1
        time_axis = time_range_start + np.linspace(0, time_span_seconds, num_points)*u.second # type: ignore

        # Calculate altaz for moon, sun, and target - doing this in parallel would be ideal
        # but we'll optimize what we can within the current structure
        moon_altaz = self.observer.moon_altaz(time_axis)
        sun_altaz = self.observer.sun_altaz(time_axis)
        
        # Validation - combine checks for efficiency
        if coord is None:
            raise ValueError("Target coordinates are None")
            
        # Transform to AltAz
        target_altaz = coord.transform_to(AltAz(obstime=time_axis, location=self.observer._earthlocation))

        # Validate primary objects exist before attempting to use them
        primary_checks = [
            (target_altaz is None, "Target altitude-azimuth calculation failed"),
            (moon_altaz is None, "Moon altitude-azimuth coordinates could not be calculated")
        ]

        for condition, message in primary_checks:
            if condition:
                raise ValueError(message)
                
        # Now that we know the primary objects exist, check their required attributes
        attribute_checks = [
            (_safe_get_alt_value(target_altaz) is None, "Target altitude values are missing"),
            (not hasattr(moon_altaz, 'separation'), "Moon coordinates object lacks separation method")
        ]

        for condition, message in attribute_checks:
            if condition:
                raise ValueError(message)

        # Now we can safely calculate separation
        target_moonsep = moon_altaz.separation(target_altaz)

        if _safe_get_separation_value(target_moonsep) is None:
            raise ValueError("Moon separation values are missing")

        # Extract values using our helper functions
        target_alts = _safe_to_list(_safe_get_alt_value(target_altaz))
        moon_alts = _safe_to_list(_safe_get_alt_value(moon_altaz))
        sun_alts = _safe_to_list(_safe_get_alt_value(sun_altaz))
        target_moonsep_vals = _safe_to_list(_safe_get_separation_value(target_moonsep))
        
        # Optimize color determination using NumPy where possible
        try:
            # Extract time values for comparison - only do this once
            obs_times = _extract_time_values(target_altaz)
            
            # Pre-allocate the color array
            color_target = ['r'] * len(target_alts)
            
            # Only process if we have valid times
            if obs_times and len(obs_times) > 0:
                obs_times_array = np.array(obs_times)
                is_night_time = np.logical_and(obs_times_array >= tonight.sunset_night, 
                                            obs_times_array <= tonight.sunrise_night)
                is_above_min_alt = np.array(target_alts) > target_minalt
                is_min_moon_sep = np.array(target_moonsep_vals) > target_minmoonsep
                
                # Combine all conditions
                observable_mask = np.logical_and.reduce((is_night_time, is_above_min_alt, is_min_moon_sep))
                color_target = np.where(observable_mask, 'g', 'r').tolist()

        except (TypeError, AttributeError) as e:
            print(f"Warning: Error processing values for coloring: {e}")
            # Default to not observable
            color_target = ['r'] * len(target_alts)
        
        # Extract times in ISO format - do this once and cache the results
        moon_times = _extract_times_safely(moon_altaz)
        sun_times = _extract_times_safely(sun_altaz)
        target_times = _extract_times_safely(target_altaz)

        # Convert night times to datetime with proper None checks
        tonight_data: Dict[str, Optional[dt]] = {
            "sunset_night": _safe_get_datetime(getattr(tonight, 'sunset_night', None)),
            "sunrise_night": _safe_get_datetime(getattr(tonight, 'sunrise_night', None)),
            "sunset_civil": _safe_get_datetime(getattr(tonight, 'sunset_civil', None)),
            "sunrise_civil": _safe_get_datetime(getattr(tonight, 'sunrise_civil', None))
        }

        # Create the data dictionary - use the cached now value
        data_dict: Dict[str, Any] = {
            "objname": objname,
            "now_datetime": now.datetime,  # Use cached value
            "time_range_start": time_range_start.datetime,
            "time_range_end": time_range_end.datetime,
            "moon_times": moon_times,
            "moon_alts": moon_alts,
            "sun_times": sun_times,
            "sun_alts": sun_alts,
            "target_ra": ra,
            "target_dec": dec,
            "target_times": target_times,
            "target_alts": target_alts,
            "target_moonsep": target_moonsep_vals,
            "color_target": color_target,
            "tonight": tonight_data,
            "target_minalt": target_minalt,
            "target_minmoonsep": target_minmoonsep
        }
        
        # Store in instance variable and return
        self.data_dict = data_dict
        return data_dict

    def plot_staralt(self, data: Optional[Dict[str, Any]] = None, show_current_time: bool = True) -> None:
        """
        Plot the altitude data from the dictionary returned by staralt_data().

        Parameters
        ----------
        data : dict, optional
            The dictionary containing data required for plotting.
        show_current_time : bool, optional
            Whether to show the current time marker on the plot.
        """
        # Unpack data, using the instance's data_dict as a fallback
        if data is None:
            if not hasattr(self, 'data_dict') or not self.data_dict:
                raise ValueError("No data available for plotting. Run staralt_data first.")
            data = self.data_dict

        # Extract data from dictionary with proper default handling
        objname = _safe_get(data, "objname")
        now_datetime = _safe_get(data, "now_datetime")
        moon_times = _safe_get(data, "moon_times", [])
        moon_alts = _safe_get(data, "moon_alts", [])
        target_times = _safe_get(data, "target_times", [])
        target_alts = _safe_get(data, "target_alts", [])
        color_target = _safe_get(data, "color_target", [])
        tonight = _safe_get(data, "tonight", {})
        target_minalt = float(_safe_get(data, "target_minalt", 30))
        target_minmoonsep = float(_safe_get(data, "target_minmoonsep", 30))
        
        # Extract and convert all datetime objects at once for better performance
        datetime_conversions = {
            "now": now_datetime,
            "sunset_night": _safe_get(tonight, "sunset_night"),
            "sunrise_night": _safe_get(tonight, "sunrise_night"),
            "sunset_civil": _safe_get(tonight, "sunset_civil"),
            "sunrise_civil": _safe_get(tonight, "sunrise_civil")
        }
        
        # Check for required night times
        required_times = ["sunset_night", "sunrise_night", "sunset_civil", "sunrise_civil"]
        if not all(datetime_conversions[key] for key in required_times):
            raise ValueError("Required night times are missing from the data")
            
        # Bulk convert to numeric format
        numeric_times = {}
        for key, dt_value in datetime_conversions.items():
            numeric_times[key] = _safe_date2num(dt_value)
            
        # Validate critical numeric values
        critical_keys = required_times
        if any(numeric_times[key] is None for key in critical_keys):
            raise ValueError("Failed to convert critical night times to numeric values")
        
        # Assign variables from the dictionary for cleaner code
        now_datetime_num = numeric_times["now"]
        sunset_night_num = numeric_times["sunset_night"]
        sunrise_night_num = numeric_times["sunrise_night"]  
        sunset_civil_num = numeric_times["sunset_civil"]
        sunrise_civil_num = numeric_times["sunrise_civil"]
        
        # Optimize time conversion by creating a conversion cache
        time_conversion_cache = {}
        
        def _cached_time_to_num(time_str):
            """Convert time string to numeric with caching for better performance."""
            if isinstance(time_str, (str, datetime)) and time_str in time_conversion_cache:
                return time_conversion_cache[time_str]
                
            try:
                if isinstance(time_str, str):
                    dt_obj = datetime.fromisoformat(time_str)
                    result = _safe_date2num(dt_obj) or 0.0
                else:
                    result = _safe_date2num(time_str) or 0.0
                    
                # Cache the result
                time_conversion_cache[time_str] = result
                return result
            except (ValueError, TypeError) as e:
                print(f"Warning: Cannot convert time {time_str}: {e}")
                return 0.0
        
        # Convert moon and target times to numeric using the cached function
        moon_times_num = [_cached_time_to_num(t) for t in moon_times]
        target_times_num = [_cached_time_to_num(t) for t in target_times]
        
        # Calculate middle of the night once
        mid_night_num = sunset_night_num + 0.5 * (sunrise_night_num - sunset_night_num) if (sunset_night_num is not None and sunrise_night_num is not None) else 0.0
        
        # Determine title
        titlename = f'Altitude of {objname}' if objname else 'Altitude of the Target'
        
        # Find observable windows more efficiently
        if 'g' in color_target:
            # Use NumPy for faster array operations
            observable_indices = np.where(np.array(color_target) == 'g')[0]
            observable_start_idx = observable_indices[0] if len(observable_indices) > 0 else None
            observable_end_idx = observable_indices[-1] if len(observable_indices) > 0 else None
        else:
            observable_start_idx = None
            observable_end_idx = None
        
        # Initialize observation variables
        obs_start_time = None
        obs_end_time = None
        obs_start_num = None
        obs_end_num = None
        total_observable_hours = 0.0
        remaining_observable_hours = 0.0
        
        # Calculate observable time windows only if there are observable periods
        if observable_start_idx is not None and observable_end_idx is not None:
            if observable_start_idx < len(target_times) and observable_end_idx < len(target_times):
                # Get the start and end times
                start_time_str = target_times[observable_start_idx]
                end_time_str = target_times[observable_end_idx]
                
                # Convert with error handling
                try:
                    # Convert start time
                    if isinstance(start_time_str, str):
                        obs_start_time = datetime.fromisoformat(start_time_str)
                    else:
                        obs_start_time = start_time_str
                    
                    # Convert end time
                    if isinstance(end_time_str, str):
                        obs_end_time = datetime.fromisoformat(end_time_str)
                    else:
                        obs_end_time = end_time_str
                    
                    # Get numeric values for plotting
                    if obs_start_time and obs_end_time:
                        obs_start_num = _safe_date2num(obs_start_time)
                        obs_end_num = _safe_date2num(obs_end_time)
                        
                        # Calculate observation times
                        if obs_start_time and obs_end_time:
                            total_observable_hours = (obs_end_time - obs_start_time).total_seconds() / 3600.0
                            
                            if now_datetime and obs_end_time:
                                if now_datetime < obs_end_time:
                                    if now_datetime > obs_start_time:
                                        remaining_observable_hours = (obs_end_time - now_datetime).total_seconds() / 3600.0
                                    else:
                                        remaining_observable_hours = total_observable_hours
                except (ValueError, TypeError, IndexError) as e:
                    print(f"Warning: Error processing observable times: {e}")

        # Optimize the plotting section by preparing data structures first
        # Plotting - Create figure just once
        fig = plt.figure(dpi=300, figsize=(10, 5))
        plt.title(titlename, loc='center')
        
        # Add subtitle with observation criteria
        plt.figtext(0.515, 0.905, f"Criteria: Alt > {target_minalt}°, Moon sep > {target_minmoonsep}°", 
                    ha='center', va='center', fontsize=9, style='italic')

        # Ensure all arrays have matching lengths - do this once before plotting
        data_arrays = [
            (moon_times_num, moon_alts, "Moon times", "altitudes"),
            (target_times_num, target_alts, "Target times", "altitudes"),
            (target_times_num, color_target, "Target times", "colors")
        ]
        
        for time_array, data_array, time_name, data_name in data_arrays:
            if len(time_array) != len(data_array):
                print(f"Warning: {time_name} ({len(time_array)}) and {data_name} ({len(data_array)}) have different lengths")
                # Adjust to minimum length
                min_len = min(len(time_array), len(data_array))
                if time_array is moon_times_num and data_array is moon_alts:
                    moon_times_num = moon_times_num[:min_len]
                    moon_alts = moon_alts[:min_len]
                elif time_array is target_times_num and data_array is target_alts:
                    target_times_num = target_times_num[:min_len]
                    target_alts = target_alts[:min_len]
                elif time_array is target_times_num and data_array is color_target:
                    target_times_num = target_times_num[:min_len]
                    color_target = color_target[:min_len]

        # Batch together similar plotting operations
        
        # 1. Plot data points
        if moon_times_num and moon_alts:
            plt.scatter(moon_times_num, moon_alts, c='b', s=10, marker='.', label='Moon')

        if target_times_num and target_alts and color_target:
            plt.scatter(target_times_num, target_alts, c=color_target, s=30, marker='*', label='Target')

        # 2. Fill regions and draw lines - use float conversion just once per value
        sunset_night_float = float(sunset_night_num)
        sunrise_night_float = float(sunrise_night_num)
        sunset_civil_float = float(sunset_civil_num)
        sunrise_civil_float = float(sunrise_civil_num)
        
        # Fill in regions
        plt.fill_betweenx([10, 90], sunset_night_float, sunrise_night_float, color='k', alpha=0.3)
        plt.fill_betweenx([10, 90], sunset_civil_float, sunrise_civil_float, color='k', alpha=0.1)
        
        # Draw vertical lines
        plt.axvline(x=sunrise_night_float, linestyle='-', c='k', linewidth=0.5)
        plt.axvline(x=sunset_night_float, linestyle='-', c='k', linewidth=0.5)
        
        # Fill region below minimum altitude
        plt.fill_between([sunset_night_float, sunrise_night_float], 0, float(target_minalt), color='r', alpha=0.3)

        # 3. Add text annotations - doing similar operations in batches
        text_annotations = [
            (sunset_night_float, 93, 'Night start'),
            (sunrise_night_float, 93, 'Night end'),
            (float(mid_night_num), 20, 'Observation limit')
        ]
        
        for x, y, text in text_annotations:
            color = 'darkred' if text == 'Observation limit' else 'k'
            plt.text(x, y, text, fontsize=10, ha='center', va='center', c=color)
        
        # 4. Add time markers with standard colors
        time_colors = {
            'current': 'purple',
            'start': 'darkgreen',
            'end': 'darkorange'
        }
        
        # Add current time marker
        if show_current_time and now_datetime_num is not None:
            now_float = float(now_datetime_num)
            plt.axvline(now_float, linestyle='--', c=time_colors['current'], linewidth=1.5, label='Current time')
            
            if now_datetime:
                plt.annotate(now_datetime.strftime("%H:%M"), 
                            xy=(now_float, 0), 
                            xycoords='data', 
                            xytext=(0, -10), 
                            textcoords='offset points',
                            color=time_colors['current'], 
                            fontsize=9,
                            ha='center', 
                            va='top')
        
        # Add observation window markers
        if (obs_start_num is not None and obs_end_num is not None):
            obs_start_float = float(obs_start_num)
            obs_end_float = float(obs_end_num)
            
            # Add vertical lines
            plt.axvline(obs_start_float, linestyle='-.', c=time_colors['start'], linewidth=1.5, label='Obs. start')
            plt.axvline(obs_end_float, linestyle='-.', c=time_colors['end'], linewidth=1.5, label='Obs. end')
            
            # Add time labels
            if obs_start_time:
                plt.annotate(obs_start_time.strftime("%H:%M"), 
                            xy=(obs_start_float, 0), 
                            xycoords='data', 
                            xytext=(0, -10), 
                            textcoords='offset points',
                            color=time_colors['start'], 
                            fontsize=9,
                            ha='center', 
                            va='top')
            
            if obs_end_time:
                plt.annotate(obs_end_time.strftime("%H:%M"), 
                            xy=(obs_end_float, 0), 
                            xycoords='data', 
                            xytext=(0, -10), 
                            textcoords='offset points',
                            color=time_colors['end'], 
                            fontsize=9,
                            ha='center', 
                            va='top')
        
        # 5. Create the information text
        time_info = []
        if now_datetime:
            time_info.append(f'Current Time: {now_datetime.strftime("%Y-%m-%d %H:%M:%S")} UTC')
        
        if obs_start_time is not None and obs_end_time is not None:
            time_info.append(f'Observable Period: {obs_start_time.strftime("%H:%M")} - {obs_end_time.strftime("%H:%M")} UTC')
            time_info.append('')  # Add empty line for spacing
            time_info.append(f'Total Observable Time: {total_observable_hours:.1f} hours')
            time_info.append(f'Remaining Observable Time: {remaining_observable_hours:.1f} hours')
        else:
            time_info.append('Target is not observable tonight')
        
        # Join once
        info_text = '\n'.join(time_info)
        
        # Calculate text position
        text_x = sunset_night_float - (sunrise_night_float - sunset_night_float) * 0.13
        
        # Add text box
        plt.text(text_x, 85, info_text, 
                fontsize=9, ha='left', va='top', c='k', 
                bbox=dict(facecolor='white', alpha=0.7, boxstyle='round'),
                linespacing=1.3)

        # 6. Final plot setup - do this once at the end
        plt.xlim(sunset_civil_float, sunrise_civil_float)
        plt.ylim(10, 90)
        plt.legend(loc='upper right')
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m-%d %H'))
        plt.xlabel('UTC Time [mm-dd hh]')
        plt.ylabel('Altitude [degrees]')
        plt.grid()
        # plt.xticks(rotation=45)
        
        # Add bottom margin
        plt.subplots_adjust(bottom=0.18)