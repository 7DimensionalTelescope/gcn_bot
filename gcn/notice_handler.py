"""
GCNNoticeHandler
================
Parses GCN notices and persists them to CSV and ASCII databases.

Ported from ``gcn_notice_handler.py`` (v1.2.0) with the following additions:
  - ``compare_event_data()`` — moved here from gcn_bot.py so all notice-data
    logic lives in one place.

Supported facilities
--------------------
Classic text format:
    Swift (BAT, XRT, UVOT), Fermi (GBM, LAT),
    IceCube (CASCADE, BRONZE, GOLD), HAWC, AMON, CALET

JSON format:
    Einstein Probe (WXT)

VOEvent format:
    SVOM (GRM, ECLAIRs, MXT)  — routed through the amon parser as a fallback
"""

import csv
import glob
import json
import logging
import math
import os
import re
import shutil
import time
from datetime import datetime, timezone
from string import ascii_lowercase, ascii_uppercase
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, Union

import voeventparse as vp

import pandas as pd

logger = logging.getLogger(__name__)


class GCNNoticeHandler:
    """
    Parse and persist GCN notices.

    Parameters
    ----------
    output_csv : str
        Path to the CSV file for storing all processed notices.
    output_ascii : str
        Path to the ASCII (space-delimited) file keeping the most recent events.
    ascii_max_events : int
        Maximum number of rows kept in the ASCII file.
    strict_parsing : bool
        When ``True``, a notice is rejected unless *all* regex patterns match.
        When ``False`` (default), partial matches are accepted.
    """

    # ------------------------------------------------------------------
    # Regex patterns per facility family
    # ------------------------------------------------------------------
    PATTERNS = {
        "fermi": {
            "ra":           r"GRB_RA:.*?(\d+\.\d+)d.*?\(J2000\)",
            "dec":          r"GRB_DEC:.*?([-+]?\d+\.\d+)d.*?\(J2000\)",
            "error":        r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            "date":         r"GRB_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            "time":         r"GRB_TIME:.*?{([\d:\.]+)}\s*UT",
            "trigger_num":  r"TRIGGER_NUM:\s*(\d+)",
            "notice_date":  r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
        },
        "swift": {
            "ra":           r"(?:GRB_RA|POINT_RA):.*?(\d+\.\d+)d?.*\(J2000\)",
            "dec":          r"(?:GRB_DEC|POINT_DEC):.*?([-+]?\d+\.\d+)d?.*\(J2000\)",
            "error":        r"GRB_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            "date":         r"(?:GRB_DATE|IMG_START_DATE):.*?(\d{2})/(\d{2})/(\d{2})",
            "time":         r"(?:GRB_TIME|IMG_START_TIME):.*?{([\d:\.]+)}\s*UT",
            "trigger_num":  r"TRIGGER_NUM:\s*(\d+)",
            "notice_date":  r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
        },
        "amon": {
            "ra":           r"SRC_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
            "dec":          r"SRC_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
            "error":        r"SRC_ERROR:\s*([\d.]+)\s*\[(\w+).*?\]",
            "date":         r"DISCOVERY_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            "time":         r"DISCOVERY_TIME:.*?{([\d:\.]+)}\s*UT",
            "trigger_num":  r"EVENT_NUM:\s*(\d+)",
            "notice_date":  r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
        },
        "calet": {
            "ra":           r"POINT_RA:.*?(\d+\.\d+)d?.*?\(J2000\)",
            "dec":          r"POINT_DEC:.*?([-+]?\d+\.\d+)d?.*?\(J2000\)",
            "error":        None,
            "date":         r"TRIGGER_DATE:.*?(\d{2})/(\d{2})/(\d{2})",
            "time":         r"TRIGGER_TIME:.*?{([\d:\.]+)}\s*UT",
            "trigger_num":  r"TRIGGER_NUM:\s*(\d+)",
            "notice_date":  r"NOTICE_DATE:\s*(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
        },
    }

    # SVOM VOEvent notice types that are parsed; all others are silently dropped
    SVOM_ACCEPTED_TYPES: frozenset = frozenset(
        {"grm-trigger", "eclairs-wakeup", "mxt-initial", "mxt-update"}
    )

    # Maps SVOM notice type → instrument-specific facility name
    SVOM_FACILITY_MAP: Dict[str, str] = {
        "grm-trigger":    "SVOM-GRM",
        "eclairs-wakeup": "SVOM-ECLAIRs",
        "mxt-initial":    "SVOM-MXT",
        "mxt-update":     "SVOM-MXT",
    }

    # Facility name → list of topic substrings that identify it
    MONITORED_FACILITIES: Dict[str, List[str]] = {
        "SwiftBAT":       ["SWIFT_BAT_GRB_POS_ACK"],
        "SwiftXRT":       ["SWIFT_XRT_POSITION"],
        "SwiftUVOT":      ["SWIFT_UVOT_POS"],
        "FermiGBM":       ["FERMI_GBM_GND_POS", "FERMI_GBM_FIN_POS", "FERMI_GBM_FLT_POS"],
        "FermiLAT":       ["FERMI_LAT_OFFLINE"],
        "AMON":           ["AMON_NU_EM_COINC"],
        "IceCubeCASCADE": ["ICECUBE_CASCADE"],
        "HAWC":           ["HAWC_BURST_MONITOR"],
        "IceCubeBRONZE":  ["ICECUBE_ASTROTRACK_BRONZE"],
        "IceCubeGOLD":    ["ICECUBE_ASTROTRACK_GOLD"],
        "CALET":          ["CALET_GBM_FLT_LC"],
        "EinsteinProbe":  ["einstein_probe"],
        "SVOM":           ["svom"],
    }

    CSV_COLUMNS = [
        "GCN_ID", "Name", "RA", "DEC", "Error",
        "Discovery_UTC", "Facility", "Trigger_num", "Notice_date",
    ]

    ASCII_COLUMNS = [
        "GCN_ID", "Name", "RA", "DEC", "Error",
        "Discovery_UTC", "Primary_Facility", "Best_Facility", "All_Facilities",
        "Trigger_num", "Notice_date", "Last_Update", "Redshift", "Host_info", "thread_ts",
    ]

    def __init__(
        self,
        output_csv: str = "gcn_notices.csv",
        output_ascii: str = "grb_targets.ascii",
        ascii_max_events: int = 10,
        strict_parsing: bool = False,
    ) -> None:
        self.output_csv = output_csv
        self.output_ascii = output_ascii
        self.ascii_max_events = ascii_max_events
        self.strict_parsing = strict_parsing
        self.file_lock = Lock()

    # ==================================================================
    # Public API
    # ==================================================================

    def parse_notice(
        self, formatted_text: Union[str, bytes], topic: str
    ) -> Optional[Dict[str, Any]]:
        """Parse a raw GCN notice and return a structured data dict, or ``None``."""
        facility = self._get_facility(topic)
        if not facility:
            logger.debug(f"No facility matched for topic: {topic}")
            return None

        if isinstance(formatted_text, bytes):
            formatted_text = formatted_text.decode("utf-8", "ignore")

        if "EinsteinProbe" in facility:
            return self._parse_einstein_probe(formatted_text, facility)

        if "SVOM" in facility:
            return self._parse_svom_voevent(formatted_text, facility)

        parser_key: Optional[str] = None
        if "Swift" in facility:
            parser_key = "swift"
        elif "Fermi" in facility:
            parser_key = "fermi"
        elif any(f in facility for f in ["AMON", "IceCube", "HAWC"]):
            parser_key = "amon"
        elif "CALET" in facility:
            parser_key = "calet"

        if parser_key:
            return self._parse_text_notice(formatted_text, facility, self.PATTERNS[parser_key])

        logger.warning(f"No parser available for facility: {facility}")
        return None

    def assign_name(self, notice_data: Dict[str, Any]) -> None:
        """Populate ``notice_data['Name']`` before the notice is persisted.

        Looks up the ASCII file so that:
        - updates reuse the name already assigned to the event
        - new events get a freshly generated name that doesn't collide with
          same-day entries
        If ``notice_data['Name']`` is already set this is a no-op.
        """
        if notice_data.get("Name"):
            return
        df = self._load_ascii()
        facility = notice_data.get("Facility", "")
        trigger_num = str(notice_data.get("Trigger_num", "")).strip()
        existing_idx = self._find_index(df, facility, trigger_num)
        if existing_idx is not None:
            notice_data["Name"] = df.at[existing_idx, "Name"]
        else:
            notice_data["Name"] = self._generate_name(
                notice_data.get("Discovery_UTC"), facility, df
            )

    def save_to_csv(self, notice_data: Dict[str, Any]) -> bool:
        """Append *notice_data* to the CSV file. Returns ``True`` on success."""
        try:
            with self.file_lock:
                new_row = pd.DataFrame([notice_data]).reindex(columns=self.CSV_COLUMNS)
                file_exists = os.path.exists(self.output_csv)
                new_row.to_csv(
                    self.output_csv,
                    mode="a",
                    header=not file_exists,
                    index=False,
                    quoting=csv.QUOTE_MINIMAL,
                )
                logger.info(f"Saved {notice_data.get('Name', 'N/A')} to {self.output_csv}")
                return True
        except Exception as exc:
            logger.error(f"Failed to save to CSV: {exc}", exc_info=True)
            return False

    def save_to_ascii(
        self,
        notice_data: Dict[str, Any],
        thread_ts: Optional[str] = None,
    ) -> bool:
        """Save or update *notice_data* in the ASCII event file.

        Returns ``True`` on success.  If a row with the same trigger number and
        facility family already exists it is updated in place; otherwise a new
        row is prepended.
        """
        try:
            with self.file_lock:
                df = self._load_ascii()

                facility = str(notice_data.get("Facility", "")).strip()
                trigger_num = str(notice_data.get("Trigger_num", "")).strip()

                existing_idx = self._find_index(df, facility, trigger_num)

                if existing_idx is not None:
                    self._update_row(df, existing_idx, facility, notice_data, thread_ts)
                else:
                    df = self._append_row(df, facility, notice_data, thread_ts)

                # Trim to max events
                if len(df) > self.ascii_max_events:
                    df["_sort"] = pd.to_datetime(df["Notice_date"], errors="coerce", utc=True)
                    df = (
                        df.sort_values("_sort", ascending=False, na_position="last")
                        .head(self.ascii_max_events)
                        .drop(columns=["_sort"])
                    )

                self._create_backup(self.output_ascii)
                df.to_csv(
                    self.output_ascii,
                    sep=" ",
                    header=True,
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                    quotechar='"',
                    columns=self.ASCII_COLUMNS,
                )
                logger.info(f"ASCII file saved with {len(df)} entries.")
                return True

        except Exception as exc:
            logger.error(f"Critical error in save_to_ascii: {exc}", exc_info=True)
            return False

    def find_existing_event(
        self,
        facility: str,
        trigger_num: str,
        return_full_data: bool = False,
    ) -> Optional[Union[str, Dict[str, Any]]]:
        """Look up an event by facility + trigger number in the ASCII file.

        Parameters
        ----------
        facility : str
        trigger_num : str
        return_full_data : bool
            If ``False`` (default) returns only the GRB name string.
            If ``True`` returns the full row as a dict.

        Returns ``None`` if not found.
        """
        if not facility or not trigger_num:
            return None
        try:
            if not os.path.exists(self.output_ascii):
                return None
            df = pd.read_csv(
                self.output_ascii,
                sep=r"\s+",
                quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC,
                dtype=str,
                na_filter=False,
            )
            if df.empty:
                return None

            norm = self._normalize_facility(facility)
            for _, row in df.iterrows():
                if str(row.get("Trigger_num", "")).strip() != str(trigger_num).strip():
                    continue
                facs = [f.strip() for f in str(row.get("All_Facilities", "")).split(",")]
                if norm in [self._normalize_facility(f) for f in facs]:
                    if return_full_data:
                        return row.to_dict()
                    return row.get("Name", "").strip().strip('"')
            return None
        except Exception as exc:
            logger.error(f"Error finding existing event: {exc}")
            return None

    def find_event_by_name(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Find an ASCII row whose ``Name`` column matches *event_id*.

        Comparison is case-insensitive, strips surrounding quotes, and ignores
        internal spaces so "EP260527a" matches "EP 260527a".
        Returns the full row as a dict, or ``None`` if not found.
        """
        if not event_id:
            return None
        try:
            if not os.path.exists(self.output_ascii):
                return None
            df = self._load_ascii()
            if df.empty:
                return None
            target = event_id.strip().lower().replace(" ", "")
            for _, row in df.iterrows():
                row_name = str(row.get("Name", "")).strip().strip('"').lower().replace(" ", "")
                if row_name == target:
                    return row.to_dict()
            return None
        except Exception as exc:
            logger.error(f"Error in find_event_by_name: {exc}")
            return None

    def update_ascii_event_name(self, old_name: str, new_name: str) -> bool:
        """Replace the ``Name`` field for the row whose Name matches *old_name*.

        Useful for upgrading a provisional event name to the official GCN
        ``eventId`` once the first facility circular arrives.

        Returns ``True`` on success, ``False`` if the row was not found.
        """
        try:
            with self.file_lock:
                df = self._load_ascii()
                if df.empty:
                    return False
                target = old_name.strip().lower()
                found  = False
                for idx, row in df.iterrows():
                    row_name = str(row.get("Name", "")).strip().strip('"').lower()
                    if row_name == target:
                        df.at[idx, "Name"] = new_name
                        found = True
                        break
                if not found:
                    return False
                self._create_backup(self.output_ascii)
                import csv as _csv
                df.to_csv(
                    self.output_ascii,
                    sep=" ",
                    header=True,
                    index=False,
                    quoting=_csv.QUOTE_NONNUMERIC,
                    quotechar='"',
                    columns=self.ASCII_COLUMNS,
                )
                logger.info(f"ASCII Name updated: '{old_name}' → '{new_name}'")
                return True
        except Exception as exc:
            logger.error(f"Error updating ASCII event name: {exc}", exc_info=True)
            return False

    def compare_event_data(
        self,
        old_data: Dict[str, Any],
        new_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Identify differences between a stored event and an incoming notice.

        Returns a dict with zero or more of these keys:

        ``coordinates``
            dict of {field: new_value} for RA/DEC/Error changes > 0.001°
        ``facility_change``
            dict with ``from`` and ``to`` keys when the *instrument* changes
            within the same mission (e.g. SwiftBAT → SwiftXRT)
        ``visibility``
            the ``visibility_info`` dict from *new_data* if present
        """
        differences: Dict[str, Any] = {}
        try:
            # Coordinate changes
            coord_changed: bool = False
            new_coords: Dict[str, Any] = {}
            for field in ("RA", "DEC", "Error"):
                old_val = old_data.get(field, "")
                new_val = new_data.get(field, "")
                try:
                    old_f = float(old_val) if old_val not in ("", None) else None
                    new_f = float(new_val) if new_val not in ("", None) else None
                    if old_f is not None and new_f is not None:
                        if abs(old_f - new_f) > 0.001:
                            coord_changed = True
                            new_coords[field] = new_f
                    elif old_f != new_f and new_f is not None:
                        coord_changed = True
                        new_coords[field] = new_f
                except (ValueError, TypeError):
                    if old_val != new_val and new_val:
                        coord_changed = True
                        new_coords[field] = new_val

            if coord_changed:
                differences["coordinates"] = new_coords

            # Facility / instrument change within same mission
            old_fac = old_data.get("Facility", "")
            new_fac = new_data.get("Facility", "")
            if (
                self._normalize_facility(old_fac) == self._normalize_facility(new_fac)
                and old_fac != new_fac
            ):
                differences["facility_change"] = {"from": old_fac, "to": new_fac}

            # Visibility info pass-through
            if "visibility_info" in new_data:
                differences["visibility"] = new_data["visibility_info"]

            logger.info(f"compare_event_data: {len(differences)} difference type(s) found")
        except Exception as exc:
            logger.error(f"Error comparing event data: {exc}")
        return differences

    def find_related_events(
        self,
        notice_data: Dict[str, Any],
        leniency_factor: float = 1.2,
        time_window_hours: float = 24.0,
    ) -> List[Dict[str, Any]]:
        """Return rows from the ASCII file that are spatially and temporally
        coincident with *notice_data*.

        Two events are considered related when their error circles overlap:
            angular_separation(c1, c2) < (r1 + r2) * leniency_factor
        and their trigger times are within *time_window_hours* of each other.

        The row belonging to *notice_data* itself (matched by GCN_ID) is always
        excluded so an event is never reported as related to itself.
        """
        ra_raw  = notice_data.get("RA")
        dec_raw = notice_data.get("DEC")
        err_raw = notice_data.get("Error")
        t_raw   = notice_data.get("Discovery_UTC")
        gcn_id  = str(notice_data.get("GCN_ID", "")).strip()

        if ra_raw in ("", None) or dec_raw in ("", None):
            return []

        try:
            ra    = float(ra_raw)
            dec   = float(dec_raw)
            error = float(err_raw) if err_raw not in ("", None) else 0.0
        except (ValueError, TypeError):
            return []

        t: Optional[datetime] = None
        if isinstance(t_raw, datetime):
            t = t_raw
        elif t_raw:
            try:
                t = datetime.fromisoformat(str(t_raw))
            except (ValueError, TypeError):
                pass

        related: List[Dict[str, Any]] = []
        try:
            if not os.path.exists(self.output_ascii):
                return []

            with self.file_lock:
                df = pd.read_csv(
                    self.output_ascii,
                    sep=r"\s+",
                    quotechar='"',
                    quoting=csv.QUOTE_NONNUMERIC,
                    dtype=str,
                    na_filter=False,
                )

            if df.empty:
                return []

            for _, row in df.iterrows():
                row_id = str(row.get("GCN_ID", "")).strip().strip('"')
                if row_id == gcn_id:
                    continue

                try:
                    row_ra  = float(str(row.get("RA",  "")).strip().strip('"'))
                    row_dec = float(str(row.get("DEC", "")).strip().strip('"'))
                except (ValueError, TypeError):
                    continue

                row_err_s = str(row.get("Error", "")).strip().strip('"')
                try:
                    row_err = float(row_err_s) if row_err_s else 0.0
                except (ValueError, TypeError):
                    row_err = 0.0

                sep = self._angular_separation(ra, dec, row_ra, row_dec)
                if sep > (error + row_err) * leniency_factor:
                    continue

                if t is not None:
                    row_t_s = str(row.get("Discovery_UTC", "")).strip().strip('"')
                    try:
                        row_t = datetime.fromisoformat(row_t_s)
                        if abs((t - row_t).total_seconds()) > time_window_hours * 3600:
                            continue
                    except (ValueError, TypeError):
                        pass  # can't parse row time → don't reject on time criterion

                related.append(row.to_dict())

        except Exception as exc:
            logger.error(f"Error in find_related_events: {exc}", exc_info=True)

        if related:
            logger.info(
                f"Cross-match: {len(related)} related event(s) found for {gcn_id} "
                f"(leniency={leniency_factor}, window={time_window_hours}h)"
            )
        return related

    @staticmethod
    def _angular_separation(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
        """Great-circle angular separation in degrees (haversine formula)."""
        r1, d1, r2, d2 = map(math.radians, [ra1, dec1, ra2, dec2])
        a = (
            math.sin((d2 - d1) / 2) ** 2
            + math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2
        )
        return math.degrees(2 * math.asin(math.sqrt(min(a, 1.0))))

    # ==================================================================
    # Private: facility resolution
    # ==================================================================

    def _get_facility(self, topic: str) -> Optional[str]:
        for facility, substrings in self.MONITORED_FACILITIES.items():
            if any(s in topic for s in substrings):
                return facility
        return None

    def _normalize_facility(self, facility: str) -> str:
        """Collapse instrument variants to the parent mission name."""
        if not facility:
            return ""
        fac = facility.strip()
        if any(n.lower() in fac.lower() for n in
               ["Swift", "SwiftBAT", "SwiftXRT", "SwiftUVOT"]):
            return "Swift"
        if any(x in fac for x in ["Fermi", "GBM", "LAT"]):
            return "Fermi"
        if "GECAM" in fac:
            return "GECAM"
        if "SVOM" in fac:
            return "SVOM"
        if "Einstein" in fac or "EP" in fac:
            return "EinsteinProbe"
        if "IceCube" in fac or "ICECUBE" in fac:
            return "IceCube"
        return fac

    # ==================================================================
    # Private: parsing
    # ==================================================================

    def _parse_text_notice(
        self,
        text: str,
        facility: str,
        patterns: Dict[str, Optional[str]],
    ) -> Optional[Dict[str, Any]]:
        parsed: Dict[str, Any] = {
            "ra": None,
            "dec": None,
            "error": None,
            "trigger_date": None,
            "trigger_num": None,
            "notice_date": datetime.now(tz=timezone.utc),
        }

        matches = {
            key: (re.search(pat, text, re.DOTALL) if pat else None)
            for key, pat in patterns.items()
        }

        if self.strict_parsing:
            missing = [k for k, m in matches.items() if not m]
            if missing:
                logger.error(f"Strict parse failed for {facility}: missing {missing}")
                return None
        else:
            if not any(matches.values()):
                logger.error(f"No patterns matched for {facility}")
                return None

        # RA
        if matches.get("ra"):
            try:
                parsed["ra"] = float(matches["ra"].group(1))
            except (ValueError, AttributeError) as e:
                logger.warning(f"Cannot parse RA for {facility}: {e}")

        # DEC
        if matches.get("dec"):
            try:
                parsed["dec"] = float(matches["dec"].group(1))
            except (ValueError, AttributeError) as e:
                logger.warning(f"Cannot parse DEC for {facility}: {e}")

        # Error radius
        if matches.get("error"):
            try:
                val, unit = matches["error"].group(1), matches["error"].group(2)
                parsed["error"] = self._normalize_error_deg(val, unit)
            except (ValueError, AttributeError) as e:
                logger.warning(f"Cannot parse error for {facility}: {e}")

        # Trigger date+time
        if matches.get("date") and matches.get("time"):
            try:
                yy, mm, dd = matches["date"].groups()
                t = matches["time"].group(1).strip()
                fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in t else "%Y-%m-%d %H:%M:%S"
                parsed["trigger_date"] = datetime.strptime(
                    f"20{yy}-{mm}-{dd} {t}", fmt
                )
            except (ValueError, AttributeError) as e:
                logger.warning(f"Cannot parse trigger date for {facility}: {e}")

        # Notice date
        if matches.get("notice_date"):
            try:
                _, day, mon, year, hh, mm2, ss = matches["notice_date"].groups()
                month_n = datetime.strptime(mon, "%b").month
                fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in ss else "%Y-%m-%d %H:%M:%S"
                parsed["notice_date"] = datetime.strptime(
                    f"20{year}-{month_n}-{day} {hh}:{mm2}:{ss}", fmt
                )
            except (ValueError, AttributeError) as e:
                logger.warning(f"Cannot parse notice date for {facility}: {e}")

        # Trigger number
        if matches.get("trigger_num"):
            try:
                parsed["trigger_num"] = matches["trigger_num"].group(1)
            except AttributeError as e:
                logger.warning(f"Cannot parse trigger_num for {facility}: {e}")

        # Round and strip microseconds
        for key in ("ra", "dec", "error"):
            if parsed[key] is not None:
                parsed[key] = round(float(parsed[key]), 2)
        for key in ("trigger_date", "notice_date"):
            if parsed[key] is not None:
                parsed[key] = parsed[key].replace(microsecond=0)

        if not any(v is not None for v in parsed.values()):
            logger.error(f"No valid data found for {facility}")
            return None

        return self._make_notice_data(
            ra=parsed["ra"],
            dec=parsed["dec"],
            error=parsed["error"],
            trigger_date=parsed["trigger_date"],
            facility=facility,
            notice_date=parsed["notice_date"],
            trigger_num=parsed["trigger_num"],
        )

    @staticmethod
    def _parse_iso_datetime(dt_str: str) -> Optional[datetime]:
        """Parse an ISO 8601 datetime string, handling any number of fractional
        second digits (Python < 3.11 fromisoformat only accepts 3 or 6 digits).

        Returns a timezone-aware datetime in UTC, or None on failure.
        """
        try:
            # Normalise 'Z' suffix and pad fractional seconds to 6 digits
            s = dt_str.replace("Z", "+00:00")
            s = re.sub(
                r"(\d{2}:\d{2}:\d{2})\.(\d+)",
                lambda m: m.group(1) + "." + (m.group(2) + "000000")[:6],
                s,
            )
            dt = datetime.fromisoformat(s)
            # Ensure UTC — replace() is safe because EP always broadcasts UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError) as exc:
            logger.warning(f"Cannot parse ISO datetime '{dt_str}': {exc}")
            return None

    def _parse_einstein_probe(self, text: str, facility: str) -> Optional[Dict[str, Any]]:
        parsed: Dict[str, Any] = {
            "ra": None,
            "dec": None,
            "error": 0.0,
            "trigger_date": None,
            "notice_date": datetime.now(tz=timezone.utc).replace(microsecond=0),
            "trigger_num": "UNKNOWN",
        }
        try:
            data = json.loads(text)
            if "ra" in data:
                parsed["ra"] = round(float(data["ra"]), 2)
            if "dec" in data:
                parsed["dec"] = round(float(data["dec"]), 2)
            if "ra_dec_error" in data:
                parsed["error"] = round(float(data["ra_dec_error"]), 2)
            if "trigger_time" in data:
                dt = self._parse_iso_datetime(data["trigger_time"])
                if dt is not None:
                    parsed["trigger_date"] = dt.replace(microsecond=0)
            if "id" in data:
                id_val = data["id"]
                if isinstance(id_val, list) and id_val:
                    parsed["trigger_num"] = str(id_val[0])
                elif isinstance(id_val, (str, int)):
                    parsed["trigger_num"] = str(id_val)

            return self._make_notice_data(
                ra=parsed["ra"],
                dec=parsed["dec"],
                error=parsed["error"],
                trigger_date=parsed["trigger_date"],
                facility=facility,
                notice_date=parsed["notice_date"],
                trigger_num=parsed["trigger_num"],
            )
        except json.JSONDecodeError as exc:
            logger.error(f"Invalid JSON for {facility}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"Unexpected error parsing {facility}: {exc}")
            return None

    def _parse_svom_voevent(self, xml_text: str, facility: str) -> Optional[Dict[str, Any]]:
        """Parse a SVOM VOEvent XML notice.

        Only grm-trigger, eclairs-wakeup, mxt-initial, and mxt-update are
        processed; all other SVOM notice types return ``None``.
        """
        try:
            raw = xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text
            v = vp.loads(raw)
        except Exception as exc:
            logger.error(f"Failed to load SVOM VOEvent XML: {exc}")
            return None

        # Determine notice type from the IVORN fragment
        ivorn = v.attrib.get("ivorn", "")
        fragment = ivorn.split("#", 1)[-1]  # e.g. "sb25020701_grm-trigger"

        notice_type: Optional[str] = None
        for accepted in self.SVOM_ACCEPTED_TYPES:
            if accepted in fragment:
                notice_type = accepted
                break

        if notice_type is None:
            logger.info(f"Skipping unsupported SVOM notice type (ivorn={ivorn!r})")
            return None

        specific_facility = self.SVOM_FACILITY_MAP[notice_type]

        # Burst ID (used as trigger_num for thread correlation)
        burst_id: Optional[str] = None
        try:
            grouped = vp.get_grouped_params(v)
            burst_id = grouped["Svom_Identifiers"]["Burst_Id"]["value"]
        except (KeyError, AttributeError, TypeError) as exc:
            logger.error(f"SVOM VOEvent missing Burst_Id: {exc}")
            return None

        # Notice date from <Who/Date>
        notice_date = datetime.now(tz=timezone.utc).replace(microsecond=0)
        try:
            date_text = v.Who.Date.text
            if date_text:
                dt = self._parse_iso_datetime(date_text.strip())
                if dt:
                    notice_date = dt.replace(microsecond=0)
        except AttributeError:
            pass

        # Trigger time
        trigger_date: Optional[datetime] = None
        try:
            dt = vp.get_event_time_as_utc(v)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                trigger_date = dt.replace(microsecond=0)
        except Exception:
            pass

        # Sky position (optional — GRM trigger carries no position)
        ra: Optional[float] = None
        dec: Optional[float] = None
        error: Optional[float] = None
        try:
            pos = vp.get_event_position(v)
            if pos.ra is not None:
                ra = round(float(pos.ra), 4)
            if pos.dec is not None:
                dec = round(float(pos.dec), 4)
            if pos.err is not None:
                unit = (pos.units or "deg").lower()
                error = round(self._normalize_error_deg(float(pos.err), unit), 4)
        except Exception:
            pass

        return self._make_notice_data(
            ra=ra,
            dec=dec,
            error=error,
            trigger_date=trigger_date,
            facility=specific_facility,
            notice_date=notice_date,
            trigger_num=burst_id,
        )

    # ==================================================================
    # Private: data helpers
    # ==================================================================

    @staticmethod
    def _to_utc(dt: Optional[datetime]) -> Optional[datetime]:
        """Strip timezone info, returning a naive UTC datetime."""
        if dt is None:
            return None
        return dt.replace(tzinfo=None)

    def _make_notice_data(
        self,
        ra: Optional[float],
        dec: Optional[float],
        error: Optional[float],
        trigger_date: Optional[datetime],
        facility: str,
        notice_date: Optional[datetime],
        trigger_num: str = "",
    ) -> Dict[str, Any]:
        return {
            "GCN_ID": f"GCN_{facility}_{trigger_num}",
            "Name": "",
            "RA": ra if ra is not None else "",
            "DEC": dec if dec is not None else "",
            "Error": error if error is not None else "",
            "Discovery_UTC": self._to_utc(trigger_date) or "",
            "Facility": facility,
            "Trigger_num": str(trigger_num) if trigger_num else "",
            "Notice_date": self._to_utc(notice_date),
        }

    @staticmethod
    def _normalize_error_deg(value: Any, unit: str) -> float:
        """Convert error value to degrees."""
        val = float(value)
        u = unit.lower()
        if u == "arcmin":
            return val / 60.0
        if u == "arcsec":
            return val / 3600.0
        return val  # deg or unknown

    # ==================================================================
    # Private: ASCII file management
    # ==================================================================

    def _load_ascii(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(
                self.output_ascii,
                sep=r"\s+",
                quotechar='"',
                quoting=csv.QUOTE_NONNUMERIC,
                dtype=str,
                na_filter=False,
            )
            for col in self.ASCII_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            return df[self.ASCII_COLUMNS].fillna("")
        except (pd.errors.EmptyDataError, FileNotFoundError):
            return pd.DataFrame(columns=self.ASCII_COLUMNS)
        except Exception as exc:
            logger.warning(f"Could not load ASCII file, recreating: {exc}")
            self._create_backup(self.output_ascii)
            return pd.DataFrame(columns=self.ASCII_COLUMNS)

    def _find_index(
        self, df: pd.DataFrame, facility: str, trigger_num: str
    ) -> Optional[int]:
        if df.empty or not facility or not trigger_num:
            return None
        norm = self._normalize_facility(facility)
        for idx, row in df.iterrows():
            if str(row.get("Trigger_num", "")).strip() != trigger_num:
                continue
            facs = [f.strip() for f in str(row.get("All_Facilities", "")).split(",")]
            if norm in [self._normalize_facility(f) for f in facs]:
                return idx
        return None

    def _update_row(
        self,
        df: pd.DataFrame,
        idx: int,
        facility: str,
        notice_data: Dict[str, Any],
        thread_ts: Optional[str],
    ) -> None:
        name = df.at[idx, "Name"]
        notice_data["Name"] = name

        existing_facs = {f.strip() for f in str(df.at[idx, "All_Facilities"]).split(",") if f.strip()}
        existing_facs.add(facility)

        for col, val in notice_data.items():
            if col in df.columns and val is not None and str(val).strip():
                df.at[idx, col] = val

        df.at[idx, "All_Facilities"] = ",".join(sorted(existing_facs))
        df.at[idx, "Last_Update"] = notice_data.get("Notice_date", "")

        if thread_ts:
            df.at[idx, "thread_ts"] = thread_ts

        logger.info(f"Updated existing ASCII entry for {facility} trigger {notice_data.get('Trigger_num')}")

    def _append_row(
        self,
        df: pd.DataFrame,
        facility: str,
        notice_data: Dict[str, Any],
        thread_ts: Optional[str],
    ) -> pd.DataFrame:
        if not notice_data.get("Name"):
            notice_data["Name"] = self._generate_name(notice_data["Discovery_UTC"], facility, df)

        row = {col: notice_data.get(col, "") for col in self.ASCII_COLUMNS}
        row.update(
            {
                "Primary_Facility": facility,
                "Best_Facility": facility,
                "All_Facilities": facility,
                "Last_Update": notice_data.get("Notice_date", ""),
                "thread_ts": thread_ts or "",
            }
        )
        new_df = pd.DataFrame([row])
        result = pd.concat([new_df, df], ignore_index=True)
        logger.info(f"Appended new ASCII entry: {notice_data['Name']} (thread_ts={thread_ts or 'empty'})")
        return result

    def _generate_name(
        self, trigger_date: Any, facility: str, df: pd.DataFrame
    ) -> str:
        """Generate GRB/EP/IceCube-YYMMDD[letter] name."""
        is_ep = "EinsteinProbe" in facility
        is_ice = any(x in facility for x in ["IceCube", "AMON"])

        if is_ice:
            prefix, sep, alphabet = "IceCube", "-", ascii_uppercase
        elif is_ep:
            prefix, sep, alphabet = "EP", " ", ascii_lowercase
        else:
            prefix, sep, alphabet = "GRB", " ", ascii_uppercase

        if isinstance(trigger_date, datetime):
            date_key = trigger_date.strftime("%y%m%d")
        else:
            date_key = datetime.now(tz=timezone.utc).strftime("%y%m%d")

        used: set = set()
        if not df.empty and "Name" in df.columns:
            pat = re.compile(rf"^{prefix}[- ]{date_key}([A-Za-z])$")
            for name in df["Name"]:
                m = pat.match(str(name).strip().strip('"'))
                if m:
                    used.add(m.group(1))

        letter = next((l for l in alphabet if l not in used), alphabet[-1])
        name = f"{prefix}{sep}{date_key}{letter}"
        logger.info(f"Generated name: {name}")
        return name

    def _create_backup(self, filepath: str, max_backups: int = 5) -> None:
        if not os.path.exists(filepath):
            return
        backup = f"{filepath}.backup.{int(time.time())}"
        try:
            shutil.copy2(filepath, backup)
            # Prune old backups
            all_backups = sorted(
                glob.glob(f"{filepath}.backup.*"),
                key=os.path.getmtime,
                reverse=True,
            )
            for old in all_backups[max_backups:]:
                os.remove(old)
        except Exception as exc:
            logger.warning(f"Backup failed for {filepath}: {exc}")
