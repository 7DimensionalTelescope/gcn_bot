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
from threading import Lock, RLock
from typing import Any, Dict, List, Optional, Tuple, Union

import voeventparse as vp

import pandas as pd

from gcn import linking

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
    thread_index_path : str, optional
        JSON file mapping ``facility-family|trigger`` → Slack ``thread_ts``.
        Unlike the ASCII file (trimmed to ``ascii_max_events`` rows, and rows
        can be re-created by circulars without a ``thread_ts``) this survives
        eviction, so a late notice still lands in its original thread.
        ``None`` disables the index.
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
        "FermiLAT":       ["FERMI_LAT_OFFLINE", "FERMI_LAT_POS_UPD"],
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
        # Auto/manual ToO send markers (UTC timestamp when a ToO was dispatched,
        # empty otherwise). One per telescope so their auto-ToO stays independent.
        # Used to prevent duplicate ToO sends across the new-event and update
        # paths (see was_too_sent / mark_too_sent). Backfilled by _load_ascii
        # for pre-existing files.
        "ToO_7DT", "ToO_RASA36",
        # Deferred-ToO fire times (UTC ISO timestamp when a pending "observable
        # soon" ToO should fire, empty otherwise). Persisted so a pending ToO
        # survives a bot restart during the wait (reloaded into the scheduler on
        # startup). One per telescope.
        "ToO_Deferred_7DT", "ToO_Deferred_RASA36",
        # Tile IDs a dispatched ToO requested (comma-separated, empty when unknown,
        # e.g. a manual send). A linked event's auto-ToO is skipped when it would
        # request exactly the same tiles again (see GCNBot._group_too_block).
        "ToO_Tiles_7DT", "ToO_Tiles_RASA36",
    ]

    # Telescope label → ASCII column holding its ToO-sent marker.
    _TOO_STATE_COLUMNS: Dict[str, str] = {"7DT": "ToO_7DT", "RASA36": "ToO_RASA36"}
    # Telescope label → ASCII column holding its pending deferred-ToO fire time.
    _TOO_DEFERRED_COLUMNS: Dict[str, str] = {
        "7DT": "ToO_Deferred_7DT", "RASA36": "ToO_Deferred_RASA36",
    }
    # Telescope label → ASCII column holding the tile IDs its ToO requested.
    _TOO_TILES_COLUMNS: Dict[str, str] = {"7DT": "ToO_Tiles_7DT", "RASA36": "ToO_Tiles_RASA36"}

    def __init__(
        self,
        output_csv: str = "gcn_notices.csv",
        output_ascii: str = "grb_targets.ascii",
        ascii_max_events: int = 10,
        strict_parsing: bool = False,
        thread_index_path: Optional[str] = None,
        thread_index_max_age_days: float = 30.0,
    ) -> None:
        self.output_csv = output_csv
        self.output_ascii = output_ascii
        self.ascii_max_events = ascii_max_events
        self.strict_parsing = strict_parsing
        # Re-entrant and shared with GCNCircularHandler (see main.py): both write
        # grb_targets.ascii, and the deferred-ToO scheduler / Slack-button threads
        # write it too, so one lock must cover every read-modify-write.
        self.file_lock = RLock()
        self.thread_index_path = thread_index_path
        self._thread_index_max_age = thread_index_max_age_days * 86400.0
        self._thread_index_lock = Lock()

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
        elif (indexed := self.lookup_thread(facility, trigger_num)) and indexed.get("name"):
            # Row was evicted from the ASCII file; keep the name already shown in Slack.
            notice_data["Name"] = indexed["name"]
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
        too_sent: Optional[List[str]] = None,
        too_tiles: Optional[Dict[str, List[str]]] = None,
    ) -> bool:
        """Save or update *notice_data* in the ASCII event file.

        Returns ``True`` on success.  If a row with the same trigger number and
        facility family already exists it is updated in place; otherwise a new
        row is prepended.

        Parameters
        ----------
        too_sent : list[str] | None
            Telescope labels (``"7DT"`` / ``"RASA36"``) whose ToO-sent marker
            should be stamped on this row. Folded into the same write so an
            auto-ToO send costs no extra file write / backup.
        too_tiles : dict[str, list[str]] | None
            Tile IDs each of those ToOs requested (``ToO_Tiles_*`` columns).
        """
        try:
            with self.file_lock:
                df = self._load_ascii()

                facility = str(notice_data.get("Facility", "")).strip()
                trigger_num = str(notice_data.get("Trigger_num", "")).strip()

                existing_idx = self._find_index(df, facility, trigger_num)

                if existing_idx is not None:
                    self._update_row(df, existing_idx, facility, notice_data, thread_ts)
                    row_idx = existing_idx
                else:
                    df = self._append_row(df, facility, notice_data, thread_ts)
                    row_idx = 0  # _append_row prepends the new row

                if too_sent:
                    self._set_too_flags(df, row_idx, too_sent, too_tiles)

                if thread_ts:
                    row = df.loc[row_idx]
                    self.record_thread(
                        facility, trigger_num, thread_ts,
                        str(row["Name"]).strip().strip('"'),
                        str(row["Discovery_UTC"]).strip().strip('"'),
                        notice_time=notice_data.get("Discovery_UTC"),
                        ra=linking.to_float(row["RA"]),
                        dec=linking.to_float(row["DEC"]),
                        err=linking.to_float(row["Error"]),
                    )

                # Trim to max events
                if len(df) > self.ascii_max_events:
                    df["_sort"] = self._recency(df)
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

    # ------------------------------------------------------------------
    # Thread index (facility-family|trigger → Slack thread_ts)
    # ------------------------------------------------------------------

    @staticmethod
    def _recency(df: pd.DataFrame) -> pd.Series:
        """UTC recency key for trimming: Last_Update, falling back to Notice_date.

        Kept identical to ``GCNCircularHandler._recency`` so both writers trim
        the shared ASCII file by the same ordering.
        """
        upd = pd.to_datetime(df["Last_Update"], errors="coerce", utc=True)
        return upd.fillna(pd.to_datetime(df["Notice_date"], errors="coerce", utc=True))

    def _thread_key(self, facility: str, trigger_num: str) -> str:
        return f"{self._normalize_facility(facility)}|{str(trigger_num).strip()}"

    def _load_thread_index(self) -> Dict[str, Any]:
        try:
            with open(self.thread_index_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.warning(f"Could not read thread index {self.thread_index_path}: {exc}")
            return {}

    def record_thread(
        self, facility: str, trigger_num: str, thread_ts: str, name: str = "",
        discovery_utc: str = "", notice_time: Any = None, **fields: Any,
    ) -> None:
        """Remember which Slack thread an event lives in (survives ASCII trimming).

        ``discovery_utc`` (trigger time, UTC) lets a circular that names the
        event only by time ("EP Alert 261106.58") find the thread later;
        ``notice_time`` (this notice's own time, e.g. SVOM-MXT's) is kept in
        ``times`` because MASTER-Net names some circulars after it.
        ``fields`` (ra / dec / err) are stored for same-burst checks once the
        ASCII row is gone. The entry is merged, so a same-burst link
        (``same_as`` / ``link``) survives later records.
        """
        facility = str(facility).strip()
        trigger_num = str(trigger_num).strip()
        thread_ts = str(thread_ts).strip()
        if not (self.thread_index_path and facility and trigger_num and thread_ts):
            return
        try:
            with self._thread_index_lock:
                data = self._load_thread_index()
                key = self._thread_key(facility, trigger_num)
                entry = dict(data.get(key) or {})
                entry.update({"thread_ts": thread_ts, "name": name})
                times = set(entry.get("times") or [])
                for t in (discovery_utc, notice_time):
                    ts = linking.to_utc(t)
                    if ts is not None:
                        times.add(ts.strftime("%Y-%m-%d %H:%M:%S"))
                discovery_utc = str(discovery_utc or "").strip()
                if discovery_utc and discovery_utc.lower() != "nan":
                    entry["discovery_utc"] = discovery_utc
                if times:
                    entry["times"] = sorted(times)
                entry.update({k: v for k, v in fields.items() if v is not None})
                data[key] = entry
                self._save_thread_index(data)
        except Exception as exc:
            logger.error(f"record_thread failed: {exc}")

    def _save_thread_index(self, data: Dict[str, Any]) -> None:
        """Prune by thread_ts age, then write atomically (caller holds the lock)."""
        cutoff = time.time() - self._thread_index_max_age
        data = {k: v for k, v in data.items() if self._ts_epoch(v.get("thread_ts")) >= cutoff}
        tmp = f"{self.thread_index_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=1)
        os.replace(tmp, self.thread_index_path)

    @staticmethod
    def _ts_epoch(thread_ts: Any) -> float:
        try:
            return float(str(thread_ts))
        except ValueError:
            return float("inf")  # unparsable → keep

    def lookup_thread(self, facility: str, trigger_num: str) -> Optional[Dict[str, Any]]:
        """Return ``{thread_ts, name}`` recorded for the event, or ``None``."""
        if not (self.thread_index_path and facility and trigger_num):
            return None
        with self._thread_index_lock:
            entry = self._load_thread_index().get(self._thread_key(facility, trigger_num))
        return entry if entry and entry.get("thread_ts") else None

    def find_ep_thread_by_time(
        self, when: Any, tolerance_min: float = 10.0
    ) -> Optional[Dict[str, Any]]:
        """Einstein Probe event triggered within *tolerance_min* of *when* —
        for "EP Alert YYMMDD.ff" circulars. See :meth:`find_thread_by_time`."""
        return self.find_thread_by_time(when, tolerance_min, {"EinsteinProbe"})

    def find_thread_by_time(
        self, when: Any, tolerance_min: float = 10.0,
        families: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Thread of the event (of *families*, all if ``None``) whose trigger —
        or any later notice — time is within *tolerance_min* of *when* (UTC),
        from the ASCII file and the thread index.

        For circulars that name the event only by time ("EP Alert 261106.58",
        "SVOM GRB261108.05"). Linked same-burst events count as one candidate
        (their canonical thread). Returns ``{thread_ts, name, delta_min}``, or
        ``None`` when nothing — or more than one unrelated event — is that close.
        """
        t = linking.to_utc(when)
        if t is None:
            return None
        found: Dict[str, Dict[str, Any]] = {}   # canonical thread_ts -> candidate
        try:
            with self.file_lock:
                df = self._load_ascii() if os.path.exists(self.output_ascii) else pd.DataFrame()
            data: Dict[str, Any] = {}
            if self.thread_index_path:
                with self._thread_index_lock:
                    data = self._load_thread_index()

            def consider(key: str, times: List[Any], thread_ts: Any, name: Any) -> None:
                thread_ts = str(thread_ts or "").strip().strip('"')
                family = key.partition("|")[0]
                if not thread_ts or (families is not None and family not in families):
                    return
                deltas = [abs((d - t).total_seconds()) / 60.0
                          for d in (linking.to_utc(x) for x in times) if d is not None]
                if not deltas or min(deltas) > tolerance_min:
                    return
                root = self._group_root(key, data)
                if root != key and data.get(root, {}).get("thread_ts"):
                    thread_ts, name = data[root]["thread_ts"], data[root].get("name", name)
                prev = found.get(thread_ts)
                delta = round(min(deltas), 1)
                if prev is None or delta < prev["delta_min"]:
                    found[thread_ts] = {"thread_ts": thread_ts,
                                        "name": str(name or "").strip().strip('"'),
                                        "delta_min": delta}

            for row in df.to_dict("records"):
                key = self._thread_key(self._row_family_facility(row), row.get("Trigger_num", ""))
                consider(key, [row.get("Discovery_UTC")], row.get("thread_ts"), row.get("Name"))
            for key, v in data.items():
                consider(key, [v.get("discovery_utc"), *(v.get("times") or [])],
                         v.get("thread_ts"), v.get("name"))
        except Exception as exc:
            logger.error(f"find_thread_by_time failed: {exc}")
            return None
        if len(found) > 1:
            logger.info(
                f"Time match at {t:%Y-%m-%d %H:%M} is ambiguous "
                f"({', '.join(c['name'] for c in found.values())}) — not used"
            )
            return None
        return next(iter(found.values()), None)

    @staticmethod
    def _name_key(name: Any) -> str:
        return str(name or "").strip().strip('"').lower().replace(" ", "")

    def lookup_thread_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Return the index entry whose event name matches *name*, plus its ``key``.

        Same comparison as :meth:`find_event_by_name` (case- and space-
        insensitive). Entries of one same-burst group share a name and resolve
        to the group's canonical entry. ``None`` when nothing — or more than one
        unrelated event — matches, so an ambiguous name never picks a thread.
        """
        target = self._name_key(name)
        if not (self.thread_index_path and target):
            return None
        with self._thread_index_lock:
            data = self._load_thread_index()
        hits = [
            {**v, "key": k} for k, v in data.items()
            if v.get("thread_ts") and self._name_key(v.get("name")) == target
        ]
        roots = {self._group_root(h["key"], data) for h in hits}
        if len(hits) > 1 and len(roots) == 1:
            root = roots.pop()
            if data.get(root, {}).get("thread_ts"):
                return {**data[root], "key": root}
        if len(hits) > 1:
            logger.info(f"Thread index: name {name!r} is ambiguous ({len(hits)} events) — not used")
            return None
        return hits[0] if hits else None

    # ------------------------------------------------------------------
    # ToO-sent state (duplicate-send guard)
    # ------------------------------------------------------------------

    def _set_too_flags(
        self, df: pd.DataFrame, idx: int, telescopes: List[str],
        tiles: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """Stamp the ToO-sent marker column(s) for *telescopes* on row *idx*,
        plus the requested tile IDs when known."""
        stamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for tel in telescopes:
            col = self._TOO_STATE_COLUMNS.get(tel)
            if col:
                df.at[idx, col] = stamp
            ids = (tiles or {}).get(tel)
            if ids and tel in self._TOO_TILES_COLUMNS:
                df.at[idx, self._TOO_TILES_COLUMNS[tel]] = ",".join(str(i) for i in ids)

    def too_sent_at(
        self, facility: str, trigger_num: str, telescope: str
    ) -> str:
        """Return the UTC marker timestamp of a prior ToO for this event+telescope.

        Looks the event up by ``(trigger_num, facility-family)`` — the same key
        used for event dedup. Returns ``""`` when no ToO was sent, the event row
        does not yet exist (a brand-new event, so the first send always
        proceeds), or the lookup fails.
        """
        col = self._TOO_STATE_COLUMNS.get(telescope)
        if not col:
            logger.warning(f"too_sent_at: unknown telescope '{telescope}'")
            return ""
        facility = str(facility).strip()
        trigger_num = str(trigger_num).strip()
        if not facility or not trigger_num:
            return ""
        try:
            with self.file_lock:
                df = self._load_ascii()
                idx = self._find_index(df, facility, trigger_num)
                if idx is None:
                    return ""
                return str(df.at[idx, col]).strip()
        except Exception as exc:
            logger.error(f"too_sent_at failed: {exc}")
            return ""

    def was_too_sent(
        self, facility: str, trigger_num: str, telescope: str
    ) -> bool:
        """Return ``True`` if a ToO for this event+telescope was already sent."""
        return bool(self.too_sent_at(facility, trigger_num, telescope))

    def mark_too_sent(
        self, facility: str, trigger_num: str, telescope: str,
        tiles: Optional[List[str]] = None,
    ) -> bool:
        """Standalone write of a ToO-sent marker (for the manual Slack path).

        The automatic path folds the marker into its ``save_to_ascii`` call via
        ``too_sent=``; the manual path has no such write, so it uses this. Does
        nothing (and warns) if no matching event row exists yet.
        """
        col = self._TOO_STATE_COLUMNS.get(telescope)
        if not col:
            logger.warning(f"mark_too_sent: unknown telescope '{telescope}'")
            return False
        facility = str(facility).strip()
        trigger_num = str(trigger_num).strip()
        if not facility or not trigger_num:
            return False
        try:
            with self.file_lock:
                df = self._load_ascii()
                idx = self._find_index(df, facility, trigger_num)
                if idx is None:
                    logger.warning(
                        f"mark_too_sent: no ASCII row for {facility} #{trigger_num}"
                    )
                    return False
                self._set_too_flags(df, idx, [telescope],
                                    {telescope: tiles} if tiles else None)
                self._write_df(df)
                logger.info(
                    f"Marked ToO sent ({telescope}) for {facility} #{trigger_num}"
                )
                return True
        except Exception as exc:
            logger.error(f"mark_too_sent failed: {exc}")
            return False

    def _write_df(self, df: pd.DataFrame) -> None:
        """Backup + write the ASCII frame with the canonical column set."""
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

    def set_too_deferred(
        self, facility: str, trigger_num: str, telescope: str, fire_at_iso: str
    ) -> bool:
        """Persist a pending deferred-ToO fire time (UTC ISO) on the event row.

        Written so the pending send survives a restart during the wait; the
        scheduler reloads it via :meth:`get_pending_deferred`.
        """
        col = self._TOO_DEFERRED_COLUMNS.get(telescope)
        if not col:
            logger.warning(f"set_too_deferred: unknown telescope '{telescope}'")
            return False
        facility = str(facility).strip()
        trigger_num = str(trigger_num).strip()
        if not facility or not trigger_num:
            return False
        try:
            with self.file_lock:
                df = self._load_ascii()
                idx = self._find_index(df, facility, trigger_num)
                if idx is None:
                    logger.warning(
                        f"set_too_deferred: no ASCII row for {facility} #{trigger_num}"
                    )
                    return False
                df.at[idx, col] = str(fire_at_iso)
                self._write_df(df)
                logger.info(
                    f"Deferred ToO persisted ({telescope}) for {facility} "
                    f"#{trigger_num} @ {fire_at_iso}"
                )
                return True
        except Exception as exc:
            logger.error(f"set_too_deferred failed: {exc}")
            return False

    def clear_too_deferred(
        self, facility: str, trigger_num: str, telescope: str
    ) -> bool:
        """Clear a pending deferred-ToO fire time (on fire, cancel, or supersede)."""
        col = self._TOO_DEFERRED_COLUMNS.get(telescope)
        if not col:
            return False
        facility = str(facility).strip()
        trigger_num = str(trigger_num).strip()
        if not facility or not trigger_num:
            return False
        try:
            with self.file_lock:
                df = self._load_ascii()
                idx = self._find_index(df, facility, trigger_num)
                if idx is None:
                    return False
                if str(df.at[idx, col]).strip():
                    df.at[idx, col] = ""
                    self._write_df(df)
                    logger.info(
                        f"Deferred ToO cleared ({telescope}) for {facility} #{trigger_num}"
                    )
                return True
        except Exception as exc:
            logger.error(f"clear_too_deferred failed: {exc}")
            return False

    def get_pending_deferred(self) -> List[Dict[str, Any]]:
        """Return all rows with a pending deferred ToO (for scheduler reload).

        Each item: ``{facility, trigger_num, telescope, fire_at, name, ra, dec,
        error}``. ``facility`` is the row's Best/Primary facility — any facility
        on the row matches the same event via ``_find_index`` at fire time.
        """
        out: List[Dict[str, Any]] = []
        try:
            with self.file_lock:
                df = self._load_ascii()
                for _, row in df.iterrows():
                    trigger = str(row.get("Trigger_num", "")).strip().strip('"')
                    if not trigger:
                        continue
                    facility = (
                        str(row.get("Best_Facility", "")).strip()
                        or str(row.get("Primary_Facility", "")).strip()
                    )
                    for telescope, col in self._TOO_DEFERRED_COLUMNS.items():
                        fire_at = str(row.get(col, "")).strip()
                        if not fire_at:
                            continue
                        out.append({
                            "facility":    facility,
                            "trigger_num": trigger,
                            "telescope":   telescope,
                            "fire_at":     fire_at,
                            "name":        str(row.get("Name", "")).strip().strip('"'),
                            "ra":          str(row.get("RA", "")).strip(),
                            "dec":         str(row.get("DEC", "")).strip(),
                            "error":       str(row.get("Error", "")).strip(),
                        })
        except Exception as exc:
            logger.error(f"get_pending_deferred failed: {exc}")
        return out

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
            hits = [row.to_dict() for _, row in df.iterrows()
                    if self._name_key(row.get("Name")) == target]
            if len(hits) > 1:
                # Linked same-burst events share a name: the canonical event is
                # the one whose thread came first.
                hits.sort(key=lambda r: self._ts_epoch(str(r.get("thread_ts", "")).strip() or "inf"))
            return hits[0] if hits else None
        except Exception as exc:
            logger.error(f"Error in find_event_by_name: {exc}")
            return None

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

    # ------------------------------------------------------------------
    # Same-burst groups (cross-facility links, kept in the thread index)
    # ------------------------------------------------------------------
    # One burst seen by several facility families (Fermi GBM + SVOM, …). Each
    # member keeps its own ASCII row and Slack thread; non-canonical members
    # carry ``same_as: <canonical key>`` in their thread-index entry. The
    # canonical member is the one whose Slack thread came first — circulars
    # collect there. Criteria: gcn/linking.py.

    @staticmethod
    def _row_family_facility(row: Any) -> str:
        return str(row.get("Primary_Facility", "") or row.get("Best_Facility", "")).strip().strip('"')

    def _group_root(self, key: str, data: Dict[str, Any]) -> str:
        root = (data.get(key) or {}).get("same_as") or key
        return root if root in data else key

    def group_root(self, key: str) -> str:
        with self._thread_index_lock:
            return self._group_root(key, self._load_thread_index())

    def group_keys(self, key: str) -> List[str]:
        """*key*'s group, canonical key first (just ``[key]`` when unlinked)."""
        with self._thread_index_lock:
            data = self._load_thread_index()
        root = self._group_root(key, data)
        keys = [root] + [k for k, v in data.items() if k != root and v.get("same_as") == root]
        return keys if key in keys else [key]

    def key_for_thread(self, thread_ts: str) -> Optional[str]:
        """Thread-index key of the event that owns *thread_ts*."""
        thread_ts = str(thread_ts or "").strip()
        if not thread_ts:
            return None
        with self._thread_index_lock:
            data = self._load_thread_index()
        return next((k for k, v in data.items() if v.get("thread_ts") == thread_ts), None)

    def link_events(self, key: str, other_key: str, evidence: Dict[str, Any]) -> Optional[str]:
        """Merge *key*'s and *other_key*'s groups; returns the canonical key.

        Canonical = the member whose thread came first. *evidence* (rule, Δt, …)
        is stored as ``link`` on whichever of the two is not canonical. ``None``
        when either event is not in the thread index (no thread yet).
        """
        try:
            with self._thread_index_lock:
                data = self._load_thread_index()
                if key not in data or other_key not in data:
                    return None
                ra, rb = self._group_root(key, data), self._group_root(other_key, data)
                if ra == rb:
                    return ra
                root, other = sorted((ra, rb), key=lambda k: self._ts_epoch(data[k].get("thread_ts")))
                for k, v in data.items():
                    if k == other or v.get("same_as") == other:
                        v["same_as"] = root
                data[root].pop("same_as", None)
                joined, partner = (key, other_key) if key != root else (other_key, key)
                data[joined]["link"] = {
                    **evidence, "with": partner,
                    "at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                self._save_thread_index(data)
            logger.info(f"Same-burst link: {key} + {other_key} → canonical {root}")
            return root
        except Exception as exc:
            logger.error(f"link_events failed: {exc}")
            return None

    def update_link_info(self, key: str, **info: Any) -> None:
        """Merge *info* into *key*'s ``link`` record (e.g. ``confirmed_by``)."""
        try:
            with self._thread_index_lock:
                data = self._load_thread_index()
                if key in data:
                    data[key]["link"] = {**(data[key].get("link") or {}), **info}
                    self._save_thread_index(data)
        except Exception as exc:
            logger.error(f"update_link_info failed: {exc}")

    def group_members(self, key: str) -> List[Dict[str, Any]]:
        """Every member of *key*'s group (itself included), canonical first.

        Values come from the ASCII row when it still exists, else from the
        thread index: ``{key, family, trigger, facility, name, thread_ts, ra,
        dec, err, t, canonical, too, too_tiles}``.
        """
        keys = self.group_keys(key)
        with self.file_lock:
            df = self._load_ascii()
        with self._thread_index_lock:
            data = self._load_thread_index()
        out: List[Dict[str, Any]] = []
        for k in keys:
            fam, _, trig = k.partition("|")
            v = data.get(k) or {}
            idx = self._find_index(df, fam, trig)
            row = df.loc[idx] if idx is not None else None

            def pick(col: str, entry_key: str) -> Any:
                val = linking.to_float(row[col]) if row is not None else None
                return val if val is not None else v.get(entry_key)

            out.append({
                "key": k, "family": fam, "trigger": trig,
                "facility": (self._row_family_facility(row) if row is not None else "") or fam,
                "name": str(row["Name"]).strip().strip('"') if row is not None else v.get("name", ""),
                "thread_ts": (str(row["thread_ts"]).strip() if row is not None else "") or v.get("thread_ts", ""),
                "ra": pick("RA", "ra"), "dec": pick("DEC", "dec"), "err": pick("Error", "err"),
                "t": linking.to_utc(row["Discovery_UTC"]) if row is not None else linking.to_utc(v.get("discovery_utc")),
                "canonical": k == keys[0],
                "too": {tel: (str(row[col]).strip() if row is not None else "")
                        for tel, col in self._TOO_STATE_COLUMNS.items()},
                "too_tiles": {tel: ([x for x in str(row[col]).split(",") if x.strip()] if row is not None else [])
                              for tel, col in self._TOO_TILES_COLUMNS.items()},
            })
        return out

    def find_same_event(
        self, notice_data: Dict[str, Any], params: "linking.LinkParams"
    ) -> Optional[Dict[str, Any]]:
        """Existing event (another facility family) that *notice_data* is the
        same burst as, by the rules in gcn/linking.py.

        Candidates: ASCII rows with a Slack thread, plus thread-index entries
        trimmed out of the ASCII file. The time rule beats the position rule,
        then the smallest |Δt| wins. Returns ``{key, root, thread_ts, name,
        matched_facility, matched_trigger, matched_name, matched_thread_ts,
        rule, dt_s, sep_deg, tol_deg}`` — ``thread_ts`` / ``name`` are the
        group's canonical event — or ``None``.
        """
        facility = str(notice_data.get("Facility", "")).strip()
        family = self._normalize_facility(facility)
        if family not in params.families:
            return None
        new = {"family": family, "t": linking.to_utc(notice_data.get("Discovery_UTC")),
               "ra": linking.to_float(notice_data.get("RA")),
               "dec": linking.to_float(notice_data.get("DEC")),
               "err": linking.to_float(notice_data.get("Error"))}
        own_key = self._thread_key(facility, notice_data.get("Trigger_num", ""))
        try:
            with self.file_lock:
                df = self._load_ascii() if os.path.exists(self.output_ascii) else pd.DataFrame()
            data: Dict[str, Any] = {}
            if self.thread_index_path:
                with self._thread_index_lock:
                    data = self._load_thread_index()
        except Exception as exc:
            logger.error(f"find_same_event: could not load events: {exc}")
            return None

        cands: Dict[str, Dict[str, Any]] = {}
        for row in df.to_dict("records"):
            thread_ts = str(row.get("thread_ts", "")).strip().strip('"')
            fac = self._row_family_facility(row)
            if not thread_ts or not fac:
                continue
            key = self._thread_key(fac, row.get("Trigger_num", ""))
            cands[key] = {"family": key.partition("|")[0], "facility": fac,
                          "t": linking.to_utc(row.get("Discovery_UTC")),
                          "ra": linking.to_float(row.get("RA")), "dec": linking.to_float(row.get("DEC")),
                          "err": linking.to_float(row.get("Error")),
                          "thread_ts": thread_ts, "name": str(row.get("Name", "")).strip().strip('"')}
        for key, v in data.items():
            if key in cands or not v.get("thread_ts"):
                continue
            cands[key] = {"family": key.partition("|")[0], "facility": key.partition("|")[0],
                          "t": linking.to_utc(v.get("discovery_utc")),
                          "ra": v.get("ra"), "dec": v.get("dec"), "err": v.get("err"),
                          "thread_ts": v["thread_ts"], "name": v.get("name", "")}

        hits = []
        for key, c in cands.items():
            if key == own_key:
                continue
            res = linking.evaluate(new, c, params)
            if res is None:
                continue
            label = (f"{facility} #{notice_data.get('Trigger_num')} vs {c['facility']} "
                     f"#{key.partition('|')[2]} ({c['name']}): Δt {res['dt_s']:+.0f} s, "
                     f"sep {res['sep_deg']:.2f}° (allowed {res['tol_deg']:.2f}°)")
            if res["rule"] is None:
                logger.info(f"Same-burst near miss — {label}: trigger times coincide but "
                            "positions disagree — not linked")
                continue
            hits.append(((res["rule"] != "time", abs(res["dt_s"])), key, c, res, label))
        if not hits:
            return None
        hits.sort(key=lambda h: h[0])
        _, key, c, res, label = hits[0]
        root = self._group_root(key, data)
        root_c = cands.get(root) or c
        logger.info(f"Same burst — {label} → {res['rule']} rule; canonical {root_c['name']} ({root})")
        return {"key": key, "root": root,
                "thread_ts": root_c["thread_ts"], "name": root_c["name"],
                "matched_facility": c["facility"], "matched_trigger": key.partition("|")[2],
                "matched_name": c["name"], "matched_thread_ts": c["thread_ts"],
                "matched_err": c.get("err"), **res}

    def rename_event(self, facility: str, trigger_num: str, new_name: str) -> Optional[str]:
        """Set one event's Name in the ASCII row and the thread index.

        Keyed by facility family + trigger — linked same-burst events share a
        name, so a name does not identify a row.
        Returns the previous name (``None`` if the event is unknown).
        """
        new_name = str(new_name).strip()
        trigger_num = str(trigger_num).strip()
        old: Optional[str] = None
        try:
            with self.file_lock:
                df = self._load_ascii()
                idx = self._find_index(df, facility, trigger_num)
                if idx is not None:
                    old = str(df.at[idx, "Name"]).strip().strip('"')
                    if old != new_name:
                        df.at[idx, "Name"] = new_name
                        self._write_df(df)
            if self.thread_index_path:
                with self._thread_index_lock:
                    data = self._load_thread_index()
                    key = self._thread_key(facility, trigger_num)
                    if key in data:
                        old = old if old is not None else data[key].get("name", "")
                        if data[key].get("name") != new_name:
                            data[key]["name"] = new_name
                            self._save_thread_index(data)
        except Exception as exc:
            logger.error(f"rename_event failed: {exc}")
            return None
        if old is not None and old != new_name:
            logger.info(f"Renamed {facility} #{trigger_num}: {old!r} → {new_name!r}")
        return old

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
        return linking.family_of(facility)

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
        # Discovery_UTC is the event's trigger time: never move it later. A later
        # instrument's notice (SVOM-MXT) carries its own, later observation time,
        # which would break trigger-time matching (same-burst links, EP alerts).
        old_t = linking.to_utc(df.at[idx, "Discovery_UTC"])
        new_t = linking.to_utc(notice_data.get("Discovery_UTC"))
        keep_disc = df.at[idx, "Discovery_UTC"] if (
            old_t is not None and (new_t is None or old_t <= new_t)) else None

        for col, val in notice_data.items():
            if col in df.columns and val is not None and str(val).strip():
                df.at[idx, col] = val
        if keep_disc is not None:
            df.at[idx, "Discovery_UTC"] = keep_disc

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

        t = linking.to_utc(trigger_date)       # datetime or "YYYY-MM-DD hh:mm:ss"
        date_key = (t if t is not None else datetime.now(tz=timezone.utc)).strftime("%y%m%d")

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
