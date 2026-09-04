"""
GCNCircularHandler
==================
Processes GCN circulars and updates the CSV / ASCII event databases,
and provides structured data for Slack thread notifications.

Notable changes from v1 (provisional port)
-------------------------------------------
* ``event_id``          — official GCN event ID taken directly from the
                          ``eventId`` JSON field (e.g. "GRB 260511B").
* ``is_first_circular`` — True when the subject matches a facility-specific
                          first-detection phrase (Fermi / SVOM / EP / IceCube).
* ``event_page_url``    — GCN event-page URL derived from ``event_id``.
* Facility-specific trigger extraction (SVOM burst-id, Fermi integer MET,
  Einstein Probe WXT ID; IceCube has no trigger_num).
* False-trigger detection now checks the *subject* line first (many EP/SVOM
  retraction circulars embed the verdict there).
* ``_update_ascii()`` uses a two-pass search: Name match first, then
  Trigger_num + facility fallback — and overwrites the Name with the
  official ``event_id`` when matched via the fallback.
* ``process_circular_from_json()`` accepts either a JSON string or an already-
  decoded dict (fixes the double-decode bug in main.py), and returns the
  processed result dict.
"""

import glob
import json
import logging
import os
import re
import shutil
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)


class GCNCircularHandler:
    """
    Parse GCN circulars and update event databases.

    Parameters
    ----------
    output_csv : str
        Path to the circular CSV database.
    output_ascii : str
        Path to the shared ASCII event file (same file used by
        ``GCNNoticeHandler``).
    ascii_max_events : int
        Maximum rows kept in the ASCII file.
    """

    CSV_COLUMNS = [
        "circular_id", "event_name", "subject", "facility", "trigger_num",
        "ra", "dec", "error", "error_unit", "redshift", "redshift_error",
        "host_info", "false_trigger", "created_on", "processed_on",
    ]

    ASCII_COLUMNS = [
        "GCN_ID", "Name", "RA", "DEC", "Error", "Discovery_UTC",
        "Primary_Facility", "Best_Facility", "All_Facilities", "Trigger_num",
        "Notice_date", "Last_Update", "Redshift", "Host_info", "thread_ts",
    ]

    # Facility precision priority (higher = more accurate position)
    FACILITY_PRIORITIES: Dict[str, int] = {
        "SwiftXRT": 10, "Swift-XRT": 10,
        "SwiftBAT": 7,  "Swift-BAT": 7,
        "Swift": 6,
        "FermiLAT": 5,  "Fermi-LAT": 5,
        "SVOM": 4,
        "FermiGBM": 3,  "Fermi-GBM": 3, "Fermi": 3,
        "GECAM": 2,     "CALET": 2,
    }

    # Regex patterns in the subject that identify the *first* circular for each facility.
    # Matched with re.search(..., re.IGNORECASE) so anchoring is not needed.
    _FIRST_CIRCULAR_PATTERNS: Dict[str, str] = {
        "FermiGBM":      r"Fermi GBM Final Real-time Localization",
        "Fermi":         r"Fermi GBM Final Real-time Localization",
        "SVOM":          r"SVOM detection of a burst",
        # Covers e.g.:
        #   "Einstein Probe detection of an X-ray transient"
        #   "Einstein Probe detection of a fast X-ray transient"
        #   "Einstein Probe WXT detection of the X-ray prompt emission"
        #   "Einstein Probe detected of a fast X-ray transient"  (typo variant)
        #   "Einstein Probe detection of a sub-threshold X-ray transient …"
        "EinsteinProbe": r"Einstein\s*Probe[/\s\-]*(?:WXT|FXT)?\s+detect(?:ion|ed)\s+of\s+(?:a|an|the)(?:\s+\S+){0,3}\s*X-ray",
        "IceCube":       r"IceCube observation of a high-energy neutrino candidate",
    }

    def __init__(
        self,
        output_csv: str = "gcn_circulars.csv",
        output_ascii: str = "grb_targets.ascii",
        ascii_max_events: int = 10,
    ) -> None:
        self.output_csv = output_csv
        self.output_ascii = output_ascii
        self.ascii_max_events = ascii_max_events
        self.file_lock = threading.Lock()

    # ==================================================================
    # Public API
    # ==================================================================

    def process_circular_from_json(
        self, json_str_or_dict: Union[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Parse *json_str_or_dict* and update both databases.

        Accepts either a raw JSON string *or* an already-decoded dict (the
        latter avoids a redundant ``json.loads`` call when main.py has already
        decoded the Kafka payload).

        Returns the processed result dict so callers can act on it (e.g. for
        Slack thread notifications).
        """
        try:
            if isinstance(json_str_or_dict, dict):
                data = json_str_or_dict
            else:
                data = json.loads(json_str_or_dict)
            processed = self.process_circular(data)
            self._update_databases(processed)
            logger.info(f"Processed circular {processed.get('circular_id')}")
            return processed
        except Exception as exc:
            logger.error(f"Error processing circular: {exc}", exc_info=True)
            return None

    def process_circular(self, circular_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract all relevant fields from a raw circular dict.

        Returns a structured result dict with the following keys (in addition
        to the standard CSV fields):

        ``event_id``
            Official GCN event ID taken from the ``eventId`` JSON field if
            present, otherwise the regex-derived ``event_name``.
        ``is_first_circular``
            ``True`` when the subject matches the facility-specific first-
            detection phrase.
        ``event_page_url``
            GCN event-page URL, e.g.
            ``https://gcn.nasa.gov/circulars/events/grb-260511b``.
        """
        try:
            subject    = circular_data.get("subject", "")
            body       = circular_data.get("body", "")
            circ_id    = circular_data.get("circularId")
            created_on = circular_data.get("createdOn")
            created_dt = datetime.fromtimestamp(int(created_on) / 1000, tz=timezone.utc)

            logger.info(f"Processing circular {circ_id}: {subject}")

            result: Dict[str, Any] = {
                "circular_id":       circ_id,
                "subject":           subject,
                "created_on":        created_dt.strftime("%Y-%m-%d_%H:%M:%S"),
                "processed_on":      datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H:%M:%S"),
                "event_id":          None,
                "event_name":        None,
                "facility":          None,
                "trigger_num":       None,
                "ra":                None,
                "dec":               None,
                "error":             None,
                "error_unit":        None,
                "redshift":          None,
                "redshift_error":    None,
                "host_info":         None,
                "false_trigger":     False,
                "is_first_circular": False,
                "event_page_url":    None,
            }

            # --- Official event_id (from JSON eventId field) ---
            event_id = circular_data.get("eventId")
            result["event_id"] = event_id

            # --- Event name (use official eventId, fall back to regex) ---
            if event_id:
                result["event_name"] = event_id
            else:
                for prefix, pattern in [
                    ("GRB",     r"(?:GRB|grb)\s*(\d{6}[A-Za-z])"),
                    ("EP",      r"(?:EP)\s*(\d{6}[A-Za-z])"),
                    ("IceCube", r"(?:IceCube)\s*(\d{6}[A-Za-z])"),
                ]:
                    m = re.search(pattern, subject, re.IGNORECASE)
                    if m:
                        result["event_name"] = f"{prefix} {m.group(1)}"
                        break
                if result["event_name"] is None:
                    logger.warning(f"process_circular: could not extract event_name for circular {circ_id} — subject: {subject!r}")

            # --- Facility ---
            # Use more of the body so follow-up circulars from optical telescopes
            # that reference the triggering instrument in passing can be identified.
            combined = subject + " " + body[:2000]
            facility_patterns = [
                (r"Swift[\s/\-]?XRT",              "SwiftXRT"),
                (r"Swift[\s/\-]?BAT",              "SwiftBAT"),
                (r"Swift[\s/\-]?UVOT",             "SwiftUVOT"),
                (r"Swift",                         "Swift"),
                (r"Fermi[\s/\-]?GBM",              "FermiGBM"),
                (r"Fermi[\s/\-]?LAT",              "FermiLAT"),
                (r"Fermi",                         "Fermi"),
                (r"Einstein[\s\-]?Probe|EP[\s\-]?[WF]XT", "EinsteinProbe"),
                (r"GECAM",                         "GECAM"),
                (r"SVOM|ECLAIRs",                  "SVOM"),
                (r"CALET",                         "CALET"),
                (r"HAWC",                          "HAWC"),
                (r"ICECUBE_Astrotrack_BRONZE",      "IceCubeBronze"),
                (r"ICECUBE_Astrotrack_GOLD",        "IceCubeGold"),
                (r"IceCube-Cascade",               "IceCubeCASCADE"),
                (r"ICECUBE",                       "IceCube"),
                (r"AMON",                          "AMON"),
            ]
            for pat, name in facility_patterns:
                if re.search(pat, combined, re.IGNORECASE):
                    result["facility"] = name
                    break

            # Fallback: infer facility from the GCN-provided eventId prefix when
            # the circular body belongs to a follow-up telescope (e.g. COLIBRÍ)
            # that doesn't explicitly name the triggering instrument.
            if result["facility"] is None and event_id:
                if re.match(r"EP\s*\d", event_id, re.IGNORECASE):
                    result["facility"] = "EinsteinProbe"
                    logger.info(
                        f"process_circular: inferred facility 'EinsteinProbe' "
                        f"from eventId '{event_id}' for circular {circ_id}"
                    )
                elif re.match(r"IceCube", event_id, re.IGNORECASE):
                    result["facility"] = "IceCube"
                    logger.info(
                        f"process_circular: inferred facility 'IceCube' "
                        f"from eventId '{event_id}' for circular {circ_id}"
                    )

            if result["facility"] is None:
                logger.warning(f"process_circular: could not identify facility for circular {circ_id} — subject: {subject!r}")

            # --- Trigger number (facility-specific) ---
            result["trigger_num"] = self._extract_trigger_num(
                combined, result["facility"]
            )
            # If not found in this circular's text, look it up via referenced
            # GCN circular IDs cited in the body (e.g. "GCN 44718").
            if result["trigger_num"] is None and result["facility"]:
                result["trigger_num"] = self._resolve_trigger_from_csv_references(
                    body, result["facility"]
                )

            # --- False trigger (check subject first, then body) ---
            result["false_trigger"] = self._detect_false_trigger(subject, body)

            # --- is_first_circular ---
            result["is_first_circular"] = self._is_first_circular(
                result["facility"], subject
            )

            # --- Event page URL ---
            result["event_page_url"] = self._get_event_page_url(
                result["event_id"] or result["event_name"]
            )

            # --- Coordinates ---
            coord_patterns: List[str] = []
            if result["facility"] and "Swift" in result["facility"] and "XRT" in result["facility"]:
                coord_patterns = [
                    r"Enhanced Swift-XRT position.*?RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                    r"RA,\s*Dec[:\s]*=?\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcsec]+)",
                ]
            elif result["facility"] and "Swift" in result["facility"] and "BAT" in result["facility"]:
                coord_patterns = [
                    r"BAT.*?RA,\s*Dec\s*([\d.]+),\s*([-+]?[\d.]+).*?uncertainty\s+of\s+([\d.]+)\s*([\"\'arcmin]+)",
                ]

            coord_patterns.extend([
                r"RA,\s*Dec\s*=\s*([\d.]+),\s*([-+]?[\d.]+).*?(?:uncertainty|error)\s+(?:of\s+)?([\d.]+)\s*([\"\'arcsecmindeg]+)?",
                r"R\.A\.,\s*Dec\.\s+([\d.]+),\s*([-+]?[\d.]+)",
                r"RA[:\s=]*([\d.]+)[,\s]+Dec[:\s=]*([-+]?[\d.]+)",
            ])

            for pat in coord_patterns:
                m = re.search(pat, body, re.IGNORECASE | re.DOTALL)
                if m:
                    groups = m.groups()
                    try:
                        result["ra"]  = float(groups[0])
                        result["dec"] = float(groups[1])
                        if len(groups) > 2 and groups[2]:
                            result["error"] = float(groups[2])
                            result["error_unit"] = groups[3] if len(groups) > 3 else "arcsec"
                        break
                    except (ValueError, IndexError):
                        continue

            # --- Redshift ---
            for pat in [
                r"redshift\s+(?:of\s+)?z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                r"at\s+z\s*[=~]\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
                r"z\s*=\s*([\d.]+)(?:\s*(?:±|\+/-)\s*([\d.]+))?",
            ]:
                m = re.search(pat, body, re.IGNORECASE)
                if m:
                    try:
                        result["redshift"] = float(m.group(1))
                        if m.group(2):
                            result["redshift_error"] = float(m.group(2))
                        break
                    except (ValueError, IndexError):
                        continue

            # --- Host info ---
            m = re.search(r"host\s+galaxy[^.]+", body, re.IGNORECASE)
            if m:
                result["host_info"] = m.group(0).strip()

            logger.info(f"Finished processing circular {circ_id}")
            return result

        except Exception as exc:
            logger.error(f"Error processing circular: {exc}", exc_info=True)
            return {
                "circular_id":       circular_data.get("circularId"),
                "subject":           circular_data.get("subject", ""),
                "created_on":        (
                    datetime.fromtimestamp(int(circular_data["createdOn"]) / 1000, tz=timezone.utc)
                    .strftime("%Y-%m-%d_%H:%M:%S")
                    if circular_data.get("createdOn") else ""
                ),
                "processed_on":      datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H:%M:%S"),
                "event_id":          circular_data.get("eventId"),
                "event_name":        None,
                "facility":          None,
                "trigger_num":       None,
                "false_trigger":     False,
                "is_first_circular": False,
                "event_page_url":    None,
            }

    # ==================================================================
    # Private: parsing helpers
    # ==================================================================

    def _extract_trigger_num(
        self, combined: str, facility: Optional[str]
    ) -> Optional[str]:
        """Return the trigger number appropriate for the given facility."""
        if not facility:
            logger.debug("_extract_trigger_num: facility is None; skipping trigger extraction")
            return None

        if facility == "SVOM":
            # e.g. "SVOM burst-id sb26051001" or "(SVOM burst-id sb26051001)"
            m = re.search(r"SVOM\s+burst-id\s+(sb\w+)", combined, re.IGNORECASE)
            if m:
                return m.group(1)
            m = re.search(r"burst-id\s+(sb\w+)", combined, re.IGNORECASE)
            if m:
                return m.group(1)
            logger.debug("_extract_trigger_num: no SVOM burst-id pattern matched")
            return None

        if facility == "EinsteinProbe":
            for pat in [
                # Explicit WXT/FXT ID forms
                r"triggered\s+EP-WXT\s+\(ID:\s*(\d+)\)",
                r"EP-WXT\s+\(ID:\s*(\d+)\)",
                r"EP/WXT\s+\(ID:\s*(\d+)\)",
                r"EP-WXT\s+trigger\s+(\d+)",
                r"EP/WXT\s+trigger\s+(\d+)",
                r"WXT\s+trigger\s+(\d+)",
                r"WXT\s+(?:source|ID)\s*[:#]?\s*(\d+)",
                r"EP-WXT\s+trigger\s+ID\s*[:#]?\s*(\d+)",
                # Generic "EP trigger …" or "Einstein Probe … ID …"
                r"(?:Einstein\s*Probe|EP)\b.{0,120}\bID\s*[:#]?\s*(\d+)",
            ]:
                m = re.search(pat, combined, re.IGNORECASE)
                if m:
                    return m.group(1)
            logger.debug("_extract_trigger_num: no EinsteinProbe WXT ID pattern matched")
            return None

        if "IceCube" in facility:
            logger.debug("_extract_trigger_num: IceCube circulars have no trigger number; matched by event_id/Name only")
            return None  # IceCube circulars matched by event_id/Name only

        if "Swift" in facility:
            for pat in [
                r"\(trigger\s*=\s*(\d+)\)",
                r"trigger\s*[:#]?\s*(\d+)",
                r"BAT\s+trigger\s*#?(\d+)",
            ]:
                m = re.search(pat, combined, re.IGNORECASE)
                if m:
                    return m.group(1)
            logger.debug(f"_extract_trigger_num: no Swift trigger pattern matched for facility '{facility}'")
            return None

        if "Fermi" in facility:
            # Prefer the integer part of the fractional MET trigger:
            # "trigger 800190036.70394 / 260511459" → "800190036"
            m = re.search(r"trigger\s+([\d]+)\.\d+\s*/\s*\d+", combined, re.IGNORECASE)
            if m:
                return m.group(1)
            for pat in [
                r"trigger\s+[Nn]o\.?\s+(\d+)",   # "trigger No 801580882" (MASTER-Net style)
                r"trigger\s+(\d+)/?(?:\d+)?",
                r"GBM\s+trigger\s+(\d+)",
            ]:
                m = re.search(pat, combined, re.IGNORECASE)
                if m:
                    return m.group(1)
            logger.debug(f"_extract_trigger_num: no Fermi trigger pattern matched for facility '{facility}'")
            return None

        # Generic fallback
        m = re.search(r"trigger\s*[:#]?\s*(\d+)", combined, re.IGNORECASE)
        if not m:
            logger.debug(f"_extract_trigger_num: no generic trigger pattern matched for facility '{facility}'")
            return None
        return m.group(1)

    def _resolve_trigger_from_csv_references(
        self, body: str, facility: str
    ) -> Optional[str]:
        """Resolve trigger_num by looking up GCN circulars cited in *body*.

        A follow-up circular (e.g. optical telescope) often doesn't state the
        original trigger number but instead cites the first-detection circular
        as "GCN NNNNN".  We extract **all** such references (a body may cite
        several circulars from different teams) and return the trigger_num from
        the first cited circular that matches *facility* and has a non-empty
        trigger_num stored in our local CSV.
        """
        if not body or not facility or not os.path.exists(self.output_csv):
            return None
        cited_ids = re.findall(r"GCN\s+(?:Circ(?:ular)?\.?\s+)?(\d+)", body, re.IGNORECASE)
        if not cited_ids:
            return None
        try:
            df = pd.read_csv(self.output_csv, dtype=str, na_filter=False)
            fac_norm = facility.lower()
            for cid in cited_ids:
                for _, row in df[df["circular_id"] == cid].iterrows():
                    row_fac = str(row.get("facility", "")).lower()
                    if not row_fac:
                        continue
                    if fac_norm in row_fac or row_fac in fac_norm:
                        trigger = str(row.get("trigger_num", "")).strip()
                        if trigger and trigger not in ("", "nan"):
                            logger.info(
                                f"_resolve_trigger_from_csv_references: resolved "
                                f"trigger {trigger!r} via cited circular {cid}"
                            )
                            return trigger
        except Exception as exc:
            logger.debug(f"_resolve_trigger_from_csv_references failed: {exc}")
        return None

    def _detect_false_trigger(self, subject: str, body: str) -> bool:
        """Return True if the circular reports a false trigger.

        Checks the subject first (many EP/SVOM retractions put the verdict
        there), then falls back to the body.
        """
        subject_patterns = [
            r"is\s+not\s+a\s+(?:real\s+)?(?:GRB|source)",
            r"is\s+(?:likely\s+)?a\s+(?:flaring\s+star|stellar\s+flare)",
            r"is\s+not\s+due\s+to",
            r"not\s+a\s+GRB",
            r"retraction",
        ]
        for pat in subject_patterns:
            if re.search(pat, subject, re.IGNORECASE):
                return True

        body_patterns = [
            r"not\s+(?:due\s+to\s+)?(?:a\s+)?GRB",
            r"false\s+(?:positive|trigger|alarm)",
            r"not\s+a\s+(?:real\s+)?(?:burst|GRB)",
            r"(?:likely\s+)?(?:due\s+to|caused\s+by)\s+(?:local\s+particles|SAA|background)",
            r"retraction",
        ]
        for pat in body_patterns:
            if re.search(pat, body, re.IGNORECASE):
                return True

        return False

    def _is_first_circular(self, facility: Optional[str], subject: str) -> bool:
        """Return True when *subject* matches the first-detection pattern for *facility*."""
        if not facility or not subject:
            return False
        for fac_key, pattern in self._FIRST_CIRCULAR_PATTERNS.items():
            if fac_key.lower() in facility.lower():
                if re.search(pattern, subject, re.IGNORECASE):
                    return True
        return False

    def _get_event_page_url(self, event_id: Optional[str]) -> Optional[str]:
        """Convert an official event ID to its GCN event-page URL.

        Examples::

            "GRB 260511B"     → "https://gcn.nasa.gov/circulars/events/grb-260511b"
            "EP260321a"       → "https://gcn.nasa.gov/circulars/events/ep260321a"
            "IceCube-260504A" → "https://gcn.nasa.gov/circulars/events/icecube-260504a"
        """
        if not event_id:
            logger.debug("_get_event_page_url: event_id is None or empty; cannot build event page URL")
            return None
        slug = event_id.lower().strip()
        slug = slug.replace(" ", "-")
        slug = re.sub(r"[^a-z0-9\-]", "", slug)
        if not slug:
            logger.warning(f"_get_event_page_url: slug is empty after cleaning '{event_id}'; cannot build event page URL")
            return None
        return f"https://gcn.nasa.gov/circulars/events/{slug}"

    # ==================================================================
    # Private: database updates
    # ==================================================================

    def _update_databases(self, data: Dict[str, Any]) -> None:
        with self.file_lock:
            try:
                self._update_csv(data)

                has_coords  = data.get("ra") is not None and data.get("dec") is not None
                has_trigger = data.get("trigger_num") is not None
                has_name    = bool(data.get("event_name") or data.get("event_id")) and data.get("facility")

                if has_name and has_trigger:
                    if data.get("false_trigger"):
                        self._remove_false_trigger(data)
                    elif has_coords:
                        self._update_ascii(data)
                    else:
                        logger.debug(
                            f"_update_databases: skipping ASCII update for circular "
                            f"{data.get('circular_id')} — no coordinates (ra/dec)"
                        )
                else:
                    logger.debug(
                        f"_update_databases: skipping ASCII update for circular "
                        f"{data.get('circular_id')} — "
                        f"has_name={has_name}, has_trigger={has_trigger}"
                    )

            except Exception as exc:
                logger.error(f"Error updating databases: {exc}", exc_info=True)

    def _update_csv(self, data: Dict[str, Any]) -> None:
        try:
            if os.path.exists(self.output_csv):
                df = pd.read_csv(self.output_csv)
            else:
                df = pd.DataFrame(columns=self.CSV_COLUMNS)

            new_row = {col: data.get(col) for col in self.CSV_COLUMNS}
            if new_row.get("trigger_num") is not None:
                new_row["trigger_num"] = str(new_row["trigger_num"])
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df.to_csv(self.output_csv, index=False)
            logger.info(f"CSV updated with circular {data.get('circular_id')}")
        except Exception as exc:
            logger.error(f"Error updating circular CSV: {exc}", exc_info=True)

    def _update_ascii(self, data: Dict[str, Any]) -> None:
        import csv as _csv
        try:
            if os.path.exists(self.output_ascii):
                df = pd.read_csv(
                    self.output_ascii,
                    sep=r"\s+",
                    quotechar='"',
                    header=0,
                    dtype=str,
                    na_filter=False,
                )
            else:
                df = pd.DataFrame(columns=self.ASCII_COLUMNS)

            for col in self.ASCII_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df = df[self.ASCII_COLUMNS].fillna("")

            # Convert error to degrees
            error_deg = data.get("error")
            if error_deg is not None and data.get("error_unit"):
                unit = data["error_unit"].lower()
                if "arcsec" in unit:
                    error_deg = error_deg / 3600.0
                elif "arcmin" in unit:
                    error_deg = error_deg / 60.0

            now        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            event_id   = data.get("event_id") or data.get("event_name")
            event_name = data.get("event_name") or event_id
            facility   = data["facility"]

            # Two-pass search:
            # Pass 1 — match by official event_id / event_name in the Name column
            existing_idx: Optional[int] = None
            if event_id:
                for idx, row in df.iterrows():
                    row_name = str(row.get("Name", "")).strip().strip('"')
                    if row_name.lower() == event_id.lower():
                        existing_idx = idx
                        break

            # Pass 2 — match by Trigger_num + facility (case-insensitive substring)
            if existing_idx is None and data.get("trigger_num"):
                trigger = str(data["trigger_num"])
                fac_lc  = facility.lower()
                for idx, row in df.iterrows():
                    row_trigger = str(row.get("Trigger_num", "")).strip().strip('"')
                    if row_trigger != trigger:
                        continue
                    facs = [f.strip().lower() for f in str(row.get("All_Facilities", "")).split(",")]
                    if any(fac_lc in f or f in fac_lc for f in facs):
                        existing_idx = idx
                        if event_id:
                            df.at[idx, "Name"] = event_id  # clean value, no embedded quotes
                            logger.info(
                                f"Updated ASCII Name from provisional to '{event_id}' "
                                f"(matched by trigger {trigger})"
                            )
                        break

            if existing_idx is not None:
                current_best  = str(df.at[existing_idx, "Best_Facility"]).strip().strip('"')
                current_all   = str(df.at[existing_idx, "All_Facilities"]).strip().strip('"')
                current_err_s = str(df.at[existing_idx, "Error"]).strip().strip('"')
                current_err   = float(current_err_s) if current_err_s not in ("", "N/A") else None

                # Update All_Facilities
                if facility not in current_all:
                    df.at[existing_idx, "All_Facilities"] = (
                        f"{current_all},{facility}" if current_all else facility
                    )

                # Upgrade Best_Facility?
                if self._should_upgrade(current_best, current_err, facility, error_deg):
                    df.at[existing_idx, "Best_Facility"] = facility
                    if data.get("ra") is not None and data.get("dec") is not None:
                        df.at[existing_idx, "RA"]  = f"{data['ra']:.3f}"
                        df.at[existing_idx, "DEC"] = f"{data['dec']:.3f}"
                        df.at[existing_idx, "Error"] = f"{error_deg:.3f}" if error_deg is not None else "N/A"
                    if data.get("trigger_num"):
                        df.at[existing_idx, "Trigger_num"] = data["trigger_num"]

                # Append circular ID to GCN_ID list
                current_gcn = str(df.at[existing_idx, "GCN_ID"]).strip().strip('"')
                cid = str(data["circular_id"])
                if cid not in current_gcn.split(","):
                    df.at[existing_idx, "GCN_ID"] = f"{current_gcn},{cid}" if current_gcn else cid

                df.at[existing_idx, "Last_Update"] = now

                if data.get("redshift") is not None:
                    df.at[existing_idx, "Redshift"] = f"{data['redshift']:.1f}"
                if data.get("host_info"):
                    df.at[existing_idx, "Host_info"] = str(data["host_info"])  # clean value

            else:
                # Add new row only when we have all required fields
                if data.get("ra") is not None and data.get("dec") is not None and data.get("trigger_num"):
                    new_row = {col: "" for col in self.ASCII_COLUMNS}
                    new_row.update({
                        "GCN_ID":           str(data["circular_id"]),
                        "Name":             event_name or "",  # clean value, no embedded quotes
                        "RA":               f"{data['ra']:.3f}",
                        "DEC":              f"{data['dec']:.3f}",
                        "Error":            f"{error_deg:.3f}" if error_deg is not None else "",
                        "Best_Facility":    facility,
                        "All_Facilities":   facility,
                        "Trigger_num":      str(data["trigger_num"]),
                        "Notice_date":      now,
                        "Last_Update":      now,
                        "Redshift":         f"{data['redshift']:.1f}" if data.get("redshift") is not None else "",
                        "Host_info":        str(data["host_info"]) if data.get("host_info") else "",
                    })
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                else:
                    logger.warning(
                        f"_update_ascii: skipping new row for circular {data.get('circular_id')} "
                        f"({event_name or event_id!r}) — missing required field(s): "
                        f"ra={data.get('ra')}, dec={data.get('dec')}, trigger_num={data.get('trigger_num')}"
                    )

            # Trim
            if len(df) > self.ascii_max_events:
                df = df.sort_values("Last_Update", ascending=False).head(self.ascii_max_events)

            # Backup + write (same format as notice_handler: space-separated, QUOTE_NONNUMERIC)
            self._create_backup(self.output_ascii)
            df[self.ASCII_COLUMNS].to_csv(
                self.output_ascii,
                sep=" ",
                header=True,
                index=False,
                quoting=_csv.QUOTE_NONNUMERIC,
                quotechar='"',
                columns=self.ASCII_COLUMNS,
            )
            logger.info("ASCII database updated from circular")

        except Exception as exc:
            logger.error(f"Error updating ASCII from circular: {exc}", exc_info=True)

    def _remove_false_trigger(self, data: Dict[str, Any]) -> None:
        import csv as _csv
        if not os.path.exists(self.output_ascii):
            return
        try:
            df = pd.read_csv(
                self.output_ascii,
                sep=r"\s+",
                quotechar='"',
                header=0,
                dtype=str,
                na_filter=False,
            )
            for col in self.ASCII_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            df = df[self.ASCII_COLUMNS].fillna("")

            trigger  = str(data.get("trigger_num", ""))
            facility = str(data.get("facility", ""))
            fac_lc   = facility.lower()

            rows_to_remove = []
            for idx, row in df.iterrows():
                if str(row.get("Trigger_num", "")).strip().strip('"') != trigger:
                    continue
                facs = [f.strip().lower() for f in str(row.get("All_Facilities", "")).split(",")]
                if any(fac_lc in f or f in fac_lc for f in facs):
                    rows_to_remove.append(idx)

            if rows_to_remove:
                df = df.drop(index=rows_to_remove)
                df[self.ASCII_COLUMNS].to_csv(
                    self.output_ascii,
                    sep=" ",
                    header=True,
                    index=False,
                    quoting=_csv.QUOTE_NONNUMERIC,
                    quotechar='"',
                    columns=self.ASCII_COLUMNS,
                )
                logger.info(f"Removed false trigger {facility}_{trigger} from ASCII")
        except Exception as exc:
            logger.error(f"Error removing false trigger: {exc}", exc_info=True)

    def _should_upgrade(
        self,
        current: str,
        current_err: Optional[float],
        new_fac: str,
        new_err: Optional[float],
    ) -> bool:
        """Return True if *new_fac* should become the Best_Facility."""
        cp  = self.FACILITY_PRIORITIES.get(current, 1) if current else 0
        np_ = self.FACILITY_PRIORITIES.get(new_fac, 1) if new_fac else 0
        if np_ > cp:
            return True
        if np_ < cp:
            return False
        # Same priority — prefer smaller error
        if current_err is not None and new_err is not None:
            return new_err < current_err
        return new_err is not None and current_err is None

    def _create_backup(self, filepath: str, max_backups: int = 5) -> None:
        if not os.path.exists(filepath):
            return
        try:
            backup = f"{filepath}.backup.{int(time.time())}"
            shutil.copy2(filepath, backup)
            backups = sorted(glob.glob(f"{filepath}.backup.*"), key=os.path.getmtime, reverse=True)
            for old in backups[max_backups:]:
                os.remove(old)
        except Exception as exc:
            logger.warning(f"Backup failed for {filepath}: {exc}")
