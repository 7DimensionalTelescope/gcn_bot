"""
MessageFormatter
================
Converts raw GCN notice payloads into Slack block-kit messages and
generates human-readable thread update messages.

All formatting logic that previously lived in ``gcn_bot.py`` is consolidated
here:

* ``format_notice()``           — main entry-point (classic text or JSON)
* ``format_connection_lost()``  — connection-lost alert blocks
* ``format_connection_restored()`` — connection-restored blocks
* ``format_thread_update()``    — compact diff message for existing events

Private helpers (not intended for external use):
    _filter_notice_text, _format_json_notice,
    _get_facility_name, _get_facility_emoji, _get_notice_url,
    _standardize_time_format, _calculate_time_diff
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import pytz
import voeventparse as vp

logger = logging.getLogger(__name__)


class MessageFormatter:
    """Stateless helper that converts GCN notices into Slack blocks."""

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def format_notice(
        self,
        topic: str,
        value: Union[str, bytes],
        csv_status: Optional[bool] = None,
        ascii_status: Optional[bool] = None,
        notice_data: Optional[Dict[str, Any]] = None,
        test_mode: bool = False,
        custom_facility: Optional[str] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        Format a raw GCN notice into a Slack message.

        Parameters
        ----------
        topic : str
        value : str | bytes
            Raw notice payload.
        csv_status : bool | None
            Whether the notice was saved to CSV (shown in footer).
        ascii_status : bool | None
            Whether the notice was saved to ASCII (shown in footer).
        notice_data : dict | None
            Parsed notice data (used to display probable GRB name).
        test_mode : bool
            When True, test notices are not skipped.
        custom_facility : str | None
            Override the auto-detected facility name.

        Returns
        -------
        tuple[dict | None, str | None, str | None]
            (slack_message, lc_url, notice_url)
            Returns ``(None, None, None)`` when the notice should be skipped.
        """
        try:
            facility = custom_facility or self._get_facility_name(topic)

            # Skip test notices unless test_mode
            if "_TEST" in topic.upper() and not test_mode:
                logger.info(f"Skipping test notice from {facility}")
                return None, None, None

            lc_url: Optional[str] = None

            # Parse notice content
            if "gcn.notices" in topic:
                if "svom.voevent" in topic:
                    formatted_text = self._format_svom_voevent(value)
                else:
                    try:
                        json_data = json.loads(value)
                        formatted_text = self._format_json_notice(json_data, facility)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON from {facility}")
                        formatted_text = value if isinstance(value, str) else value.decode("utf-8", "ignore")
            else:
                try:
                    formatted_text, lc_url = self._filter_notice_text(value, topic)
                except Exception as exc:
                    logger.warning(f"Failed to parse classic text from {facility}: {exc}")
                    formatted_text = value if isinstance(value, str) else value.decode("utf-8", "ignore")

                # Construct Swift LC URL if not found in text
                if lc_url is None and notice_data and notice_data.get("Trigger_num"):
                    lc_url = self._build_swift_lc_url(notice_data)

            if isinstance(formatted_text, bytes):
                formatted_text = formatted_text.decode("utf-8", "ignore")

            notice_url = self._get_notice_url(topic, value)

            # Inject probable GRB name into [BASIC INFO] section
            if notice_data and notice_data.get("Name"):
                formatted_text = self._inject_grb_name(formatted_text, notice_data["Name"])

            blocks: List[Dict[str, Any]] = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{self._get_facility_emoji(facility)} {facility}",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": formatted_text},
                },
            ]

            # Footer with storage status
            status_lines: List[str] = []
            if csv_status is not None:
                icon = "✅" if csv_status else "❌"
                label = "Parsed" if (csv_status and test_mode) else ("Saved" if csv_status else "Failed")
                status_lines.append(f"{icon} CSV Database: {label}")
            if ascii_status is not None:
                icon = "✅" if ascii_status else "❌"
                label = "Parsed" if (ascii_status and test_mode) else ("Saved" if ascii_status else "Failed")
                status_lines.append(f"{icon} ASCII Database: {label}")
            if status_lines:
                blocks.append(
                    {
                        "type": "context",
                        "elements": [{"type": "mrkdwn", "text": " | ".join(status_lines)}],
                    }
                )

            return {"blocks": blocks}, lc_url, notice_url

        except Exception as exc:
            logger.error(f"Error formatting notice: {exc}", exc_info=True)
            fac = custom_facility or self._get_facility_name(topic)
            raw = value if isinstance(value, str) else value.decode("utf-8", "ignore")
            return (
                {
                    "blocks": [
                        {
                            "type": "header",
                            "text": {"type": "plain_text", "text": f"New GCN Alert: {fac}"},
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Error formatting message:* {exc}\n\n```{raw[:1000]}```",
                            },
                        },
                    ]
                },
                None,
                None,
            )

    def format_connection_lost(
        self, last_heartbeat: datetime, elapsed_seconds: float
    ) -> Dict[str, Any]:
        """Build a Slack message for a lost GCN connection."""
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "⚠️ GCN Connection Alert"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "*Status:* Connection Lost\n"
                            f"*Last Heartbeat:* {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
                            f"*Elapsed:* {elapsed_seconds:.1f} seconds"
                        ),
                    },
                },
            ]
        }

    def format_connection_restored(self, last_heartbeat: datetime) -> Dict[str, Any]:
        """Build a Slack message for a restored GCN connection."""
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "✅ GCN Connection Restored"},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            "*Status:* Connection Restored\n"
                            f"*Restored At:* {last_heartbeat.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                        ),
                    },
                },
            ]
        }

    def format_thread_update(
        self,
        differences: Dict[str, Any],
        notice_data: Dict[str, Any],
    ) -> str:
        """
        Compose a compact Slack mrkdwn string showing only changed fields.

        Parameters
        ----------
        differences : dict
            As returned by ``GCNNoticeHandler.compare_event_data()``.
        notice_data : dict
            Incoming notice data (used for the facility name in the header).
        """
        try:
            facility = notice_data.get("Facility", "Unknown")
            lines: List[str] = [
                f"🔄 *UPDATE: {facility}*",
                "> *New Information:*",
            ]

            if "facility_change" in differences:
                ch = differences["facility_change"]
                lines.append(f"> - 🔬 *Instrument:* {ch['from']} → *{ch['to']}*")

            if "coordinates" in differences:
                coords = differences["coordinates"]
                parts: List[str] = []
                if "RA"    in coords: parts.append(f"RA={coords['RA']}")
                if "DEC"   in coords: parts.append(f"DEC={coords['DEC']}")
                if "Error" in coords: parts.append(f"±{coords['Error']}°")
                if parts:
                    lines.append(f"> - 📍 *Coordinates:* {', '.join(parts)}")

            if "visibility" in differences:
                vis   = differences["visibility"]
                case  = vis.get("case", vis.get("status", ""))  # support both key names
                msg   = vis.get("message", "")

                if case == "observable_now":
                    remaining = vis.get("remaining_hours", 0)
                    end_t     = vis.get("observable_end")
                    end_s     = end_t.strftime("%H:%M") if hasattr(end_t, "strftime") else str(end_t or "Unknown")
                    lines.append(f"> - 🌃 *Visibility:* 🟢 Observable until {end_s} CLT ({remaining:.1f}h remaining)")
                elif case == "observable_later":
                    hrs   = vis.get("hours_until_observable", 0)
                    start = vis.get("observable_start")
                    st_s  = start.strftime("%H:%M") if hasattr(start, "strftime") else str(start or "Unknown")
                    lines.append(f"> - 🌃 *Visibility:* 🟠 Observable in {hrs:.1f}h (from {st_s} CLT)")
                elif case == "observable_tomorrow":
                    lines.append("> - 🌃 *Visibility:* 🔵 Observable Tomorrow Night")
                else:
                    reason = vis.get("reason", msg or "Unknown limitation")
                    lines.append(f"> - 🌃 *Visibility:* 🔴 Not Observable ({reason})")

            if len(lines) <= 2:
                lines.append("> - ℹ️ *Status:* Updated information received")

            return "\n".join(lines)

        except Exception as exc:
            logger.error(f"Error formatting thread message: {exc}")
            return (
                f"🔄 *UPDATE: {notice_data.get('Facility', 'Unknown')}*\n"
                "> - ℹ️ *Status:* Updated information received"
            )

    # ------------------------------------------------------------------
    # Private: facility helpers
    # ------------------------------------------------------------------

    def _get_facility_name(self, topic: str) -> str:
        try:
            if "gcn.classic.text." in topic:
                parts = topic.split("text.")[1].split("_")
                if parts[0] in ("FERMI", "SWIFT"):
                    return f"{parts[0]}-{parts[1]}"
                elif parts[0] == "ICECUBE":
                    return topic.split("text.")[1]
                elif parts[0] in ("AMON", "HAWC"):
                    return parts[0]
                else:
                    return parts[0]
            elif "gcn.notices." in topic:
                return topic.split("notices.")[1].split(".")[0].upper()
            return topic
        except Exception as exc:
            logger.error(f"Error extracting facility name: {exc}")
            return topic

    def _get_facility_emoji(self, facility: str) -> str:
        fu = facility.upper()
        if "SWIFT" in fu:
            return "💥"
        if "FERMI" in fu:
            return "⚛️"
        if "EINSTEIN" in fu:
            return "👴"
        if "ICECUBE" in fu:
            return "❄️" if "CASCADE" in fu else "🧊"
        if "AMON" in fu:
            return "🔗"
        if "HAWC" in fu:
            return "🏔️"
        if "CALET" in fu:
            return "🛰️"
        if "SVOM" in fu:
            return "🛸"
        return "📡"

    # ------------------------------------------------------------------
    # Private: notice text filtering (classic text format)
    # ------------------------------------------------------------------

    def _filter_notice_text(
        self, text: Union[str, bytes], topic: str
    ) -> Tuple[str, Optional[str]]:
        if isinstance(text, bytes):
            text = text.decode("utf-8")

        lines = text.splitlines()

        exclude_patterns = [
            r"RECORD_NUM", r"SUN_POSTN", r"SUN_DIST",
            r"MOON_POSTN", r"MOON_DIST", r"MOON_ILLUM",
            r"GAL_COORDS", r"ECL_COORDS",
            r"AMP[0-3]", r"WAVEFORM", r"TAM\[0-3\]",
            r"PKT_SER_NUM", r"PKT_HOP_CNT", r"PKT_SOD",
            r"RETRACTION", r"SC_LON_LAT",
            r"LC_URL", r"LOC_URL", r"SKYMAP_\w+_URL",
            r"GRB_PHI", r"GRB_THETA",
            r"SOLN", r"MERIT_PARAMS",
            r"BKG_INTEN", r"BKG_TIME", r"BKG_DUR",
            r"LOC_ALGORITHM", r"E_RANGE", r"POS_MAP_URL",
            r"DATA_INTERVAL", r"AMPLIFIER",
            r"COMMENTS:", r"TRIGGER_INDEX",
            r"TITLE", r"SRC_ERROR50", r"REVISION",
        ]

        sections: Dict[str, List[str]] = {"basic": [], "location": [], "timing": [], "analysis": []}
        current_section = "basic"

        notice_date: Optional[str] = None
        trigger_time: Optional[str] = None
        notice_date_line: Optional[str] = None
        trigger_time_line: Optional[str] = None
        combined_date_time: Optional[str] = None
        lc_url: Optional[str] = None

        # First pass: extract key values
        for line in lines:
            if "NOTICE_DATE:" in line:
                m = re.search(r"NOTICE_DATE:\s*(.+)", line)
                if m:
                    notice_date = m.group(1)
                    notice_date_line = line

            time_brace = re.search(
                r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_TIME:.*?{([\d:\.]+)}\s*UT", line
            )
            if time_brace:
                trigger_time = time_brace.group(1)
                trigger_time_line = line
                continue

            time_no_brace = re.search(
                r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_TIME:\s*([\d:\.]+)\s*UT", line
            )
            if time_no_brace:
                trigger_time = time_no_brace.group(1)
                trigger_time_line = line
                continue

            if "LC_URL:" in line:
                m = re.search(r"LC_URL:\s*([^\s]+)", line)
                if m:
                    lc_url = m.group(1).replace("medres34", "all").replace("http://", "https://")

        # Second pass: build sections
        for line in lines:
            # Capture GRB type classification from COMMENTS lines before they are excluded
            if "COMMENTS:" in line:
                grb_class = re.search(
                    r"(?:long|short)[- ]?(?:GRB|duration)|likely a GRB",
                    line, re.IGNORECASE
                )
                if grb_class:
                    comment_text = re.sub(r"^.*?COMMENTS:\s*", "", line).strip()
                    if comment_text:
                        sections["analysis"].append(f"GRB_TYPE:      {comment_text}")
                continue  # always skip COMMENTS: lines otherwise

            if any(re.search(p, line) for p in exclude_patterns):
                continue
            if "current" in line or "1950" in line:
                continue
            if any(c in line for c in ("GRB_RA", "GRB_DEC", "SRC_RA", "SRC_DEC", "POINT_RA", "POINT_DEC")):
                if "J2000" not in line:
                    continue

            # NOTICE_DATE
            if line == notice_date_line and notice_date:
                sections["basic"].append(
                    f"NOTICE_DATE:      {self._standardize_time_format(notice_date)}"
                )
                continue

            # Date line — store for pairing with time
            if any(d in line for d in ("GRB_DATE:", "TRIGGER_DATE:", "DISCOVERY_DATE:", "EVENT_DATE:", "IMG_START_DATE:")):
                m = re.search(r"(?:GRB|TRIGGER|DISCOVERY|EVENT|IMG_START)_DATE:.*?(\d{2})/(\d{2})/(\d{2})", line)
                if m:
                    combined_date_time = f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
                continue

            # Time line
            if line == trigger_time_line and trigger_time and notice_date:
                today = datetime.now(tz=timezone.utc).strftime("%y/%m/%d")
                full_t = f"{combined_date_time} {trigger_time}" if combined_date_time else f"{today} {trigger_time}"
                std_t  = self._standardize_time_format(full_t)
                diff_t = self._calculate_time_diff(notice_date, full_t)

                field = self._extract_time_field(line)
                sections["timing"].append(f"{field}:      {std_t}")
                sections["timing"].append(f"{field}_DIFF:      {diff_t}")
                combined_date_time = None
                continue

            # Section routing
            if any(x in line for x in ("NOTICE_DATE:", "TRIGGER_NUM:", "EVENT_NUM:", "RUN_NUM:", "STREAM:", "ID:")):
                current_section = "basic"
            elif any(x in line for x in ("GRB_RA:", "GRB_DEC:", "SRC_RA:", "SRC_DEC:", "POINT_RA:", "POINT_DEC:", "RA:", "DEC:", "GRB_ERROR:", "SRC_ERROR", "RA_DEC_ERROR")):
                current_section = "location"
            elif any(x in line for x in ("TRIGGER_DATE:", "TRIGGER_TIME:", "DISCOVERY_DATE:", "DISCOVERY_TIME:", "IMG_START_DATE:", "IMG_START_TIME:", "GRB_DATE:", "GRB_TIME:")):
                current_section = "timing"
            elif any(x in line for x in ("ENERGY:", "SIGNALNESS:", "FAR:", "COINCIDENCE:", "SIGNIFICANCE:", "DELTA_T:", "COINC_PAIR:", "GRB_INTEN:", "GRB_SIGNIF:", "GRB_MAG:", "RATE_SIGNIF:", "IMAGE_SIGNIF:", "CHARGE:", "IMAGE_SNR:", "SNR:", "TRIGGER_DUR:", "LC_URL:")):
                current_section = "analysis"

            if line.strip():
                if current_section == "location":
                    line = self._simplify_location_line(line)
                sections[current_section].append(line)

        # Assemble
        formatted = ""
        if sections["basic"]:
            formatted += "*[BASIC INFO]*\n> " + "\n> ".join(sections["basic"]) + "\n\n"
        if sections["location"]:
            formatted += "*[LOCATION]*\n> " + "\n> ".join(sections["location"]) + "\n\n"
        if sections["timing"]:
            formatted += "*[TIMING]*\n> " + "\n> ".join(sections["timing"]) + "\n\n"
        if sections["analysis"]:
            formatted += "*[ANALYSIS]*\n> " + "\n> ".join(sections["analysis"]) + "\n\n"

        formatted = self._apply_bold_keys(formatted, self._TEXT_KEY_FIELDS)
        formatted = re.sub(r"(Long GRB|long GRB)", r"*\1*", formatted)
        formatted = re.sub(r"(Short GRB|short GRB)", r"*\1*", formatted)
        formatted = re.sub(r"(likely a GRB)", r"*\1*", formatted)

        return formatted.strip(), lc_url

    _TEXT_KEY_FIELDS = [
        "GRB_RA:", "SRC_RA:", "POINT_RA:", "RA:",
        "GRB_DEC:", "SRC_DEC:", "POINT_DEC:", "DEC:",
        "GRB_ERROR:", "SRC_ERROR", "RA_DEC_ERROR",
        "TRIGGER_TIME:", "GRB_TIME:", "GRB_DATETIME:", "DISCOVERY_TIME:", "IMG_START_TIME:",
        "TRIGGER_DATE:", "DISCOVERY_DATE:", "IMG_START_DATE:",
        "GRB_TIME_DIFF:", "TRIGGER_TIME_DIFF:", "EVENT_TIME_DIFF:",
        "ENERGY:", "SIGNALNESS:", "FAR:", "COINC_PAIR:", "PVALUE:",
        # "SIGNIFICANCE:", "GRB_INTEN:", "GRB_MAG:", "GRB_SIGNIF:", "DATA_SIGNIF:", "IMAGE_SIGNIF:",
        "IMAGE_SNR:", "SNR:", "CHARGE:", "NET_COUNT_RATE:",
        "DELTA_T:", "SIGMA_T:", "SIGNAL_TRACKNESS:", "TRIGGER_DUR:",
        "NOTICE_DATE:", "TRIGGER_NUM:", "EVENT_NUM:", "ID:",
        "GRB_TYPE:",
    ]

    def _apply_bold_keys(self, text: str, key_fields: List[str]) -> str:
        result = text.split("\n")
        headers = {"*[BASIC INFO]*", "*[LOCATION]*", "*[TIMING]*", "*[ANALYSIS]*"}
        for i, line in enumerate(result):
            if any(h in line for h in headers):
                continue
            if (line.startswith("> ") or line.startswith(">")) and any(k in line for k in key_fields):
                content = line[2:] if line.startswith("> ") else line[1:]
                result[i] = f"> *{content}*"
        return "\n".join(result)

    @staticmethod
    def _extract_time_field(line: str) -> str:
        m = re.search(r"(\w+_TIME):", line)
        return m.group(1) if m else "TIME"

    @staticmethod
    def _simplify_location_line(line: str) -> str:
        """Reduce RA/DEC/Error lines to 'FIELD:      VALUE deg'."""
        m = re.match(
            r"\s*(GRB_RA|SRC_RA|POINT_RA|GRB_DEC|SRC_DEC|POINT_DEC|RA|DEC):\s*([-+]?\d+\.\d+)",
            line,
        )
        if m:
            return f"{m.group(1)}:      {m.group(2)} deg"

        m = re.search(r"(GRB_ERROR|SRC_ERROR|RA_DEC_ERROR):\s*([\d.]+)\s*\[(\w+)", line)
        if m:
            value = float(m.group(2))
            unit = m.group(3).lower()
            if unit == "arcmin":
                value = round(value / 60.0, 4)
            elif unit == "arcsec":
                value = round(value / 3600.0, 4)
            return f"{m.group(1)}:      {value} deg"

        return line

    # ------------------------------------------------------------------
    # Private: SVOM VOEvent formatting
    # ------------------------------------------------------------------

    def _format_svom_voevent(self, xml_text: Union[str, bytes]) -> str:
        """Convert a SVOM VOEvent XML payload to a Slack mrkdwn string."""
        raw = xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text
        try:
            v = vp.loads(raw)
        except Exception as exc:
            logger.error(f"Failed to parse SVOM VOEvent for display: {exc}")
            return f"```{raw.decode('utf-8', 'ignore')[:500]}```"

        # IVORN → notice type label
        ivorn = v.attrib.get("ivorn", "")
        fragment = ivorn.split("#", 1)[-1]
        after_id = fragment.split("_", 1)[1] if "_" in fragment else fragment
        notice_type = after_id.split("_")[0]  # "grm-trigger", "eclairs-wakeup", "mxt-initial" …

        # Notice date
        notice_date_str: Optional[str] = None
        try:
            t = v.Who.Date.text.strip()
            if "T" in t and not t.endswith("Z") and "+" not in t:
                t += "Z"
            notice_date_str = t
        except AttributeError:
            pass

        # Grouped params
        burst_id: Optional[str] = None
        notice_level: Optional[str] = None
        instrument: Optional[str] = None
        detection_params: Dict[str, Dict[str, str]] = {}
        try:
            grouped = vp.get_grouped_params(v)
            for gname, gparams in grouped.allitems():
                if gname == "Svom_Identifiers":
                    for pname, pattrs in gparams.allitems():
                        if pname == "Burst_Id":
                            burst_id = pattrs.get("value")
                        elif pname == "Notice_Level":
                            notice_level = pattrs.get("value")
                elif gname == "Detection_Info":
                    for pname, pattrs in gparams.allitems():
                        detection_params[pname] = pattrs
        except Exception as exc:
            logger.warning(f"Error reading SVOM grouped params: {exc}")

        try:
            toplevel = vp.get_toplevel_params(v)
            instrument = toplevel["Instrument"]["value"]
        except (KeyError, AttributeError, TypeError):
            pass

        # Trigger time
        trigger_time_str: Optional[str] = None
        try:
            dt = vp.get_event_time_as_utc(v)
            if dt:
                trigger_time_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
        except Exception:
            pass

        # Sky position (optional)
        ra: Optional[float] = None
        dec: Optional[float] = None
        error: Optional[float] = None
        try:
            pos = vp.get_event_position(v)
            ra, dec, error = pos.ra, pos.dec, pos.err
        except Exception:
            pass

        # Probability (Why element is absent in some ECLAIRs VOEvents)
        try:
            probability = v.Why.Inference.attrib.get('probability', 'N/A')
        except AttributeError:
            probability = 'N/A'

        # Assemble sections
        sections: Dict[str, List[str]] = {
            "basic": [], "timing": [], "location": [], "analysis": []
        }

        now_str = datetime.now(tz=timezone.utc).strftime("%a %d %b %y %H:%M:%S")
        date_src = notice_date_str if notice_date_str else now_str
        sections["basic"].append(
            f"NOTICE_DATE:      {self._standardize_time_format(date_src)}"
        )
        if burst_id:
            sections["basic"].append(f"BURST_ID:      {burst_id}")
        if notice_level:
            sections["basic"].append(f"NOTICE_LEVEL:      {notice_level}")
        if instrument:
            sections["basic"].append(f"INSTRUMENT:      {instrument}")
        if notice_type:
            sections["basic"].append(f"NOTICE_TYPE:      {notice_type}")

        if trigger_time_str:
            sections["timing"].append(
                f"TRIGGER_TIME:      {self._standardize_time_format(trigger_time_str)}"
            )
            sections["timing"].append(
                f"TRIGGER_TIME_DIFF:      {self._calculate_time_diff(now_str, trigger_time_str)}"
            )

        if ra is not None:
            sections["location"].append(f"RA:      {ra} deg")
        if dec is not None:
            sections["location"].append(f"DEC:      {dec} deg")
        if error is not None:
            sections["location"].append(f"ERROR_RADIUS:      {error} deg")

        snr = detection_params.get("SNR")
        if snr:
            unit = snr.get("unit", "sigma")
            sections["analysis"].append(f"SNR:      {snr.get('value', '')} {unit}".rstrip())
        ts = detection_params.get("Timescale")
        if ts:
            unit = ts.get("unit", "s")
            sections["analysis"].append(f"TIMESCALE:      {ts.get('value', '')} {unit}".rstrip())
        e_lo = detection_params.get("Lower_Energy_Bound")
        e_hi = detection_params.get("Upper_Energy_Bound")
        if e_lo and e_hi:
            sections["analysis"].append(
                f"ENERGY_RANGE:      {e_lo.get('value', '')} - {e_hi.get('value', '')} keV"
            )
        if probability:
            sections["analysis"].append(f"PROBABILITY:      {probability}")

        formatted = ""
        if sections["basic"]:
            formatted += "*[BASIC INFO]*\n> " + "\n> ".join(sections["basic"]) + "\n\n"
        if sections["timing"]:
            formatted += "*[TIMING]*\n> " + "\n> ".join(sections["timing"]) + "\n\n"
        if sections["location"]:
            formatted += "*[LOCATION]*\n> " + "\n> ".join(sections["location"]) + "\n\n"
        if sections["analysis"]:
            formatted += "*[ANALYSIS]*\n> " + "\n> ".join(sections["analysis"]) + "\n\n"

        svom_key_fields = [
            "NOTICE_DATE:", "BURST_ID:", "NOTICE_LEVEL:", "INSTRUMENT:", "NOTICE_TYPE:",
            "TRIGGER_TIME:", "TRIGGER_TIME_DIFF:",
            "RA:", "DEC:", "ERROR_RADIUS:",
            "SNR:", "TIMESCALE:", "ENERGY_RANGE:", "PROBABILITY:",
        ]
        return self._apply_bold_keys(formatted, svom_key_fields).strip()

    # ------------------------------------------------------------------
    # Private: JSON notice formatting
    # ------------------------------------------------------------------

    def _format_json_notice(self, json_data: Dict[str, Any], facility: str) -> str:
        sections: Dict[str, List[str]] = {"basic": [], "location": [], "timing": [], "analysis": []}

        now_str = datetime.now(tz=timezone.utc).strftime("%a %d %b %y %H:%M:%S")
        sections["basic"].append(f"NOTICE_DATE:      {self._standardize_time_format(now_str)}")

        if "EINSTEIN_PROBE" in facility.upper() or "EINSTEIN" in facility.upper():
            if "id" in json_data:
                id_val = json_data["id"][0] if isinstance(json_data["id"], list) and json_data["id"] else json_data["id"]
                sections["basic"].append(f"ID:      {id_val}")
            if "ra"          in json_data: sections["location"].append(f"RA:      {json_data['ra']} deg")
            if "dec"         in json_data: sections["location"].append(f"DEC:      {json_data['dec']} deg")
            if "ra_dec_error" in json_data: sections["location"].append(f"RA_DEC_ERROR:      {json_data['ra_dec_error']} deg")
            if "trigger_time" in json_data:
                t_std = self._standardize_time_format(json_data["trigger_time"])
                t_diff = self._calculate_time_diff(now_str, json_data["trigger_time"])
                sections["timing"].append(f"TRIGGER_TIME:      {t_std}")
                sections["timing"].append(f"TRIGGER_TIME_DIFF:      {t_diff}")
            if "image_energy_range" in json_data: sections["analysis"].append(f"IMAGE_ENERGY_RANGE:      {json_data['image_energy_range']}")
            if "net_count_rate"     in json_data: sections["analysis"].append(f"NET_COUNT_RATE:      {json_data['net_count_rate']}")
            if "image_snr"          in json_data: sections["analysis"].append(f"IMAGE_SNR:      {json_data['image_snr']}")

        else:
            remaining = {k: v for k, v in json_data.items() if k not in ("schema", "$schema", "id", "instrument", "trigger_time")}
            if remaining:
                sections["basic"].append("OTHER_FIELDS:")
                for k, v in remaining.items():
                    val = json.dumps(v) if isinstance(v, (list, dict)) else str(v)
                    sections["basic"].append(f"{k.upper()}: {val}")

        formatted = ""
        if sections["basic"]:    formatted += "*[BASIC INFO]*\n> " + "\n> ".join(sections["basic"]) + "\n\n"
        if sections["location"]: formatted += "*[LOCATION]*\n> "   + "\n> ".join(sections["location"]) + "\n\n"
        if sections["timing"]:   formatted += "*[TIMING]*\n> "     + "\n> ".join(sections["timing"]) + "\n\n"
        if sections["analysis"]: formatted += "*[ANALYSIS]*\n> "   + "\n> ".join(sections["analysis"])

        json_key_fields = [
            "RA:", "DEC:", "RA_DEC_ERROR:",
            # "IMAGE_ENERGY_RANGE:", "NET_COUNT_RATE:", 
            "IMAGE_SNR:", "ENERGY:", "SIGNALNESS:", "FAR:",
            "ID:", "TRIGGER_NUM:", "EVENT_NUM:",
            "TRIGGER_TIME:", "DISCOVERY_TIME:", "TRIGGER_TIME_DIFF:",
        ]
        formatted = self._apply_bold_keys(formatted, json_key_fields)
        return formatted.strip()

    # ------------------------------------------------------------------
    # Private: notice URL generation
    # ------------------------------------------------------------------

    def _get_notice_url(self, topic: str, text: Union[str, bytes]) -> Optional[str]:
        try:
            if isinstance(text, bytes):
                text = text.decode("utf-8", errors="ignore")

            if "ICECUBE_ASTROTRACK_BRONZE" in topic or "ICECUBE_ASTROTRACK_GOLD" in topic:
                r = re.search(r"RUN_NUM:\s*(\d+)", text)
                e = re.search(r"EVENT_NUM:\s*(\d+)", text)
                if r and e:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_g_b/{r.group(1)}_{e.group(1)}.amon"

            elif "HAWC_BURST_MONITOR" in topic:
                r = re.search(r"RUN_NUM:\s*(\d+)", text)
                e = re.search(r"EVENT_NUM:\s*(\d+)", text)
                if r and e:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_hawc/{r.group(1)}_{e.group(1)}.amon"

            elif "AMON_NU_EM_COINC" in topic:
                r = re.search(r"RUN_NUM:\s*(\d+)", text)
                e = re.search(r"EVENT_NUM:\s*(\d+)", text)
                if r and e:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_nu_em/{r.group(1)}_{e.group(1)}.amon"

            elif "ICECUBE_CASCADE" in topic:
                r = re.search(r"RUN_NUM:\s*(\d+)", text)
                e = re.search(r"EVENT_NUM:\s*(\d+)", text)
                if r and e:
                    return f"https://gcn.gsfc.nasa.gov/notices_amon_icecube_cascade/{r.group(1)}_{e.group(1)}.amon"

            elif "FERMI" in topic.upper():
                for pat in (r"TRIGGER_NUM:\s*(\d+)", r"TRIGGER_NUM.*?(\d+)", r"TRIGGER.*?(\d{9})", r"trigger.*?#?(\d+)"):
                    m = re.search(pat, text, re.IGNORECASE)
                    if m:
                        return f"https://gcn.gsfc.nasa.gov/other/{m.group(1)}.fermi"

            elif "SWIFT" in topic.upper():
                for pat in (r"TRIGGER_NUM:\s*(\d+)", r"TRIGGER_NUM.*?(\d+)", r"trigger.*?#?(\d+)"):
                    m = re.search(pat, text, re.IGNORECASE)
                    if m:
                        return f"https://gcn.gsfc.nasa.gov/other/{m.group(1)}.swift"

            elif "SVOM" in topic.upper():
                return "https://fsc.svom.org/alerts"

        except Exception as exc:
            logger.error(f"Error building notice URL: {exc}")
        return None

    def _build_swift_lc_url(self, notice_data: Dict[str, Any]) -> Optional[str]:
        try:
            trig = str(notice_data.get("Trigger_num", ""))
            fac  = notice_data.get("Facility", "")
            if not trig:
                return None
            if "SwiftBAT" in fac:
                return f"https://gcn.gsfc.nasa.gov/notices_s/sw0{trig}000msb.gif"
            if "SwiftXRT" in fac:
                return f"https://gcn.gsfc.nasa.gov/notices_s/sw0{trig}000msx.gif"
        except Exception as exc:
            logger.warning(f"Error building Swift LC URL: {exc}")
        return None

    # ------------------------------------------------------------------
    # Private: time utilities
    # ------------------------------------------------------------------

    def _standardize_time_format(self, time_str: str) -> str:
        """Return 'yy-mm-dd HH:MM:SS (UTC) / yy-mm-dd HH:MM:SS (KST)'."""
        formats = [
            "%y/%m/%d %H:%M:%S.%f",
            "%y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%a %d %b %y %H:%M:%S",
            "%a %d %b %y %H:%M:%S.%f",
        ]
        dt = None
        for fmt in formats:
            try:
                if fmt == "%Y-%m-%dT%H:%M:%S.%fZ" and "T" in time_str and time_str.endswith("Z"):
                    if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", time_str):
                        dt = datetime.strptime(time_str[:-1] + "000Z", fmt)
                    else:
                        dt = datetime.strptime(time_str, fmt)
                else:
                    dt = datetime.strptime(time_str, fmt)
                break
            except (ValueError, TypeError):
                continue

        if dt is None and "T" in time_str and time_str.endswith("Z"):
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except Exception:
                pass

        if dt is None:
            m = re.search(
                r"(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
                time_str,
            )
            if m:
                _, day, mon, year, hh, mm, ss = m.groups()
                month_n = datetime.strptime(mon, "%b").month
                fmt2 = "%Y-%m-%d %H:%M:%S.%f" if "." in ss else "%Y-%m-%d %H:%M:%S"
                try:
                    dt = datetime.strptime(f"20{year}-{month_n:02d}-{int(day):02d} {hh}:{mm}:{ss}", fmt2)
                except ValueError:
                    pass

        if dt is None:
            return time_str

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        kst = pytz.timezone("Asia/Seoul")
        dt_kst = dt.astimezone(kst)
        return f"{dt.strftime('%y-%m-%d %H:%M:%S')} (UTC) / {dt_kst.strftime('%y-%m-%d %H:%M:%S')} (KST)"

    def _calculate_time_diff(self, notice_time_str: str, trigger_time_str: str) -> str:
        """Return '+HH:MM:SS (Notice after Trigger)' or similar."""
        try:
            notice_dt  = self._parse_dt(notice_time_str)
            trigger_dt = self._parse_dt(trigger_time_str)
            if notice_dt and trigger_dt:
                diff = (notice_dt - trigger_dt).total_seconds()
                h, rem = divmod(abs(diff), 3600)
                m, s   = divmod(rem, 60)
                sign   = "+" if diff >= 0 else "-"
                label  = "Notice after Trigger" if diff >= 0 else "Notice before Trigger"
                return f"{sign}{int(h):02d}:{int(m):02d}:{int(s):02d} ({label})"
        except Exception as exc:
            logger.error(f"Error calculating time diff: {exc}")
        return "Time difference could not be calculated"

    def _parse_dt(self, time_str: str) -> Optional[datetime]:
        formats = [
            "%y/%m/%d %H:%M:%S.%f",
            "%y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%a %d %b %y %H:%M:%S",
            "%a %d %b %y %H:%M:%S.%f",
        ]
        for fmt in formats:
            try:
                if fmt == "%Y-%m-%dT%H:%M:%S.%fZ" and "T" in time_str and time_str.endswith("Z"):
                    if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", time_str):
                        dt = datetime.strptime(time_str[:-1] + "000Z", fmt)
                    else:
                        dt = datetime.strptime(time_str, fmt)
                else:
                    dt = datetime.strptime(time_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError):
                continue

        if "T" in time_str and time_str.endswith("Z"):
            try:
                return datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except Exception:
                pass

        m = re.search(
            r"(\w{3})\s+(\d{1,2})\s+(\w{3})\s+(\d{2})\s+(\d{2}):(\d{2}):(\d{2}(?:\.\d+)?)\s*UT",
            time_str,
        )
        if m:
            _, day, mon, year, hh, mm, ss = m.groups()
            month_n = datetime.strptime(mon, "%b").month
            fmt2 = "%Y-%m-%d %H:%M:%S.%f" if "." in ss else "%Y-%m-%d %H:%M:%S"
            try:
                dt = datetime.strptime(f"20{year}-{month_n:02d}-{int(day):02d} {hh}:{mm}:{ss}", fmt2)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return None

    # ------------------------------------------------------------------
    # Private: GRB name injection
    # ------------------------------------------------------------------

    def _inject_grb_name(self, text: str, name: str) -> str:
        """Insert a GRB_NAME line into the [BASIC INFO] block."""
        pattern = r"(\*\[BASIC INFO\]\*\n(?:>.*\n)*?)"
        m = re.search(pattern, text)
        if m:
            grb_line = f"> *GRB_NAME(Probably):      {name}*\n"
            text = text.replace(m.group(1), m.group(1) + grb_line, 1)
        return text
