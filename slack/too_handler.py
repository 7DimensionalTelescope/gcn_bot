"""
SlackToOHandler
===============
Manages all Target-of-Opportunity (ToO) interactive features in Slack.

Responsibilities
----------------
* Check whether a Slack user is authorised to submit ToO requests
  (membership of a configured user-group).
* Add a "Submit ToO Request" button block to outgoing GRB alert messages.
* Open a pre-populated Slack modal form when the button is clicked.
* Process the form submission: extract values, send a ToO email, and post
  a confirmation (or error) message in the thread.
* Register all Bolt action / view handlers on the Bolt ``App`` instance.

Dependencies
------------
``SlackToOHandler`` takes ``SlackManager`` and ``GCNToOEmailer`` as
constructor arguments so it can be tested with mocks independently.
"""

import json
import uuid
import logging
from typing import Any, Callable, Dict, List, Optional

from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackToOHandler:
    """
    Handle Slack interactive ToO workflow.

    Parameters
    ----------
    slack : SlackManager
        The live Slack manager used for API calls.
    emailer : GCNToOEmailer
        The emailer used to send ToO request emails.
    user_group : str
        Handle of the Slack user-group whose members are authorised
        (e.g. ``"too-operators"`` or ``"7dt"``).
    too_config : dict | None
        Default observation parameters to pre-populate the modal (7DT).
    sent_at_provider : callable | None
        ``provider(facility, trigger_num, telescope) -> str`` returning the UTC
        timestamp of a ToO already sent for this event+telescope (``""`` if
        none). When set, the modal opens with a warning so an operator does not
        unknowingly send a second request (e.g. after the automatic one).
    defaults_provider : callable | None
        ``provider(notice_data) -> dict | None`` returning the observation
        config the *automatic* ToO would use for this event. The RASA36 form is
        pre-filled from it (overlaid on the emailer's own defaults) so the
        manual and automatic requests start from identical values.
    """

    def __init__(
        self,
        slack,
        emailer,
        user_group: str,
        too_config: Optional[Dict[str, Any]] = None,
        telescope: str = "7DT",
        on_sent: Optional[Callable[[str, str, str], None]] = None,
        defaults_provider: Optional[Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]] = None,
        sent_at_provider: Optional[Callable[[str, str, str], str]] = None,
    ) -> None:
        self._slack      = slack
        self._emailer    = emailer
        self.user_group  = user_group
        self.too_config  = too_config or {}
        self.telescope   = telescope
        # Called (facility, trigger_num, telescope) after a successful manual
        # send, so the ASCII ToO-sent marker is set and a later auto-ToO for the
        # same event is suppressed. Kept as a callback so this handler stays
        # decoupled from the notice handler.
        self._on_sent    = on_sent
        self._defaults_provider = defaults_provider
        self._sent_at_provider = sent_at_provider
        # Bolt routing keys — unique per telescope so multiple handlers coexist
        self.action_id   = f"submit_too_request_{telescope.lower()}"
        self.callback_id = f"too_request_modal_{telescope.lower()}"

    # ------------------------------------------------------------------
    # Authorisation
    # ------------------------------------------------------------------

    def is_user_authorized(self, user_id: str) -> bool:
        """Return ``True`` if *user_id* is in the configured user-group."""
        members = self._slack.get_usergroup_members(self.user_group)
        result  = user_id in members
        logger.info(f"Authorization check for {user_id}: {result}")
        return result

    # ------------------------------------------------------------------
    # Button
    # ------------------------------------------------------------------

    def add_too_button(
        self,
        blocks: List[Dict[str, Any]],
        notice_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Append a "Submit ToO Request" button to *blocks*.

        The button value carries a compact JSON payload (name, RA, DEC,
        facility, trigger) so the modal can be pre-populated.

        Returns the enhanced block list (original is not modified).
        """
        try:
            payload = {
                "Name":        str(notice_data.get("Name", "")),
                "RA":          str(notice_data.get("RA", "")),
                "DEC":         str(notice_data.get("DEC", "")),
                "Error":       str(notice_data.get("Error", "")),
                "Facility":    str(notice_data.get("Facility", "")),
                "Trigger_num": str(notice_data.get("Trigger_num", "")),
            }
            payload = {k: v for k, v in payload.items() if v and v not in ("nan", "N/A", "None")}
            json_str = json.dumps(payload, ensure_ascii=True)

            # Slack button value limit is 2000 characters
            if len(json_str) > 2000:
                json_str = json.dumps(
                    {"Name": payload.get("Name", ""), "RA": payload.get("RA", ""), "DEC": payload.get("DEC", "")},
                    ensure_ascii=True,
                )

            button_block = {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": f"📧 Submit {self.telescope} ToO Request",
                            "emoji": True,
                        },
                        "style": "primary",
                        "action_id": self.action_id,
                        "value": json_str,
                    }
                ],
            }

            enhanced = blocks.copy()
            # Only add a divider before the first ToO button block (avoid stacked
            # dividers when this method is called for multiple telescopes).
            if not enhanced or enhanced[-1].get("type") != "actions":
                enhanced.append({"type": "divider"})
            enhanced.append(button_block)
            return enhanced

        except Exception as exc:
            logger.error(f"Failed to add ToO button: {exc}")
            return blocks

    # ------------------------------------------------------------------
    # Modal
    # ------------------------------------------------------------------

    def create_too_modal(
        self,
        trigger_id: str,
        user_email: str,
        notice_data: Dict[str, Any],
    ) -> bool:
        """Open the ToO request modal form for the user."""
        target = str(notice_data.get("Name", ""))
        ra     = str(notice_data.get("RA", ""))
        dec    = str(notice_data.get("DEC", ""))

        # Read defaults from too_config
        cfg = self.too_config
        if self.telescope == "RASA36":
            # Follow the automatic RASA36 ToO: emailer defaults overlaid by the
            # auto config for this event (not the generic TOO_CONFIG, which is
            # 7DT-oriented: 100 s x 3, priority 10, gain 2750).
            auto_cfg = None
            if self._defaults_provider is not None:
                try:
                    auto_cfg = self._defaults_provider(notice_data)
                except Exception as exc:
                    logger.warning(f"RASA36 form defaults provider failed: {exc}")
            cfg = self._emailer.default_form_config(auto_cfg)
            init_exposure = str(cfg.get("singleExposure"))
            init_count    = str(cfg.get("imageCount"))
            init_priority = str(cfg.get("priority"))
            init_gain     = str(cfg.get("gain"))
            init_binning  = str(cfg.get("binning"))
            init_rapid    = "True" if any(
                str(cfg.get(k, "")).strip().lower() in ("true", "yes", "1")
                for k in ("rapidToO", "abortObservation")
            ) else "False"
        else:
            init_exposure = str(cfg.get("exptime", cfg.get("singleExposure", 100)))
            init_count    = str(cfg.get("count",   cfg.get("imageCount", 3)))
            init_priority = str(cfg.get("priority", "50"))
            init_gain     = str(cfg.get("gain", "2750"))
            init_binning  = str(cfg.get("binning", "1"))
            init_obsmode  = cfg.get("obsmode", "Spec")
            init_specmode = cfg.get("specmode", "specall")

        if self.telescope == "RASA36":
            return self._open_modal_rasa36(
                trigger_id, user_email, notice_data,
                target, ra, dec,
                init_exposure, init_count, init_priority, init_gain, init_binning,
                init_rapid,
            )

        modal = {
            "type": "modal",
            "callback_id": self.callback_id,
            "title": {"type": "plain_text", "text": f"{self.telescope} ToO Request Form"},
            "submit": {"type": "plain_text", "text": "Submit Request"},
            "close":  {"type": "plain_text", "text": "Cancel"},
            "notify_on_close": True,   # → view_closed: release the SMTP pre-warm
            "blocks": [
                # Header
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Target of Opportunity Request*\n*Target:* {target or 'Not specified'}",
                    },
                },
                {"type": "divider"},
                # Requester email
                self._input_block("requester_block", "requester_input", "Requester Email *", user_email, "your.email@example.com"),
                # Target name
                self._input_block("target_block", "target_input", "Target Name *", target, "GRB240101A"),
                # RA
                self._input_block("ra_block", "ra_input", "Right Ascension (RA) *", ra, "150.1234 (degrees)"),
                # DEC
                self._input_block("dec_block", "dec_input", "Declination (DEC) *", dec, "-25.5678 (degrees)"),
                {"type": "divider"},
                # Exposure time
                self._input_block("exposure_block", "exposure_input", "Single Exposure Time (seconds) *", init_exposure, "100"),
                # Image count
                self._input_block("count_block", "count_input", "Number of Images *", init_count, "3"),
                # Obs mode
                {
                    "type": "input",
                    "block_id": "obsmode_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "obsmode_input",
                        "initial_option": {"text": {"type": "plain_text", "text": init_obsmode}, "value": init_obsmode},
                        "options": [
                            {"text": {"type": "plain_text", "text": "Deep"}, "value": "Deep"},
                            {"text": {"type": "plain_text", "text": "Spec"}, "value": "Spec"},
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Observation Mode *"},
                },
                # Spec mode
                self._input_block("specmode_block", "specmode_input", "Spec Mode *", init_specmode, "specall"),
                # Filters
                {
                    "type": "input",
                    "block_id": "filters_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "filters_input",
                        "initial_options": [
                            {"text": {"type": "plain_text", "text": f}, "value": f} for f in ["g", "r", "i"]
                        ],
                        "options": [
                            {"text": {"type": "plain_text", "text": f}, "value": f} for f in ["u", "g", "r", "i", "z"]
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Filters *"},
                },
                {"type": "divider"},
                # Priority
                self._input_block("priority_block", "priority_input", "Priority *", init_priority, "50"),
                # Binning
                {
                    "type": "input",
                    "block_id": "binning_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "binning_input",
                        "initial_option": {"text": {"type": "plain_text", "text": init_binning}, "value": init_binning},
                        "options": [
                            {"text": {"type": "plain_text", "text": str(n)}, "value": str(n)} for n in range(1, 6)
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Binning *"},
                },
                # Gain
                self._input_block("gain_block", "gain_input", "Gain *", init_gain, "2750"),
                # Abort current observation
                {
                    "type": "input",
                    "block_id": "abort_block",
                    "element": {
                        "type": "radio_buttons",
                        "action_id": "abort_input",
                        "initial_option": {"text": {"type": "plain_text", "text": "False"}, "value": "False"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "True"},  "value": "True"},
                            {"text": {"type": "plain_text", "text": "False"}, "value": "False"},
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Abort Current Observation *"},
                },
                {"type": "divider"},
                # Comments
                {
                    "type": "input",
                    "block_id": "comments_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "comments_input",
                        "multiline": True,
                        "initial_value": f"Submitted via Slack for {target}" if target else "Submitted via Slack",
                        "placeholder": {"type": "plain_text", "text": "Additional comments or special instructions..."},
                    },
                    "label": {"type": "plain_text", "text": "Comments"},
                    "optional": True,
                },
            ],
            "private_metadata": json.dumps(notice_data, default=str),
        }

        try:
            self._prepend_duplicate_warning(modal, notice_data)
            self._slack.client.views_open(trigger_id=trigger_id, view=modal)
            logger.info("ToO modal opened successfully")
            return True
        except SlackApiError as exc:
            logger.error(f"Failed to open ToO modal: {exc}")
            return False

    # ------------------------------------------------------------------
    # RASA36 modal
    # ------------------------------------------------------------------

    def _open_modal_rasa36(
        self,
        trigger_id: str,
        user_email: str,
        notice_data: Dict[str, Any],
        target: str,
        ra: str,
        dec: str,
        init_exposure: str,
        init_count: str,
        init_priority: str,
        init_gain: str,
        init_binning: str,
        init_rapid: str = "False",
    ) -> bool:
        """Open the RASA36 ToO modal — no obsmode/spec/filter selectors."""
        modal = {
            "type": "modal",
            "callback_id": self.callback_id,
            "title": {"type": "plain_text", "text": "RASA36 ToO Request Form"},
            "submit": {"type": "plain_text", "text": "Submit Request"},
            "close":  {"type": "plain_text", "text": "Cancel"},
            "notify_on_close": True,   # → view_closed: release the SMTP pre-warm
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*RASA36 Target of Opportunity Request*\n"
                            f"*Target:* {target or 'Not specified'}\n"
                            "_Fixed: Obsmode=Single, Filter=r_"
                        ),
                    },
                },
                {"type": "divider"},
                self._input_block("requester_block", "requester_input", "Requester Email *", user_email, "your.email@example.com"),
                self._input_block("target_block",    "target_input",    "Target Name *",     target,     "GRB240101A"),
                self._input_block("ra_block",        "ra_input",        "Right Ascension (RA) *", ra,    "150.1234 (degrees)"),
                self._input_block("dec_block",       "dec_input",       "Declination (DEC) *",    dec,   "-25.5678 (degrees)"),
                {"type": "divider"},
                self._input_block("exposure_block", "exposure_input", "Single Exposure Time (seconds) *", init_exposure, "60"),
                self._input_block("count_block",    "count_input",    "Number of Images *",               init_count,    "55"),
                {"type": "divider"},
                self._input_block("priority_block", "priority_input", "Priority *", init_priority, "1"),
                {
                    "type": "input",
                    "block_id": "binning_block",
                    "element": {
                        "type": "static_select",
                        "action_id": "binning_input",
                        "initial_option": {"text": {"type": "plain_text", "text": init_binning}, "value": init_binning},
                        "options": [
                            {"text": {"type": "plain_text", "text": str(n)}, "value": str(n)} for n in range(1, 6)
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Binning *"},
                },
                self._input_block("gain_block", "gain_input", "Gain *", init_gain, "25"),
                {
                    "type": "input",
                    "block_id": "abort_block",
                    "element": {
                        "type": "radio_buttons",
                        "action_id": "abort_input",
                        "initial_option": {"text": {"type": "plain_text", "text": init_rapid}, "value": init_rapid},
                        "options": [
                            {"text": {"type": "plain_text", "text": "True"},  "value": "True"},
                            {"text": {"type": "plain_text", "text": "False"}, "value": "False"},
                        ],
                    },
                    "label": {"type": "plain_text", "text": "Rapid ToO (Is_rapid_ToO) *"},
                },
                {"type": "divider"},
                {
                    "type": "input",
                    "block_id": "comments_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "comments_input",
                        "multiline": True,
                        "initial_value": f"Submitted via Slack for {target}" if target else "Submitted via Slack",
                        "placeholder": {"type": "plain_text", "text": "Additional comments or special instructions..."},
                    },
                    "label": {"type": "plain_text", "text": "Comments"},
                    "optional": True,
                },
            ],
            "private_metadata": json.dumps(notice_data, default=str),
        }

        try:
            self._prepend_duplicate_warning(modal, notice_data)
            self._slack.client.views_open(trigger_id=trigger_id, view=modal)
            logger.info("RASA36 ToO modal opened successfully")
            return True
        except SlackApiError as exc:
            logger.error(f"Failed to open RASA36 ToO modal: {exc}")
            return False

    # ------------------------------------------------------------------
    # Form data extraction
    # ------------------------------------------------------------------

    def extract_form_data(self, form_values: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and normalise submitted modal values into a flat dict."""
        def text_val(block, action):
            return form_values.get(block, {}).get(action, {}).get("value", "")

        def select_val(block, action):
            opt = form_values.get(block, {}).get(action, {}).get("selected_option", {})
            return opt.get("value", "") if opt else ""

        def radio_val(block, action):
            opt = form_values.get(block, {}).get(action, {}).get("selected_option", {})
            return opt.get("value", "") if opt else ""

        if self.telescope == "RASA36":
            return {
                "requester":        text_val("requester_block", "requester_input"),
                "target":           text_val("target_block",    "target_input"),
                "ra":               text_val("ra_block",         "ra_input"),
                "dec":              text_val("dec_block",        "dec_input"),
                "exposure":         text_val("exposure_block",   "exposure_input"),
                "imageCount":       text_val("count_block",      "count_input"),
                "obsmode":          "Single",
                "priority":         text_val("priority_block",   "priority_input"),
                "binning":          select_val("binning_block",  "binning_input"),
                "gain":             text_val("gain_block",       "gain_input"),
                "abortObservation": radio_val("abort_block",     "abort_input"),
                "comments":         text_val("comments_block",   "comments_input"),
                "selectedFilters":  ["r"],
            }

        filters = [
            opt["value"]
            for opt in form_values.get("filters_block", {})
                                  .get("filters_input", {})
                                  .get("selected_options", [])
        ]

        return {
            "requester":        text_val("requester_block", "requester_input"),
            "target":           text_val("target_block",    "target_input"),
            "ra":               text_val("ra_block",         "ra_input"),
            "dec":              text_val("dec_block",        "dec_input"),
            "exposure":         text_val("exposure_block",   "exposure_input"),
            "imageCount":       text_val("count_block",      "count_input"),
            "obsmode":          select_val("obsmode_block",  "obsmode_input"),
            "specmode":         text_val("specmode_block",   "specmode_input"),
            "priority":         text_val("priority_block",   "priority_input"),
            "binning":          select_val("binning_block",  "binning_input"),
            "gain":             text_val("gain_block",       "gain_input"),
            "abortObservation": radio_val("abort_block",     "abort_input"),
            "comments":         text_val("comments_block",   "comments_input"),
            "selectedFilters":  filters,
        }

    # ------------------------------------------------------------------
    # Bolt handler registration
    # ------------------------------------------------------------------

    def register_handlers(self, app) -> None:
        """Register all Bolt action/view handlers on *app*."""

        @app.action(self.action_id)
        def handle_button_click(ack, body, client):
            ack()
            self._on_button_click(body, client)

        @app.view(self.callback_id)
        def handle_modal_submit(ack, body, client, view):
            ack()
            self._on_modal_submit(body, client, view)

        # Sent because the modal sets notify_on_close: the form was cancelled,
        # so the SMTP connection pre-warmed when it opened can be closed.
        @app.view_closed(self.callback_id)
        def handle_modal_closed(ack, body, view):
            ack()
            self._emailer.release_prewarm(self._lease_from(view))

        logger.info(f"Slack ToO handlers registered ({self.telescope})")

    def _prepend_duplicate_warning(
        self, modal: Dict[str, Any], notice_data: Dict[str, Any]
    ) -> None:
        """Put a warning at the top of *modal* if a ToO was already sent.

        Not blocking: a second request can be legitimate (changed parameters),
        so the operator is only told, and the form still submits.
        """
        if self._sent_at_provider is None:
            return
        facility = str(notice_data.get("Facility", "")).strip()
        trigger  = str(notice_data.get("Trigger_num", "")).strip()
        if not (facility and trigger):
            return
        try:
            sent_at = self._sent_at_provider(facility, trigger, self.telescope)
        except Exception as exc:
            logger.warning(f"ToO already-sent lookup failed: {exc}")
            return
        if not sent_at:
            return
        modal["blocks"].insert(0, {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":warning: *A {self.telescope} ToO request was already sent for "
                    f"this event* ({sent_at.replace('T', ' ')}).\n"
                    "Submitting again will send a *second* request."
                ),
            },
        })

    # ------------------------------------------------------------------
    # Private: button click handler
    # ------------------------------------------------------------------

    def _on_button_click(self, body: Dict[str, Any], client) -> None:
        user_id    = body["user"]["id"]
        trigger_id = body["trigger_id"]
        channel_id = body.get("channel", {}).get("id", "")
        message_ts = body.get("message", {}).get("ts")

        # Authorization
        if not self.is_user_authorized(user_id):
            try:
                client.chat_postEphemeral(
                    channel=channel_id,
                    user=user_id,
                    text=(
                        f"❌ *Access Denied*\n\n"
                        f"You must be a member of @{self.user_group} to submit ToO requests.\n"
                        "Please contact the system administrator to request access."
                    ),
                    thread_ts=message_ts,
                )
            except SlackApiError as exc:
                logger.error(f"Failed to send ephemeral access-denied: {exc}")
            logger.warning(f"Unauthorized ToO attempt by {user_id}")
            return

        # Parse notice data from button value
        try:
            action  = body["actions"][0]
            payload = json.loads(action.get("value", "{}"))
        except (KeyError, json.JSONDecodeError) as exc:
            logger.error(f"Failed to parse button payload: {exc}")
            payload = {}

        user_email = self._slack.get_user_email(user_id) or ""
        payload["_channel_id"] = channel_id
        payload["_message_ts"] = message_ts
        # Pre-warm SMTP while the person fills in the form; the lease rides in
        # the modal's private_metadata and is given back on submit or cancel.
        token = f"manual:{self.telescope}:{uuid.uuid4().hex[:8]}"
        self._emailer.prewarm(token, self._emailer.PREWARM_HOLD_MANUAL_SEC)
        payload["_smtp_lease"] = token
        if not self.create_too_modal(trigger_id, user_email, payload):
            self._emailer.release_prewarm(token)

    @staticmethod
    def _lease_from(view: Dict[str, Any]) -> Optional[str]:
        try:
            return json.loads(view.get("private_metadata") or "{}").get("_smtp_lease")
        except (json.JSONDecodeError, AttributeError):
            return None

    # ------------------------------------------------------------------
    # Private: modal submission handler
    # ------------------------------------------------------------------

    def _on_modal_submit(self, body: Dict[str, Any], client, view: Dict[str, Any]) -> None:
        try:
            self._submit(body, client, view)
        finally:
            # Normally the send already gave the lease back; this covers the
            # early-return and exception paths (releasing twice is harmless).
            self._emailer.release_prewarm(self._lease_from(view))

    def _submit(self, body: Dict[str, Any], client, view: Dict[str, Any]) -> None:
        user_id    = body["user"]["id"]
        channel_id = body.get("container", {}).get("channel_id") or self._slack.channel

        try:
            form_data = self.extract_form_data(view["state"]["values"])
        except Exception as exc:
            logger.error(f"Failed to extract form data: {exc}")
            client.chat_postEphemeral(
                channel=channel_id,
                user=user_id,
                text="❌ Failed to process your form submission. Please try again.",
            )
            return

        # Reconstruct notice_data from private_metadata
        try:
            notice_data = json.loads(view.get("private_metadata", "{}"))
        except json.JSONDecodeError:
            notice_data = {}

        thread_ts  = notice_data.pop("_message_ts", None)
        thread_channel = notice_data.pop("_channel_id", None) or channel_id
        lease      = notice_data.pop("_smtp_lease", None)

        # Build too_config from form
        if self.telescope == "RASA36":
            too_config = {
                "singleExposure":    int(form_data.get("exposure", 60)),
                "imageCount":        int(form_data.get("imageCount", 5)),
                "obsmode":           "Single",
                "selectedFilters":   ["r"],
                # RASA36 has a dedicated Rapid ToO flag (the form's radio); the
                # abort field is a 7DT concept.
                "rapidToO":          form_data.get("abortObservation", "False"),
                "priority":          form_data.get("priority", "50"),
                "gain":              form_data.get("gain", "25"),
                "binning":           form_data.get("binning", "1"),
                "additional_comments": form_data.get("comments", ""),
            }
        else:
            too_config = {
                "singleExposure":    int(form_data.get("exposure", 100)),
                "imageCount":        int(form_data.get("imageCount", 3)),
                "obsmode":           form_data.get("obsmode", "Spec"),
                "specmode":          form_data.get("specmode", "specall"),
                "selectedFilters":   form_data.get("selectedFilters", []),
                "abortObservation":  form_data.get("abortObservation", "False"),
                "priority":          form_data.get("priority", "50"),
                "gain":              form_data.get("gain", "2750"),
                "binning":           form_data.get("binning", "1"),
                "additional_comments": form_data.get("comments", ""),
            }

        # Merge with notice_data for the emailer
        merged = {**notice_data, "Name": form_data.get("target", notice_data.get("Name", ""))}

        username = self._slack.get_user_display_name(user_id)
        success  = self._emailer.send_too_email(merged, too_config=too_config, release_token=lease)

        post_channel = thread_channel or channel_id
        if success:
            msg_kwargs: dict = {
                "channel": post_channel,
                "text": (
                    f"✅ *{self.telescope} ToO Request Submitted*\n"
                    f"*Target:* {merged.get('Name', 'Unknown')}\n"
                    f"*Submitted by:* {username}\n"
                    f"*RA/DEC:* {form_data.get('ra')}, {form_data.get('dec')}"
                ),
            }
            if thread_ts:
                msg_kwargs["thread_ts"] = thread_ts
            client.chat_postMessage(**msg_kwargs)
            logger.info(f"ToO form submitted by {user_id} for {merged.get('Name')}")

            # Record the send so a later auto-ToO for this event is suppressed.
            if self._on_sent is not None:
                facility = str(merged.get("Facility", "")).strip()
                trigger  = str(merged.get("Trigger_num", "")).strip()
                if facility and trigger:
                    try:
                        self._on_sent(facility, trigger, self.telescope)
                    except Exception as exc:
                        logger.error(f"on_sent callback failed: {exc}")
        else:
            client.chat_postEphemeral(
                channel=post_channel,
                user=user_id,
                text="❌ Failed to send the ToO request email. Please contact an administrator.",
            )

    # ------------------------------------------------------------------
    # Private: block builder helper
    # ------------------------------------------------------------------

    @staticmethod
    def _input_block(
        block_id: str,
        action_id: str,
        label: str,
        initial_value: str,
        placeholder: str,
    ) -> Dict[str, Any]:
        return {
            "type": "input",
            "block_id": block_id,
            "element": {
                "type": "plain_text_input",
                "action_id": action_id,
                "initial_value": initial_value,
                "placeholder": {"type": "plain_text", "text": placeholder},
            },
            "label": {"type": "plain_text", "text": label},
        }
