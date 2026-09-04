"""
SlackManager
============
Thin wrapper around ``slack_sdk.WebClient`` and the Slack Bolt ``App``
for Socket Mode.

Responsibilities
----------------
* Send messages and thread replies to a channel.
* Upload file attachments (e.g. visibility plots) to a thread.
* Start the Bolt Socket Mode handler in a daemon thread.

All methods return ``True``/``False`` or the raw Slack API response
depending on what callers need.
"""

import logging
import threading
from io import BytesIO
from typing import Any, Dict, List, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackManager:
    """
    Wraps Slack WebClient and Bolt App.

    Parameters
    ----------
    token : str
        Slack bot OAuth token (``xoxb-...``).
    channel : str
        Default channel ID for outgoing messages.
    app_token : str | None
        Socket Mode app token (``xapp-...``).  When ``None``, Socket Mode is
        disabled and interactive features (buttons, modals) will not work.
    """

    def __init__(
        self,
        token: str,
        channel: str,
        app_token: Optional[str] = None,
    ) -> None:
        self.channel    = channel
        self.app_token  = app_token
        self._client    = WebClient(token=token)
        self._app       = None
        self._handler   = None

        if app_token:
            try:
                from slack_bolt import App
                self._app = App(token=token)
                logger.info("Slack Bolt App initialised")
            except Exception as exc:
                logger.error(f"Failed to initialise Slack Bolt App: {exc}")
                self._app = None
        else:
            logger.warning("No SLACK_APP_TOKEN — interactive features disabled")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def client(self) -> WebClient:
        """Direct access to the underlying WebClient when needed."""
        return self._client

    @property
    def app(self):
        """The Bolt App instance (may be ``None`` when Socket Mode is off)."""
        return self._app

    # ------------------------------------------------------------------
    # Messaging
    # ------------------------------------------------------------------

    def send_message(
        self,
        blocks: List[Dict[str, Any]],
        channel: Optional[str] = None,
        text: str = "",
    ) -> Optional[Dict[str, Any]]:
        """
        Post a block-kit message to *channel* (defaults to the configured channel).

        Returns the Slack API response dict, or ``None`` on failure.
        """
        target = channel or self.channel
        try:
            resp = self._client.chat_postMessage(
                channel=target,
                blocks=blocks,
                text=text or "New GCN Alert",
            )
            ts = resp.get("ts")
            logger.info(f"Message sent to {target} (ts={ts})")
            return resp.data
        except SlackApiError as exc:
            logger.error(f"Failed to send message: {exc.response['error']}")
            return None

    def send_thread_message(
        self,
        thread_ts: str,
        blocks: Optional[List[Dict[str, Any]]] = None,
        text: str = "",
        channel: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Reply in a thread.

        Returns the Slack API response dict, or ``None`` on failure.
        """
        target = channel or self.channel
        try:
            resp = self._client.chat_postMessage(
                channel=target,
                thread_ts=thread_ts,
                blocks=blocks or [],
                text=text or "Update",
            )
            logger.info(f"Thread reply sent (thread_ts={thread_ts})")
            return resp.data
        except SlackApiError as exc:
            logger.error(f"Failed to send thread message: {exc.response['error']}")
            return None

    def upload_file(
        self,
        thread_ts: str,
        file: BytesIO,
        filename: str = "visibility.png",
        title: str = "Visibility Plot",
        channel: Optional[str] = None,
    ) -> bool:
        """
        Upload a file to a thread.

        Returns ``True`` on success.
        """
        target = channel or self.channel
        try:
            self._client.files_upload_v2(
                channel=target,
                thread_ts=thread_ts,
                file=file,
                filename=filename,
                title=title,
            )
            logger.info(f"File '{filename}' uploaded to thread {thread_ts}")
            return True
        except SlackApiError as exc:
            logger.error(f"Failed to upload file: {exc.response['error']}")
            return False

    def update_message(
        self,
        ts: str,
        blocks: List[Dict[str, Any]],
        channel: Optional[str] = None,
    ) -> bool:
        """Replace the content of an existing message."""
        target = channel or self.channel
        try:
            self._client.chat_update(channel=target, ts=ts, blocks=blocks)
            logger.info(f"Message {ts} updated")
            return True
        except SlackApiError as exc:
            logger.error(f"Failed to update message {ts}: {exc.response['error']}")
            return False

    # ------------------------------------------------------------------
    # User info
    # ------------------------------------------------------------------

    def get_permalink(self, message_ts: str, channel: Optional[str] = None) -> str:
        """Return the permalink URL for *message_ts*, or an empty string on failure."""
        target = channel or self.channel
        try:
            resp = self._client.chat_getPermalink(channel=target, message_ts=message_ts)
            return resp.get("permalink", "")
        except SlackApiError as exc:
            logger.warning(f"Could not get permalink for {message_ts}: {exc.response['error']}")
            return ""

    def get_user_email(self, user_id: str) -> Optional[str]:
        """Return the Slack user's email address, or ``None``."""
        try:
            info = self._client.users_info(user=user_id)
            return info["user"]["profile"].get("email")
        except SlackApiError as exc:
            logger.error(f"Failed to get email for {user_id}: {exc}")
            return None

    def get_user_display_name(self, user_id: str) -> str:
        """Return the user's display name, falling back to login name."""
        try:
            info = self._client.users_info(user=user_id)
            profile = info["user"]["profile"]
            return profile.get("display_name") or profile.get("real_name") or info["user"]["name"]
        except SlackApiError:
            return "Unknown User"

    def get_usergroup_members(self, group_handle: str) -> List[str]:
        """Return list of user IDs in a user-group identified by its handle."""
        try:
            resp = self._client.usergroups_list()
            for group in resp["usergroups"]:
                if group["handle"] == group_handle:
                    members_resp = self._client.usergroups_users_list(usergroup=group["id"])
                    return members_resp["users"]
            logger.warning(f"User group '{group_handle}' not found")
        except SlackApiError as exc:
            logger.error(f"Failed to get user group members: {exc}")
        return []

    # ------------------------------------------------------------------
    # Socket Mode
    # ------------------------------------------------------------------

    def start_socket_mode(self) -> bool:
        """
        Start the Bolt Socket Mode handler in a background daemon thread.

        Returns ``True`` if Socket Mode started successfully, ``False``
        when the app token is missing or initialisation failed.
        """
        if self._app is None or not self.app_token:
            logger.warning("Cannot start Socket Mode: Bolt App not initialised")
            return False
        try:
            from slack_bolt.adapter.socket_mode import SocketModeHandler

            self._handler = SocketModeHandler(self._app, self.app_token)
            t = threading.Thread(
                target=self._handler.start,
                daemon=True,
                name="slack-socket-mode",
            )
            t.start()
            logger.info("Slack Socket Mode started in background thread")
            return True
        except Exception as exc:
            logger.error(f"Failed to start Socket Mode: {exc}")
            return False

    def stop_socket_mode(self) -> None:
        """Gracefully stop the Socket Mode handler if running."""
        if self._handler:
            try:
                self._handler.close()
                logger.info("Slack Socket Mode stopped")
            except Exception as exc:
                logger.warning(f"Error stopping Socket Mode: {exc}")
