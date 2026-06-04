"""UPnP GetInfoEx poller for Linkplay/WiiM devices.

Polls the AVTransport GetInfoEx SOAP action (Linkplay-specific extension)
which returns TrackMetaData containing albumArtURI — not available via the
HTTP API.

This is intentionally a standalone module with no pywiim imports so it works
regardless of which pywiim version is cached, and avoids any monkey-patching
of the library.

Usage (from coordinator):
    poller = UpnpGetInfoExPoller(host, session)
    result = await poller.poll()
    if result and result.get("image_url"):
        ...
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

# Port used by Linkplay/WiiM for UPnP AVTransport control
_UPNP_PORT = 59152
_UPNP_PATH = "/upnp/control/rendertransport1"
_SOAP_ACTION = "urn:schemas-upnp-org:service:AVTransport:1#GetInfoEx"

_SOAP_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<s:Envelope s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"'
    ' xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">'
    "<s:Body>"
    '<u:GetInfoEx xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">'
    "<InstanceID>0</InstanceID>"
    "</u:GetInfoEx>"
    "</s:Body>"
    "</s:Envelope>"
)

# XML namespaces used in DIDL-Lite metadata
_NS = {
    "dc": "http://purl.org/dc/elements/1.1/",
    "upnp": "urn:schemas-upnp-org:metadata-1-0/upnp/",
    "didl": "urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/",
    "r": "urn:schemas-rinconnetworks-com:metadata-1-0/",
}

# Regex to fix bare & in attribute values that aren't already &amp;
# This mirrors the PowerShell: -replace '&(?!(amp|lt|gt|quot|apos);)', '&amp;'
_AMP_FIX_RE = re.compile(r"&(?!(amp|lt|gt|quot|apos);)")


def _fix_ampersands(xml_text: str) -> str:
    """Replace bare & with &amp; while leaving existing XML entities intact."""
    return _AMP_FIX_RE.sub("&amp;", xml_text)


def _parse_didl(didl_raw: str) -> dict[str, str | None]:
    """Parse DIDL-Lite XML and extract track metadata fields.

    Returns dict with keys: title, artist, album, image_url (all may be None).
    """
    result: dict[str, str | None] = {
        "title": None,
        "artist": None,
        "album": None,
        "image_url": None,
    }

    if not didl_raw or didl_raw.strip().lower() in ("not_implemented", ""):
        return result

    try:
        clean = _fix_ampersands(didl_raw)
        root = ET.fromstring(clean)  # noqa: S314 — trusted local device response
    except ET.ParseError as err:
        _LOGGER.debug("DIDL-Lite parse error: %s", err)
        return result

    # DIDL-Lite item is the first <item> inside <DIDL-Lite>
    item = root.find(".//{urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/}item")
    if item is None:
        # Some devices omit the namespace wrapper, try without
        item = root.find(".//item")
    if item is None:
        return result

    def _text(tag_ns: str, tag: str) -> str | None:
        el = item.find(f"{{{tag_ns}}}{tag}")
        return el.text if el is not None and el.text else None

    result["title"] = _text("http://purl.org/dc/elements/1.1/", "title")
    result["artist"] = _text("urn:schemas-upnp-org:metadata-1-0/upnp/", "artist")
    result["album"] = _text("urn:schemas-upnp-org:metadata-1-0/upnp/", "album")
    result["image_url"] = _text("urn:schemas-upnp-org:metadata-1-0/upnp/", "albumArtURI")

    return result


class UpnpGetInfoExPoller:
    """Polls UPnP GetInfoEx and returns parsed track metadata including cover art URL.

    Args:
        host: Device IP or hostname.
        session: Shared aiohttp.ClientSession (from HA, already managed).
        timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        host: str,
        session: aiohttp.ClientSession,
        timeout: int = 5,
    ) -> None:
        self._url = f"http://{host}:{_UPNP_PORT}{_UPNP_PATH}"
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=timeout)

    async def poll(self) -> dict[str, Any] | None:
        """Perform one GetInfoEx SOAP request.

        Returns a dict with keys::

            play_state  – normalised string ("playing", "paused", "stopped") or None
            position    – seconds (float) or None
            duration    – seconds (float) or None
            title       – str or None
            artist      – str or None
            album       – str or None
            image_url   – str or None  ← the reason this module exists

        Returns None on any network or parse error (errors are DEBUG-logged,
        not WARNING, because this is a best-effort supplemental poll).
        """
        try:
            headers = {
                "SOAPACTION": f'"{_SOAP_ACTION}"',
                "Content-Type": 'text/xml;charset="utf-8"',
            }
            async with self._session.post(
                self._url,
                data=_SOAP_BODY,
                headers=headers,
                timeout=self._timeout,
            ) as resp:
                if resp.status != 200:
                    _LOGGER.debug(
                        "GetInfoEx HTTP %d from %s", resp.status, self._url
                    )
                    return None
                text = await resp.text()
        except asyncio.TimeoutError:
            _LOGGER.debug("GetInfoEx timed out for %s", self._url)
            return None
        except aiohttp.ClientError as err:
            _LOGGER.debug("GetInfoEx client error for %s: %s", self._url, err)
            return None
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("GetInfoEx unexpected error for %s: %s", self._url, err)
            return None

        return self._parse_response(text)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hms_to_seconds(hms: str | None) -> float | None:
        """Convert HH:MM:SS to float seconds, return None on failure."""
        if not hms or hms in ("NOT_IMPLEMENTED", "00:00:00"):
            return None
        try:
            parts = hms.strip().split(":")
            if len(parts) == 3:
                h, m, s = parts
                return int(h) * 3600 + int(m) * 60 + float(s)
            if len(parts) == 2:
                m, s = parts
                return int(m) * 60 + float(s)
        except (ValueError, TypeError):
            pass
        return None

    @staticmethod
    def _normalise_play_state(raw: str | None) -> str | None:
        """Map UPnP transport states to pywiim-compatible strings."""
        if not raw:
            return None
        mapping = {
            "PLAYING": "playing",
            "PAUSED_PLAYBACK": "paused",
            "STOPPED": "stopped",
            "NO_MEDIA_PRESENT": "idle",
            "TRANSITIONING": "transitioning",
        }
        return mapping.get(raw.upper(), raw.lower())

    def _parse_response(self, xml_text: str) -> dict[str, Any] | None:
        """Parse GetInfoEx SOAP response into a flat dict."""
        try:
            root = ET.fromstring(xml_text)  # noqa: S314
        except ET.ParseError as err:
            _LOGGER.debug("GetInfoEx response parse error: %s", err)
            return None

        # Locate GetInfoExResponse anywhere in the envelope
        resp_el = root.find(
            ".//{urn:schemas-upnp-org:service:AVTransport:1}GetInfoExResponse"
        )
        if resp_el is None:
            _LOGGER.debug("GetInfoExResponse element not found in SOAP response")
            return None

        def _get(tag: str) -> str | None:
            el = resp_el.find(tag)
            return el.text if el is not None else None

        play_state = self._normalise_play_state(_get("CurrentTransportState"))
        position = self._hms_to_seconds(_get("RelTime"))
        duration = self._hms_to_seconds(_get("TrackDuration"))

        # Volume: raw 0-100 integer → normalise to 0.0-1.0 float (HA convention)
        volume_level: float | None = None
        raw_vol = _get("CurrentVolume")
        if raw_vol is not None:
            try:
                volume_level = max(0.0, min(1.0, int(raw_vol) / 100.0))
            except (ValueError, TypeError):
                pass

        # Mute: CurrentChannel — 0 = not muted, 1 = muted
        is_muted: bool | None = None
        raw_ch = _get("CurrentChannel")
        if raw_ch is not None:
            try:
                is_muted = int(raw_ch) != 0
            except (ValueError, TypeError):
                pass

        # LoopMode: raw integer — kept as-is; coordinator maps to shuffle/repeat
        loop_mode: int | None = None
        raw_loop = _get("LoopMode")
        if raw_loop is not None:
            try:
                loop_mode = int(raw_loop)
            except (ValueError, TypeError):
                pass

        meta_raw = _get("TrackMetaData")
        metadata = _parse_didl(meta_raw) if meta_raw else {}

        result: dict[str, Any] = {
            "play_state": play_state,
            "position": position,
            "duration": duration,
            "volume_level": volume_level,
            "is_muted": is_muted,
            "loop_mode": loop_mode,
            **metadata,
        }

        _LOGGER.debug(
            "GetInfoEx result: state=%s pos=%s/%s vol=%s muted=%s loop=%s title=%r art=%s",
            play_state,
            position,
            duration,
            volume_level,
            is_muted,
            loop_mode,
            metadata.get("title"),
            metadata.get("image_url"),
        )
        return result


# ---------------------------------------------------------------------------
# asyncio import — kept at bottom to avoid circular import issues in HA
# ---------------------------------------------------------------------------
import asyncio  # noqa: E402
