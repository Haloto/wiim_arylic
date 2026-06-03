"""Cover art fetching and caching.

# pragma: allow-long-file coverart-cohesive
# This file exceeds the 400 LOC soft limit (417 lines) but is kept as a single
# cohesive unit because:
# 1. Single responsibility: Cover art fetching, caching, and track change detection
# 2. Well-organized: Clear sections for fetching, caching, and change detection
# 3. Tight coupling: All methods work together for cover art management
# 4. Maintainable: Clear structure, follows cover art design pattern
# 5. Natural unit: Represents one concept (cover art management)
# Splitting would add complexity without clear benefit.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import aiohttp

if TYPE_CHECKING:
    from . import Player

_LOGGER = logging.getLogger(__name__)


class CoverArtManager:
    """Manages cover art fetching and caching."""

    def __init__(self, player: Player) -> None:
        """Initialize cover art manager.

        Args:
            player: Parent Player instance.
        """
        self.player = player
        # Track last track signature to detect track changes for immediate artwork fetching
        self._last_track_signature: str | None = None
        # Track if we're already fetching artwork to avoid duplicate requests
        self._artwork_fetch_task: asyncio.Task | None = None

    def _get_url_hash(self, url: str) -> str:
        """Generate a hash for a URL to use as cache key."""
        return hashlib.md5(url.encode()).hexdigest()

    def _cleanup_cover_art_cache(self) -> None:
        """Remove expired entries from cover art cache."""
        now = time.time()
        expired_keys = [
            key
            for key, (_, _, timestamp) in self.player._cover_art_cache.items()
            if now - timestamp > self.player._cover_art_cache_ttl
        ]
        for key in expired_keys:
            del self.player._cover_art_cache[key]

        # If still over max size, remove oldest entries
        if len(self.player._cover_art_cache) > self.player._cover_art_cache_max_size:
            sorted_entries = sorted(
                self.player._cover_art_cache.items(),
                key=lambda x: x[1][2],
            )
            for key, _ in sorted_entries[: len(self.player._cover_art_cache) - self.player._cover_art_cache_max_size]:
                del self.player._cover_art_cache[key]

    async def fetch_cover_art(self, url: str | None = None) -> tuple[bytes, str] | None:
        """Fetch cover art image from URL or return embedded fallback logo.

        Args:
            url: Cover art URL to fetch. If None, uses current track's cover art URL.
                If no valid URL is found, returns the embedded PyWiim logo (no HTTP call).

        Returns:
            Tuple of (image_bytes, content_type) if successful, None otherwise.
        """
        import base64

        from .properties import PlayerProperties

        if url is None:
            url = PlayerProperties(self.player).media_image_url

        # If no URL provided OR sentinel value, return embedded PyWiim logo directly (no HTTP call needed)
        from ..api.constants import DEFAULT_WIIM_LOGO_URL, EMBEDDED_LOGO_BASE64

        if not url or url == DEFAULT_WIIM_LOGO_URL:
            try:
                # Decode the embedded base64 PNG logo (join tuple of strings first)
                base64_string = "".join(EMBEDDED_LOGO_BASE64)
                logo_bytes = base64.b64decode(base64_string)
                _LOGGER.debug("Returning embedded PyWiim fallback logo (%d bytes)", len(logo_bytes))
                return (logo_bytes, "image/png")
            except Exception as e:
                _LOGGER.error("Failed to decode embedded logo: %s", e)
                return None

        # Clean up expired cache entries
        self._cleanup_cover_art_cache()

        # Check cache first
        url_hash = self._get_url_hash(url)
        if url_hash in self.player._cover_art_cache:
            cached_bytes, content_type, timestamp = self.player._cover_art_cache[url_hash]
            if time.time() - timestamp < self.player._cover_art_cache_ttl:
                _LOGGER.debug("Returning cover art from cache for URL: %s", url)
                return (cached_bytes, content_type)

        # Fetch from URL
        try:
            session = self.player.client._session
            should_close_session = False

            if session is None:
                session = aiohttp.ClientSession()
                should_close_session = True

            try:
                # Get SSL context from client if URL is HTTPS
                timeout = aiohttp.ClientTimeout(total=10)
                if url.startswith("https://"):
                    # Use the client's SSL context for HTTPS URLs
                    # This ensures we can fetch artwork from device URLs with self-signed certs
                    ssl_ctx = await self.player.client._get_ssl_context()
                    async with session.get(url, timeout=timeout, ssl=ssl_ctx) as response:
                        if response.status == 200:
                            image_bytes = await response.read()
                            content_type = response.headers.get("Content-Type", "image/jpeg")
                            if "image" not in content_type.lower():
                                content_type = "image/jpeg"

                            # Cache the result
                            self.player._cover_art_cache[url_hash] = (
                                image_bytes,
                                content_type,
                                time.time(),
                            )
                            _LOGGER.debug("Fetched and cached cover art from URL: %s", url)

                            # Clean up cache if needed
                            self._cleanup_cover_art_cache()

                            return (image_bytes, content_type)
                        else:
                            _LOGGER.debug(
                                "Failed to fetch cover art: HTTP %d from %s",
                                response.status,
                                url,
                            )
                            return None
                else:
                    # For HTTP URLs, use default SSL handling
                    async with session.get(url, timeout=timeout) as response:
                        if response.status == 200:
                            image_bytes = await response.read()
                            content_type = response.headers.get("Content-Type", "image/jpeg")
                            if "image" not in content_type.lower():
                                content_type = "image/jpeg"

                            # Cache the result
                            self.player._cover_art_cache[url_hash] = (
                                image_bytes,
                                content_type,
                                time.time(),
                            )
                            _LOGGER.debug("Fetched and cached cover art from URL: %s", url)

                            # Clean up cache if needed
                            self._cleanup_cover_art_cache()

                            return (image_bytes, content_type)
                        else:
                            _LOGGER.debug(
                                "Failed to fetch cover art: HTTP %d from %s",
                                response.status,
                                url,
                            )
                            return None
            finally:
                if should_close_session:
                    await session.close()
        except Exception as e:
            _LOGGER.debug("Error fetching cover art from %s: %s", url, e)
            return None

    def check_track_changed(self, merged_state: dict[str, Any]) -> bool:
        """Check if track changed based on title/artist/album signature.

        Args:
            merged_state: Merged state dictionary from StateSynchronizer.

        Returns:
            True if track changed, False otherwise.
        """
        # Build current track signature
        title = merged_state.get("title") or ""
        artist = merged_state.get("artist") or ""
        album = merged_state.get("album") or ""
        current_signature = f"{title}|{artist}|{album}"

        # Check if track changed
        track_changed = bool(
            current_signature and self._last_track_signature and current_signature != self._last_track_signature
        )

        if track_changed:
            self._last_track_signature = current_signature

        if not self._last_track_signature and current_signature:
            # First track detected
            self._last_track_signature = current_signature

        return track_changed

    async def enrich_metadata_on_track_change(self, merged_state: dict[str, Any]) -> None:
        """Fetch metadata from getMetaInfo when artwork missing or metadata is Unknown.

        This runs as a background task when:
        1. Track changed and artwork is missing, OR
        2. Metadata (title/artist) is "Unknown" (Bluetooth AVRCP case)

        Args:
            merged_state: Merged state dictionary from StateSynchronizer.
        """

        # Helper to check if metadata value is invalid/unknown
        def is_invalid_metadata(val: str | None) -> bool:
            if not val:
                return True
            val_lower = str(val).strip().lower()
            return val_lower in ("unknow", "unknown", "un_known", "", "none")

        # Build current track signature
        title = merged_state.get("title") or ""
        artist = merged_state.get("artist") or ""
        album = merged_state.get("album") or ""
        current_signature = f"{title}|{artist}|{album}"

        # Check if track changed
        track_changed = (
            current_signature and self._last_track_signature and current_signature != self._last_track_signature
        )

        # Check if metadata needs enrichment (title/artist/album are Unknown)
        # This is common with Bluetooth AVRCP where getPlayerStatusEx returns "Unknown"
        # but getMetaInfo has the actual track info
        needs_metadata_enrichment = is_invalid_metadata(title) or is_invalid_metadata(artist)

        if track_changed:
            self._last_track_signature = current_signature

        # Check if artwork is missing or is default logo
        image_url = merged_state.get("image_url")
        from ..api.constants import DEFAULT_WIIM_LOGO_URL

        has_valid_artwork = (
            image_url
            and str(image_url).strip()
            and str(image_url).strip().lower() not in ("unknow", "unknown", "un_known", "none", "")
            and str(image_url).strip() != DEFAULT_WIIM_LOGO_URL
        )

        # Fetch metadata from getMetaInfo if:
        # 1. Track changed and artwork is missing, OR
        # 2. Metadata (title/artist) is "Unknown" (Bluetooth AVRCP case)
        should_fetch_metadata = (track_changed and not has_valid_artwork) or needs_metadata_enrichment

        if should_fetch_metadata:
            if hasattr(self.player.client, "get_meta_info"):
                # Cancel any existing fetch task
                if self._artwork_fetch_task and not self._artwork_fetch_task.done():
                    self._artwork_fetch_task.cancel()

                # Start background task to fetch metadata
                try:
                    loop = asyncio.get_event_loop()
                    # ARYLIC PATCH: Try getMetaInfo first (standard path).
                    # If the device profile is Arylic AND a UPnP client is available,
                    # we use _fetch_artwork_from_upnp_position as a second fallback
                    # because Arylic A31 devices return albumArtURI inside the
                    # GetPositionInfo DIDL-Lite blob, not via getMetaInfo.
                    profile = getattr(self.player, "_profile", None)
                    is_arylic = profile is not None and getattr(profile, "vendor", "") == "arylic"
                    upnp_available = getattr(self.player, "_upnp_client", None) is not None
                    if is_arylic and upnp_available:
                        self._artwork_fetch_task = loop.create_task(
                            self._fetch_artwork_from_get_info_ex(merged_state)
                        )
                        _LOGGER.debug("Arylic device: fetching artwork+metadata via UPnP GetInfoEx")
                    else:
                        self._artwork_fetch_task = loop.create_task(self._fetch_artwork_from_metainfo(merged_state))
                    if needs_metadata_enrichment:
                        _LOGGER.debug("Metadata is Unknown, fetching from getMetaInfo")
                    else:
                        _LOGGER.debug("Track changed, fetching metadata from getMetaInfo")
                except RuntimeError:
                    # No event loop available (sync context) - will fetch on next poll
                    _LOGGER.debug("No event loop available, metadata will be fetched on next poll")

        if not self._last_track_signature and current_signature:
            # First track detected
            self._last_track_signature = current_signature

    async def _fetch_artwork_from_metainfo(self, merged_state: dict[str, Any]) -> None:
        """Fetch metadata and artwork from getMetaInfo and update state.

        This runs as a background task when track changes and artwork is missing.
        Also updates title/artist/album from getMetaInfo when the status endpoint
        returns "Unknown" values (common with Bluetooth AVRCP sources).

        Args:
            merged_state: Current merged state dictionary from StateSynchronizer.
        """
        try:
            if not hasattr(self.player.client, "get_meta_info"):
                return

            meta_info = await self.player.client.get_meta_info()
            if not meta_info or "metaData" not in meta_info:
                return

            meta_data = meta_info["metaData"]
            update: dict[str, Any] = {}

            # Helper to check if a value is invalid/unknown
            def is_invalid(val: str | None) -> bool:
                if not val:
                    return True
                val_lower = str(val).strip().lower()
                return val_lower in ("unknow", "unknown", "un_known", "", "none")

            # Extract and apply title if current is invalid and getMetaInfo has valid data
            meta_title = meta_data.get("title")
            if meta_title and not is_invalid(meta_title):
                current_title = merged_state.get("title")
                if is_invalid(current_title):
                    update["title"] = meta_title
                    update["Title"] = meta_title  # For Pydantic model alias

            # Extract and apply artist if current is invalid and getMetaInfo has valid data
            meta_artist = meta_data.get("artist")
            if meta_artist and not is_invalid(meta_artist):
                current_artist = merged_state.get("artist")
                if is_invalid(current_artist):
                    update["artist"] = meta_artist
                    update["Artist"] = meta_artist  # For Pydantic model alias

            # Extract and apply album if current is invalid and getMetaInfo has valid data
            meta_album = meta_data.get("album")
            if meta_album and not is_invalid(meta_album):
                current_album = merged_state.get("album")
                if is_invalid(current_album):
                    update["album"] = meta_album
                    update["Album"] = meta_album  # For Pydantic model alias

            # Extract artwork URL
            artwork_url = (
                meta_data.get("cover")
                or meta_data.get("cover_url")
                or meta_data.get("albumart")
                or meta_data.get("albumArtURI")
                or meta_data.get("albumArtUri")
                or meta_data.get("albumarturi")
                or meta_data.get("art_url")
                or meta_data.get("artwork_url")
                or meta_data.get("pic_url")
            )

            # Validate and add artwork URL
            if artwork_url and not is_invalid(artwork_url):
                # Basic URL validation
                if "http" in str(artwork_url).lower() or str(artwork_url).startswith("/"):
                    # Get metadata for cache-busting (use new values if we're updating)
                    title = update.get("title") or merged_state.get("title") or ""
                    artist = update.get("artist") or merged_state.get("artist") or ""
                    album = update.get("album") or merged_state.get("album") or ""
                    cache_key = f"{title}-{artist}-{album}"

                    if cache_key:
                        encoded = quote(cache_key)
                        sep = "&" if "?" in artwork_url else "?"
                        artwork_url = f"{artwork_url}{sep}cache={encoded}"

                    update["entity_picture"] = artwork_url

            # Apply updates if any
            if update:
                _LOGGER.debug("Applying metadata from getMetaInfo: %s", update)

                # Update state synchronizer
                self.player._state_synchronizer.update_from_http(update, timestamp=time.time())

                # Update cached status model
                merged = self.player._state_synchronizer.get_merged_state()
                if self.player._status_model:
                    if "title" in update:
                        self.player._status_model.title = merged.get("title")
                    if "artist" in update:
                        self.player._status_model.artist = merged.get("artist")
                    if "album" in update:
                        self.player._status_model.album = merged.get("album")
                    if "entity_picture" in update:
                        image_url = merged.get("image_url")
                        self.player._status_model.entity_picture = image_url
                        self.player._status_model.cover_url = image_url

                # Trigger callback to notify of update
                if self.player._on_state_changed:
                    try:
                        self.player._on_state_changed()
                    except Exception as err:
                        _LOGGER.debug("Error in callback after metadata update: %s", err)

                _LOGGER.debug(
                    "Fetched metadata from getMetaInfo: title=%s, artist=%s, album=%s",
                    update.get("title"),
                    update.get("artist"),
                    update.get("album"),
                )
        except asyncio.CancelledError:
            # Task was cancelled (new track change detected)
            pass
        except Exception as e:
            _LOGGER.debug("Error fetching metadata from getMetaInfo on track change: %s", e)

    async def _fetch_artwork_from_get_info_ex(self, merged_state: dict[str, Any]) -> None:
        """Fetch artwork AND full metadata via UPnP GetInfoEx for Arylic devices.

        GetInfoEx is a LinkPlay/Arylic-specific extended AVTransport action.
        Unlike the standard GetPositionInfo, it returns everything in one call:
        - TrackMetaData: DIDL-Lite XML with title, artist, album, albumArtURI
        - CurrentTransportState: so we know if the device is actually playing
        - RelTime / TrackDuration: current position and total duration
        - SlaveList: current multiroom group members (future use)

        This is more efficient and more complete than GetPositionInfo.
        If GetInfoEx fails (older firmware that does not support it),
        we fall back to getMetaInfo (the standard HTTP path).

        Args:
            merged_state: Current merged state dictionary.
        """
        import xml.etree.ElementTree as ET
        from html import unescape

        upnp_client = getattr(self.player, "_upnp_client", None)
        if upnp_client is None:
            await self._fetch_artwork_from_metainfo(merged_state)
            return

        artwork_url: str | None = None
        enriched: dict[str, Any] = {}

        try:
            # GetInfoEx: the LinkPlay-specific call that returns everything.
            # Documented in GetInfoEx.sh — returns a single response with
            # transport state, DIDL-Lite metadata blob, position, and more.
            info = await upnp_client.get_info_ex()
            raw_didl = info.get("TrackMetaData", "")

            if raw_didl and raw_didl.strip() not in ("", "NOT_IMPLEMENTED"):
                didl = unescape(str(raw_didl))

                try:
                    root = ET.fromstring(didl)
                    ns = {
                        "dc":   "http://purl.org/dc/elements/1.1/",
                        "upnp": "urn:schemas-upnp-org:metadata-1-0/upnp/",
                    }

                    # --- Album art ---
                    art_elem = root.find(".//upnp:albumArtURI", ns)
                    if art_elem is None:
                        art_elem = root.find(".//albumArtURI")
                    if art_elem is not None and art_elem.text:
                        candidate = art_elem.text.strip()
                        if candidate.startswith(("http://", "https://")):
                            artwork_url = candidate
                            _LOGGER.debug("GetInfoEx artwork URL: %s", artwork_url)

                    # --- Title ---
                    title_elem = root.find(".//dc:title", ns)
                    if title_elem is None:
                        title_elem = root.find(".//title")
                    if title_elem is not None and title_elem.text:
                        val = title_elem.text.strip()
                        if val and val.lower() not in ("unknown", "unknow"):
                            enriched["title"] = val

                    # --- Artist ---
                    artist_elem = root.find(".//upnp:artist", ns)
                    if artist_elem is None:
                        artist_elem = root.find(".//artist")
                    if artist_elem is not None and artist_elem.text:
                        val = artist_elem.text.strip()
                        if val and val.lower() not in ("unknown", "unknow"):
                            enriched["artist"] = val

                    # --- Album ---
                    album_elem = root.find(".//upnp:album", ns)
                    if album_elem is None:
                        album_elem = root.find(".//album")
                    if album_elem is not None and album_elem.text:
                        val = album_elem.text.strip()
                        if val and val.lower() not in ("unknown", "unknow"):
                            enriched["album"] = val

                except ET.ParseError as xml_err:
                    _LOGGER.debug("Could not parse GetInfoEx DIDL-Lite: %s", xml_err)

        except Exception as upnp_err:
            _LOGGER.debug(
                "GetInfoEx failed for %s, falling back to getMetaInfo: %s",
                getattr(self.player, "host", "?"),
                upnp_err,
            )
            # GetInfoEx not supported by this firmware — use standard path
            await self._fetch_artwork_from_metainfo(merged_state)
            return

        # Apply whatever we got from GetInfoEx to the player state
        if artwork_url:
            enriched["entity_picture"] = artwork_url
            enriched["cover_url"] = artwork_url

        if enriched:
            from ..api.constants import DEFAULT_WIIM_LOGO_URL

            # Remove default logo — not useful
            if enriched.get("entity_picture") == DEFAULT_WIIM_LOGO_URL:
                enriched.pop("entity_picture", None)
                enriched.pop("cover_url", None)

            self.player._state_synchronizer.update_from_http(
                enriched, timestamp=time.time()
            )
            if self.player._status_model:
                for key, val in enriched.items():
                    if hasattr(self.player._status_model, key):
                        setattr(self.player._status_model, key, val)
            if self.player._on_state_changed:
                try:
                    self.player._on_state_changed()
                except Exception as cb_err:
                    _LOGGER.debug("Callback error after GetInfoEx update: %s", cb_err)
            _LOGGER.debug(
                "Applied GetInfoEx enrichment to player state: %s",
                {k: v for k, v in enriched.items() if k != "entity_picture"},
            )
        else:
            # GetInfoEx returned nothing useful — fall back to getMetaInfo
            _LOGGER.debug("GetInfoEx returned no metadata, falling back to getMetaInfo")
            await self._fetch_artwork_from_metainfo(merged_state)

    async def get_cover_art_bytes(self, url: str | None = None) -> bytes | None:
        """Get cover art image bytes (convenience method).

        Args:
            url: Cover art URL to fetch. If None, uses current track's cover art URL.

        Returns:
            Image bytes if successful, None otherwise.
        """
        result = await self.fetch_cover_art(url)
        if result:
            return result[0]
        return None
