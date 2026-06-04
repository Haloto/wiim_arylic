"""WiiM coordinator - minimal integration layer using pywiim."""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from pywiim import Player, PollingStrategy, WiiMClient
from pywiim.exceptions import WiiMError

from .upnp_getinfoex import UpnpGetInfoExPoller

_LOGGER = logging.getLogger(__name__)


def _is_expected_unreachable_error(err: Exception) -> bool:
    """Return True when error indicates expected offline/unreachable device."""
    err_text = str(err).lower()
    return "device unreachable" in err_text or "connection failed on all attempted protocols" in err_text


def _compact_wiim_error(err: Exception) -> str:
    """Return compact error text to avoid log spam."""
    if _is_expected_unreachable_error(err):
        return "device unreachable"
    return str(err)


class WiiMCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """WiiM coordinator - minimal glue between pywiim and Home Assistant."""

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        entry=None,
        capabilities: dict[str, Any] | None = None,
        port: int | None = None,
        protocol: str | None = None,
        timeout: int = 10,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"WiiM {host}",
            update_interval=timedelta(seconds=5),  # Default, will adapt
        )
        self.hass = hass
        self.entry = entry
        self._capabilities = capabilities or {}

        # Get HA's shared aiohttp session (for connection pooling)
        session = async_get_clientsession(hass)

        # Create pywiim client with HA's session
        # Only pass port/protocol if we have a cached endpoint (optimized pattern)
        # Otherwise, let pywiim probe automatically (simplest pattern)
        client_kwargs = {
            "host": host,
            "timeout": timeout,
            "session": session,
            "capabilities": capabilities,
        }
        if port is not None and protocol is not None:
            # We have a cached endpoint - use it for faster startup
            client_kwargs["port"] = port
            client_kwargs["protocol"] = protocol
        # If port/protocol not provided, pywiim will probe automatically

        client = WiiMClient(**client_kwargs)

        # Wrap client in Player (recommended for HA - pywiim manages all state)
        # pywiim 2.1.70+ handles player linking internally via its player registry

        # We need to include player_finder and all_players_finder to enable cross-coordinator group linking.
        # In pywiim's player/groupsops.py the "Case 2" for device is master but we don't have a group object
        # short-circuits if there isn't a player_finder callback.
        # all_players_finder is also included as a final fallback in case the UUID lookup doesn't work for
        # some reason.

        self.player = Player(
            client,
            on_state_changed=self._on_player_state_changed,
            player_finder=self._player_finder,
            all_players_finder=self._all_players_finder,
        )

        # Use pywiim's PollingStrategy to determine when to poll
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})
        self._refresh_in_progress = False

        # UPnP GetInfoEx poller for cover art (not available via HTTP API).
        # Uses HA's shared session - same one passed to WiiMClient above.
        self._upnp_poller = UpnpGetInfoExPoller(host=host, session=session)
        # Track last injected values to detect changes and avoid redundant pushes.
        self._last_upnp_image_url: str | None = None
        self._last_upnp_play_state: str | None = None
        # Unsubscribe handle for the independent UPnP fast polling loop.
        self._upnp_loop_unsub: object | None = None

    def update_capabilities(self, capabilities: dict[str, Any]) -> None:
        """Apply a refreshed capabilities mapping (e.g. after firmware change).

        Config entry data is updated separately in ``__init__``; this keeps the
        coordinator, adaptive polling, and the pywiim client flags in sync.
        """
        merged = dict(capabilities)
        self._capabilities.clear()
        self._capabilities.update(merged)
        client_caps = getattr(self.player.client, "_capabilities", None)
        if client_caps is not None and client_caps is not self._capabilities:
            client_caps.clear()
            client_caps.update(merged)
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})

    def _player_finder(self, host_or_uuid: str) -> Player | None:
        """Find a Player object across all coordinators by host IP or UUID.

        Called by pywiim when it needs to resolve a slave's IP/UUID (from
        getSlaveList) to an actual Player object for group linking.
        """
        from .data import get_all_coordinators

        for coordinator in get_all_coordinators(self.hass):
            if coordinator is self:
                continue
            try:
                p = coordinator.player
                if getattr(p, "host", None) == host_or_uuid:
                    return p
                if getattr(p, "uuid", None) == host_or_uuid:
                    return p
            except Exception as err:
                _LOGGER.debug("Error in player_finder for %s: %s", host_or_uuid, _compact_wiim_error(err))
        return None

    def _all_players_finder(self) -> list[Player]:
        """Return all Player objects from every registered coordinator.

        Called by pywiim to infer slave role if e.g. a device is still reporting
        that it's solo even though it appears in another device's getSlaveList.
        """
        from .data import get_all_coordinators

        players = []
        for c in get_all_coordinators(self.hass):
            try:
                players.append(c.player)
            except Exception as err:
                _LOGGER.debug("Error in all_players_finder: %s", _compact_wiim_error(err))
        return players

    @callback
    def _on_player_state_changed(self) -> None:
        """Callback when pywiim Player detects state changes.

        Directly notifies listeners to update immediately without going through
        the coordinator's data update mechanism (which has throttling/debouncing).
        The callback fires AFTER pywiim has fully updated the Player object's
        properties (including metadata), so entities can read fresh data directly
        from self.player.
        """
        # Update coordinator's cached data reference (but don't trigger update flow)
        # This ensures self.data is always in sync with self.player
        self.data = {"player": self.player}

        # Directly notify all entities to refresh their state from the player
        # This bypasses DataUpdateCoordinator's throttling for immediate UI updates,
        # except while the coordinator is already performing a timed refresh. In
        # that case DataUpdateCoordinator publishes the completed refresh once.
        if self._refresh_in_progress:
            return
        self.async_update_listeners()

    def start_upnp_loop(self) -> None:
        """Start the independent 1-second UPnP polling loop.

        This loop runs completely separately from the HTTP coordinator cycle.
        It polls GetInfoEx every second and immediately notifies HA listeners
        when play_state changes — without waiting for the slow HTTP poll to finish.
        Called from async_setup_entry after the coordinator is created.
        """
        if self._upnp_loop_unsub is not None:
            return  # Already running

        self._upnp_loop_unsub = async_track_time_interval(
            self.hass,
            self._async_upnp_loop_tick,
            timedelta(seconds=1),
        )
        _LOGGER.debug("UPnP fast loop started for %s", self.player.host)

    def stop_upnp_loop(self) -> None:
        """Stop the independent UPnP polling loop."""
        if self._upnp_loop_unsub is not None:
            self._upnp_loop_unsub()
            self._upnp_loop_unsub = None
            _LOGGER.debug("UPnP fast loop stopped for %s", self.player.host)

    @callback
    def _async_upnp_loop_tick(self, _now: object) -> None:
        """Called every second by async_track_time_interval.

        Schedules the async UPnP poll as a fire-and-forget task so the
        callback itself stays synchronous (required by HA event loop).
        """
        self.hass.async_create_task(self._async_fast_upnp_poll())

    async def _async_fast_upnp_poll(self) -> None:
        """Poll GetInfoEx and push state to HA immediately on change.

        This is the fast path for play/pause/track-change detection.
        Runs every second independently of the 4-6s HTTP coordinator cycle.
        On play_state change it calls async_update_listeners() directly,
        which is the same mechanism used by _on_player_state_changed().
        """
        try:
            upnp_data = await self._upnp_poller.poll()
            if not upnp_data:
                return

            new_play_state = upnp_data.get("play_state")
            new_image_url = upnp_data.get("image_url")

            # Detect meaningful state changes that warrant an immediate HA push
            play_state_changed = (
                new_play_state is not None
                and new_play_state != self._last_upnp_play_state
            )
            image_url_changed = (
                new_image_url
                and new_image_url != self._last_upnp_image_url
            )

            if not play_state_changed and not image_url_changed:
                # Nothing interesting changed — inject silently but don't push
                # position/duration/metadata into state machine to avoid churn.
                # The regular _poll_upnp_cover_art inside _async_update_data
                # handles the full injection on each HTTP coordinator tick.
                return

            # Something changed — do full injection and notify HA immediately
            _LOGGER.debug(
                "UPnP fast loop: state change detected for %s "
                "(play_state: %s→%s, art_changed: %s)",
                self.player.host,
                self._last_upnp_play_state,
                new_play_state,
                image_url_changed,
            )

            if play_state_changed:
                self._last_upnp_play_state = new_play_state
            if image_url_changed:
                self._last_upnp_image_url = new_image_url

            # Build and inject the full payload
            inject: dict = {}
            if new_play_state is not None:
                inject["play_state"] = new_play_state
            if upnp_data.get("position") is not None:
                inject["position"] = upnp_data["position"]
            if upnp_data.get("duration") is not None:
                inject["duration"] = upnp_data["duration"]
            for field in ("title", "artist", "album"):
                val = upnp_data.get(field)
                if val:
                    inject[field] = val
            if new_image_url:
                inject["image_url"] = new_image_url

            state_sync = getattr(self.player, "_state_synchronizer", None)
            if state_sync is not None and hasattr(state_sync, "update_from_upnp"):
                state_sync.update_from_upnp(inject, timestamp=time.time())

            # Stamp status model for immediate visibility
            status_model = getattr(self.player, "_status_model", None)
            if status_model is not None:
                _model_map = {
                    "play_state": "play_state",
                    "position": "position",
                    "duration": "duration",
                    "title": "title",
                    "artist": "artist",
                    "album": "album",
                    "image_url": "entity_picture",
                }
                for inject_key, model_attr in _model_map.items():
                    if inject_key in inject:
                        try:
                            setattr(status_model, model_attr, inject[inject_key])
                        except (AttributeError, TypeError):
                            pass
                if "image_url" in inject:
                    try:
                        setattr(status_model, "cover_url", inject["image_url"])
                    except (AttributeError, TypeError):
                        pass

            # Push immediately to HA — don't wait for the HTTP coordinator tick
            self.data = {"player": self.player}
            if not self._refresh_in_progress:
                self.async_update_listeners()

        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "UPnP fast loop error for %s (non-fatal): %s",
                self.player.host,
                err,
            )

    async def _async_update_data(self) -> dict[str, Any]:
        """Update coordinator data - polls device following pywiim's PollingStrategy."""
        try:
            # Call player.refresh() to poll device and update cached state
            # PollingStrategy determines WHEN to poll (adaptive intervals)
            # Pre-seed entity_picture from previous UPnP poll so that
            # get_player_status() sees valid artwork and skips its bonus
            # getMetaInfo HTTP request. That extra request fires on every
            # tick when entity_picture is un_known/None (Spotify Connect
            # on Linkplay returns un_known from the HTTP API), and is the
            # primary cause of the 4-6s fetch times observed in the logs.
            if self._last_upnp_image_url:
                status_model = getattr(self.player, "_status_model", None)
                if status_model is not None:
                    for attr in ("entity_picture", "cover_url"):
                        try:
                            setattr(status_model, attr, self._last_upnp_image_url)
                        except (AttributeError, TypeError):
                            pass

            self._refresh_in_progress = True
            try:
                await self.player.refresh()
            finally:
                self._refresh_in_progress = False

            # --- UPnP GetInfoEx cover art poll (supplemental, best-effort) ---
            # The HTTP API does not expose albumArtURI. We poll the Linkplay
            # UPnP AVTransport endpoint (port 59152) ourselves and inject the
            # result into pywiim's state machine via update_from_upnp().
            # We do NOT use pywiim's built-in UPnP eventer because it requires
            # a push subscription that reliably times out on these devices.
            await self._poll_upnp_cover_art()
            # ------------------------------------------------------------------

            # Update polling interval using pywiim's PollingStrategy
            role = self.player.role
            is_playing = self.player.is_playing  # pywiim v2.1.37+ provides bool directly
            optimal_interval = self._polling_strategy.get_optimal_interval(role, is_playing)
            current_interval = self.update_interval.total_seconds() if self.update_interval else 5.0
            if current_interval != optimal_interval:
                self.update_interval = timedelta(seconds=optimal_interval)

            # Return Player object - it has everything (state, metadata, group info, etc.)
            if is_playing and _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug(
                    "Poll result for %s: state=%s, pos=%s, dur=%s, title='%s'",
                    self.player.host,
                    self.player.play_state,
                    self.player.media_position,
                    self.player.media_duration,
                    self.player.media_title,
                )

            result = {"player": self.player}
            return result

        except WiiMError as err:
            if _is_expected_unreachable_error(err):
                _LOGGER.debug("Update failed for %s: %s", self.player.host, _compact_wiim_error(err))
            else:
                _LOGGER.warning("Update failed for %s: %s", self.player.host, _compact_wiim_error(err))
            # Return cached Player object even on error
            if self.data:
                return self.data
            raise UpdateFailed(f"Failed to communicate with {self.player.host}: {_compact_wiim_error(err)}") from err

    async def _poll_upnp_cover_art(self) -> None:
        """Poll UPnP GetInfoEx and inject cover art URL into pywiim's state.

        This is a best-effort supplemental call. Any exception is swallowed so
        it never disrupts the main coordinator update cycle.

        Injection strategy
        ------------------
        We call ``player._state_synchronizer.update_from_upnp()`` with an
        ``image_url`` key. The state machine gives UPnP source priority over
        HTTP for ``image_url`` (see pywiim/state.py SOURCE_PRIORITY), so the
        art URL we inject will be picked up by ``player.media_image_url`` and
        subsequently by ``async_get_media_image()`` in the entity.

        We also update ``player._status_model.entity_picture`` / ``cover_url``
        directly as a belt-and-braces fallback, mirroring what pywiim's own
        ``_fetch_artwork_from_metainfo`` does.

        We only push an update when the image_url actually changes to avoid
        unnecessary state churn.
        """
        try:
            upnp_data = await self._upnp_poller.poll()
            if not upnp_data:
                return

            image_url: str | None = upnp_data.get("image_url")

            # Skip full injection if nothing has changed at all.
            # We check image_url as the proxy since it changes least frequently
            # (title/artist/position change every second while playing).
            # play_state and position are always injected regardless.
            image_url_changed = image_url != self._last_upnp_image_url
            if image_url and image_url_changed:
                self._last_upnp_image_url = image_url

            # --- Build injection payload ---
            # Inject all fields GetInfoEx provides. Every field here has UPnP
            # priority over HTTP in pywiim's SOURCE_PRIORITY, so these values
            # will win the merge and replace the sluggish HTTP responses.
            # Fields: play_state, position, duration, title, artist, album,
            # image_url — exactly what update_from_upnp() accepts natively.
            inject: dict = {}

            if upnp_data.get("play_state") is not None:
                inject["play_state"] = upnp_data["play_state"]

            if upnp_data.get("position") is not None:
                inject["position"] = upnp_data["position"]

            if upnp_data.get("duration") is not None:
                inject["duration"] = upnp_data["duration"]

            # Only inject metadata when it's actually present (not None).
            # Injecting None would overwrite valid HTTP metadata with nothing.
            for field in ("title", "artist", "album"):
                val = upnp_data.get(field)
                if val:
                    inject[field] = val

            if image_url:
                inject["image_url"] = image_url

            if not inject:
                return

            _LOGGER.debug(
                "UPnP GetInfoEx: injecting %s for %s",
                list(inject.keys()),
                self.player.host,
            )

            # --- Inject into pywiim state machine ---
            # update_from_upnp() handles all these fields natively and merges
            # them with correct source priority (UPnP > HTTP for all of them).
            state_sync = getattr(self.player, "_state_synchronizer", None)
            if state_sync is not None and hasattr(state_sync, "update_from_upnp"):
                state_sync.update_from_upnp(inject, timestamp=time.time())

            # Belt-and-braces: stamp the cached status model so
            # PlayerProperties sees position/duration/image_url immediately
            # on the current tick before the next merge cycle.
            status_model = getattr(self.player, "_status_model", None)
            if status_model is not None:
                _model_map = {
                    "position": "position",
                    "duration": "duration",
                    "play_state": "play_state",
                    "title": "title",
                    "artist": "artist",
                    "album": "album",
                    "image_url": "entity_picture",
                }
                for inject_key, model_attr in _model_map.items():
                    if inject_key in inject:
                        try:
                            setattr(status_model, model_attr, inject[inject_key])
                        except (AttributeError, TypeError):
                            pass
                # cover_url mirrors entity_picture
                if "image_url" in inject:
                    try:
                        setattr(status_model, "cover_url", inject["image_url"])
                    except (AttributeError, TypeError):
                        pass

        except Exception as err:  # noqa: BLE001
            # Never let cover art polling break the main update
            _LOGGER.debug(
                "UPnP cover art poll failed for %s (non-fatal): %s",
                self.player.host,
                err,
            )
