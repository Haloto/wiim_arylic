"""WiiM coordinator - minimal integration layer using pywiim."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
# Import UpnpClient instead of legacy WiiMClient names
from pywiim import Player, PollingStrategy, UpnpClient
from pywiim.exceptions import WiiMError

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
        self._host = host  # Save explicit host IP string for internal HA use
        self._capabilities = capabilities or {}

        # Get HA's shared aiohttp session (for connection pooling)
        session = async_get_clientsession(hass)

        # Map out the correct port number, defaulting to standard UPnP 49152
        port_num = port or 49152

        # Initialize the modern UpnpClient wrapper
        self.client = UpnpClient(
            host=host,
            description_url=f"http://{host}:{port_num}/description.xml",
            session=session,
        )

        # Wrap client in Player (pywiim manages all state)
        self.player = Player(
            self.client,
            on_state_changed=self._on_player_state_changed,
            player_finder=self._player_finder,
            all_players_finder=self._all_players_finder,
        )

        # Use pywiim's PollingStrategy to determine when to poll
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})
        self._refresh_in_progress = False

    def update_capabilities(self, capabilities: dict[str, Any]) -> None:
        """Apply a refreshed capabilities mapping (e.g. after firmware change)."""
        merged = dict(capabilities)
        self._capabilities.clear()
        self._capabilities.update(merged)
        client_caps = getattr(self.player.client, "_capabilities", None)
        if client_caps is not None and client_caps is not self._capabilities:
            client_caps.clear()
            client_caps.update(merged)
        self._polling_strategy = PollingStrategy(self._capabilities) if self._capabilities else PollingStrategy({})

    def _player_finder(self, host_or_uuid: str) -> Player | None:
        """Find a Player object across all coordinators by host IP or UUID."""
        from .data import get_all_coordinators

        for coordinator in get_all_coordinators(self.hass):
            if coordinator is self:
                continue
            try:
                p = coordinator.player
                p_host = getattr(p, "host", None) or getattr(coordinator, "_host", None)
                if p_host == host_or_uuid:
                    return p
                if getattr(p, "uuid", None) == host_or_uuid:
                    return p
            except Exception as err:
                _LOGGER.debug("Error in player_finder for %s: %s", host_or_uuid, _compact_wiim_error(err))
        return None

    def _all_players_finder(self) -> list[Player]:
        """Return all Player objects from every registered coordinator."""
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
        """Callback when pywiim Player detects state changes."""
        self.data = {"player": self.player}
        if self._refresh_in_progress:
            return
        self.async_update_listeners()

    async def _async_update_data(self) -> dict[str, Any]:
        """Update coordinator data - polls device following pywiim's PollingStrategy."""
        try:
            self._refresh_in_progress = True
            try:
                # Direct modern UPnP method state resolution to bypass missing legacy models
                transport_info = await self.client.get_transport_info()
                position_info = await self.client.get_position_info()
                media_info = await self.client.get_media_info()
                
                # Dynamically push raw properties onto the stateful player object if it doesn't auto-update
                if hasattr(self.player, "update_from_upnp"):
                    self.player.update_from_upnp(transport_info, position_info, media_info)
                else:
                    # Generic fallback execution if player has its own refresh routine
                    await self.player.refresh()
            finally:
                self._refresh_in_progress = False

            # ARYLIC PATCH: Proactively initialize UPnP client for Arylic devices.
            if getattr(self.player, "_upnp_client", None) is None:
                profile = getattr(self.player, "_profile", None)
                vendor = getattr(profile, "vendor", "") if profile else ""
                if vendor == "arylic" and hasattr(self.player, "_ensure_upnp_client"):
                    try:
                        await self.player._ensure_upnp_client()
                    except Exception as _upnp_err:
                        _LOGGER.debug("Arylic UPnP client init failed for %s: %s", self._host, _upnp_err)

            # Update polling interval using pywiim's PollingStrategy
            role = self.player.player_role if hasattr(self.player, 'player_role') else getattr(self.player, 'role', 'solo')
            is_playing = getattr(self.player, 'is_playing', False)
            optimal_interval = self._polling_strategy.get_optimal_interval(role, is_playing)
            current_interval = self.update_interval.total_seconds() if self.update_interval else 5.0
            if current_interval != optimal_interval:
                self.update_interval = timedelta(seconds=optimal_interval)

            result = {"player": self.player}
            return result

        except Exception as err:
            if _is_expected_unreachable_error(err):
                _LOGGER.debug("Update failed for %s: %s", self._host, _compact_wiim_error(err))
            else:
                _LOGGER.warning("Update failed for %s: %s", self._host, _compact_wiim_error(err))
            if self.data:
                return self.data
            raise UpdateFailed(f"Failed to communicate with {self._host}: {_compact_wiim_error(err)}") from err
