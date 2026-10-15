"""mock_sherpa_runner.py — asyncio adapter for running mock sherpas inside the automation suite.

Bridges the synchronous `provisioned_fleet()` context manager (conftest.py) with the async
`MockSherpa` engine (mock_sherpa.engine / mock_sherpa), which speaks the real
sherpa ↔ Fleet Manager protocol over WebSocket + HTTP.

The mock sherpas must already be registered in FM/DM (via dm_create_sherpa /
dm_assign_sherpa_to_fleet) before calling `start_mock_sherpas()`. This module only handles
the connection and lifecycle; DM registration is done by the existing provisioning code in
sil_fm_automation.py / sil.py.

Usage inside sil_fm_automation.py::provisioned_fleet():

    from mock_sherpa.runner import start_mock_sherpas

    entries = [{"name": "mock-sherpa-01", "api_key": "mock-sherpa-01"}, ...]
    runner = start_mock_sherpas(entries, fm_host="fm.example.com:443",
                                map_zip="/path/to/the_map.zip")
    runner.start()
    runner.wait_all_connected(timeout=60)
    ...
    runner.stop()
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import threading
import time
from typing import List, Optional

log = logging.getLogger("mock_sherpa_runner")

from . import MockSherpa
from .gates import MapModel

_GATES_AVAILABLE = True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class MockSherpaRunner:
    """Manages a pool of MockSherpa instances running in a dedicated asyncio event loop
    on a background daemon thread.

    The loop runs in isolation so it never competes with pytest's own event loop (if any),
    and stops cleanly when `stop()` is called (or the process exits).
    """

    def __init__(
        self,
        entries: List[dict],
        fm_host: str,
        *,
        tls: bool = True,
        leg_seconds: float = 20.0,
        station_seconds: float = 2.0,
        mode: str = "fleet",
        map_zip: Optional[str] = None,
        speed_mps: float = 1.5,
        **sherpa_options,
    ):
        """
        entries        : list of {"name": ..., "api_key": ...}
        fm_host        : "host:port" (e.g. "fm.example.com:443")
        tls            : True → wss / https; False → ws / http
        leg_seconds    : simulated time per trip leg (20 s matches perf default)
        station_seconds: time at a station before answering a peripherals request
        mode           : initial mode reported to FM ("fleet" | "simulation")
        map_zip        : path to the map ZIP for realistic lane-following + gate handling;
                         None → straight-line legs (no gate/visa logic)
        speed_mps      : movement speed along lanes when map_zip is provided
        sherpa_options : any other MockSherpa keyword (handshake=False, chaos={...}, client_ip=..., on_move_to=...)
        """
        self._sherpa_options = sherpa_options
        self._entries = entries
        self._fm_host = fm_host
        self._tls = tls
        self._leg_seconds = leg_seconds
        self._station_seconds = station_seconds
        self._mode = mode
        self._map_zip = map_zip
        self._speed_mps = speed_mps

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._sherpas: List[MockSherpa] = []
        self._started = threading.Event()   # set once the loop + sherpas are initialised
        self._start_exc: Optional[Exception] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> "MockSherpaRunner":
        """Spin up the asyncio loop on a daemon thread and begin connecting all sherpas.
        Returns self so callers can chain: runner = start_mock_sherpas(...).start()
        """
        if self._thread is not None:
            raise RuntimeError("MockSherpaRunner.start() called more than once")
        self._thread = threading.Thread(
            target=self._run_loop, name="mock-sherpa-runner", daemon=True
        )
        self._thread.start()
        # Block until the loop + sherpas are initialised (or failed)
        self._started.wait(timeout=30)
        if self._start_exc is not None:
            raise self._start_exc
        return self

    def stop(self, join_timeout: float = 10.0) -> None:
        """Signal all mock sherpas to disconnect and wait for the background thread to exit."""
        if self._loop is None or self._stop_event is None:
            return
        # Schedule the stop on the runner's own loop (thread-safe)
        self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread is not None:
            self._thread.join(timeout=join_timeout)
        log.info("MockSherpaRunner stopped (%d sherpas)", len(self._sherpas))

    def wait_all_connected(self, timeout: float = 60.0) -> bool:
        """Block until every mock sherpa reports `connected=True`, or `timeout` elapses.
        Returns True if all are connected, False if the timeout was reached first.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._sherpas and all(s.connected for s in self._sherpas):
                return True
            time.sleep(0.5)
        still_waiting = [s.name for s in self._sherpas if not s.connected]
        if still_waiting:
            log.warning("wait_all_connected: %d sherpa(s) not yet connected: %s",
                        len(still_waiting), still_waiting)
        return not bool(still_waiting)

    def run(self, coro, timeout: float = 60.0):
        """Run a coroutine on the runner's loop from a synchronous test and return its result, e.g.
        `runner.run(sherpa.power_cut(5))` or `runner.run(sherpa.go_offline(10))`."""
        if self._loop is None:
            raise RuntimeError("runner not started")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    @property
    def sherpas(self) -> List[MockSherpa]:
        """The live MockSherpa instances (read-only)."""
        return list(self._sherpas)

    # ------------------------------------------------------------------
    # Internal asyncio runner
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Entry point of the daemon thread: create a new event loop, set it up, run until stopped."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._async_main())
        except Exception as exc:  # noqa: BLE001
            log.error("MockSherpaRunner loop crashed: %r", exc)
        finally:
            loop.close()

    async def _async_main(self) -> None:
        """Main coroutine: build all MockSherpa instances, start their tasks, wait for stop."""
        import aiohttp  # imported here to keep top-level imports cheap

        self._stop_event = asyncio.Event()

        # Load the map model once (shared across all sherpas on the same map)
        map_model = None
        if self._map_zip and _GATES_AVAILABLE and MapModel is not None:
            try:
                map_model = MapModel.from_zip(self._map_zip)
                log.info("MockSherpaRunner: loaded map model from %s "
                         "(%d stations, %d gates)",
                         self._map_zip,
                         len(map_model.stations),
                         len(map_model.gates))
            except Exception as exc:  # noqa: BLE001
                log.warning("MockSherpaRunner: could not load map model from %s "
                            "(%r) — falling back to straight-line legs", self._map_zip, exc)

        conn = aiohttp.TCPConnector(ssl=False, limit=0)
        timeout = aiohttp.ClientTimeout(total=30)

        async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
            self._sherpas = [
                MockSherpa(
                    name=e["name"],
                    api_key=e["api_key"],
                    fm_host=self._fm_host,
                    session=session,
                    start_pose=(10.0 + 2.0 * i, 10.0, 0.0),
                    tls=self._tls,
                    leg_seconds=self._leg_seconds,
                    station_seconds=self._station_seconds,
                    mode=self._mode,
                    map_model=map_model,
                    speed_mps=self._speed_mps,
                    **{"verify_ssl": False, **self._sherpa_options},
                )
                for i, e in enumerate(self._entries)
            ]

            # Signal that sherpas are built (start() can return now)
            self._started.set()

            # Launch all sherpa coroutines as tasks
            tasks = [asyncio.ensure_future(s.start()) for s in self._sherpas]

            # Wait until the stop_event is set (by stop() from the main thread)
            await self._stop_event.wait()

            # Gracefully stop all sherpas
            for s in self._sherpas:
                s.stop()

            # Wait for their tasks to finish
            await asyncio.gather(*tasks, return_exceptions=True)

        log.info("MockSherpaRunner: all %d sherpa tasks finished", len(self._sherpas))


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def start_mock_sherpas(
    entries: List[dict],
    fm_host: str,
    *,
    tls: bool = True,
    leg_seconds: float = 20.0,
    station_seconds: float = 2.0,
    mode: str = "fleet",
    map_zip: Optional[str] = None,
    speed_mps: float = 1.5,
    **sherpa_options,
) -> MockSherpaRunner:
    """Create and return a `MockSherpaRunner` (NOT yet started).

    Call `.start()` to connect the sherpas and `.wait_all_connected()` to confirm they
    are online before proceeding with trip booking.

    Args:
        entries        : [{"name": ..., "api_key": ...}, ...]
        fm_host        : "host:port" string
        tls            : True → wss / https (FM's port 443)
        leg_seconds    : simulated travel time per trip leg
        station_seconds: wait at station before answering peripherals request
        mode           : initial sherpa mode ("fleet" or "simulation")
        map_zip        : absolute path to map zip for lane-following + gate handling;
                         None → straight-line legs
        speed_mps      : m/s along lanes when map_zip is used
        sherpa_options : passed through to every MockSherpa (handshake, chaos, client_ip, on_move_to, ...)
    """
    return MockSherpaRunner(
        entries=entries,
        fm_host=fm_host,
        tls=tls,
        leg_seconds=leg_seconds,
        station_seconds=station_seconds,
        mode=mode,
        map_zip=map_zip,
        speed_mps=speed_mps,
        **sherpa_options,
    )
