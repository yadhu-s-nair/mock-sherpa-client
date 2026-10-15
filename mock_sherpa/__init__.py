"""Programmable mock sherpa for testing and load generation (public API).

A thin wrapper over the protocol engine in engine.py, which implements the real sherpa <-> Fleet Manager
protocol (WebSocket status + HTTP acks/reached/peripherals, read from FM's source and verified against a live FM).
Everything below works out of the box; override any FM request with `on()`.

Usage:
    async with aiohttp.ClientSession() as session:
        sherpa = MockSherpa("sherpa-01", "api_key_123", "192.168.100.120:443", session)

        # Replace the behaviour for one FM request. handler(msg) or handler(msg, sherpa); sync or async.
        # Return a dict for the ack's response body, False to nack. default_handler() runs the built-in behaviour.
        sherpa.on("diagnostics", lambda msg: {"cpu": 12})
        async def slow_move(msg, sh):
            await asyncio.sleep(5)
            return await sh.default_handler("move_to")(msg)
        sherpa.on("move_to", slow_move)

        asyncio.create_task(sherpa.start())          # connect + telemetry loops, reconnects on failure

        sherpa.update_pose(x=10.0, y=5.0, theta=1.57)
        sherpa.update_battery(85.0)
        sherpa.update_mode("fleet")
        sherpa.set_error("E_LIDAR", "lidar timeout") # reports mode=error + error/error_info; clear_error() to recover
        ...
        sherpa.stop()

Built-in behaviour (see engine.py for the protocol notes):
    connect            X-API-Key header, self-signed certs accepted, never reconnects faster than FM allows
    sherpa_status      every 2 s (pose, battery, mode, error fields in error mode); mule_message every 10 s
    init               answers with display_name/hwid/sw_tag/... (FM needs it)
    reset_pose         adopts the pose (FM uses this to induct a sherpa)
    switch_mode        adopts the mode
    pause_resume       holds the current leg and reports it as paused
    terminate_trip     cancels the running leg
    move_to            runs a leg: trip_status ~1 Hz, then POST trip/reached (with destination pose/name).
                       Pass map_model=gates.MapModel.from_zip("<map>.zip") and speed_mps=... and the leg follows the
                       map's lane graph and behaves like a real sherpa at GATES: it asks FM for the visa
                       (POST access/resource) when it reaches a gate, STOPS and re-asks every 5 s while another
                       sherpa holds it ("Waiting for visa"), and releases it once clear. FM decides who gets the gate.
    revoke_visa        drops that gate; the sherpa asks again. reset_visas_held clears all.
    peripherals        answers dispatch button / hitch / lift / conveyor after `station_seconds`; without this FM keeps
                       the trip in `waiting_station` forever
    anything else      acked (FM blocks a worker on every un-acked request)

Approximations: the route is a shortest path over the map's lane graph, not mule's router (lane constraints,
orientation, turning), so a few routes differ in detail; the gates crossed and the corridors used match closely.
Parking/unparking visas are not distinguished (all "transit"). Without map_model legs are straight lines.
Not simulated: localisation, sensors/obstacles, alerts/incidents, diagnostics data, software updates, map/file
downloads, charging. Add them with on() or in the engine.

Class attributes you may read/set: pose, battery, mode, paused, connected, leg_seconds, station_seconds, stats.
"""
from .engine import MockSherpa as _Engine
from .gates import MapModel

__all__ = ["MockSherpa", "MapModel"]


class MockSherpa(_Engine):
    """MockSherpa(name, api_key, fm_host, session, start_pose=(0, 0, 0), tls=True, **options)

    fm_host is "host:port". tls=True -> wss/https (FM's port 443), False -> ws/http. Options (all keyword): leg_seconds=20,
    battery=100, sw_tag="mock-sherpa", mode="fleet", station_seconds=2, verify_ssl=False, on_move_to=None.
    """

    def __init__(self, name, api_key, fm_host, session, start_pose=(0.0, 0.0, 0.0), tls=True, **options):
        super().__init__(name, api_key, fm_host, session, start_pose=start_pose, tls=tls, **options)
