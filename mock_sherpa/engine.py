#!/usr/bin/env python3
"""Mock sherpas for Fleet Manager load testing (AF-1376).

Speaks the real sherpa<->FM protocol (read from fleet_manager/app/routers/sherpa_ws.py,
sherpa_http.py and mule/ati/comms/fm/fleet_bridge_utils.py) without the heavy mule stack:

  * WebSocket  wss|ws://<fm>:<port>/ws/api/v1/sherpa/   header  X-API-Key: <plain key>
      sherpa -> FM   sherpa_status  every 2 s   (FM drops same-type msgs < 500 ms apart or with
                                                 a non-increasing timestamp)
                     trip_status    ~1 Hz while a leg is active
                     mule_message   every 10 s
      FM -> sherpa   JSON {endpoint, req_id, ack_reqd, ...}: init, move_to, reset_pose,
                     switch_mode, terminate_trip, pause_resume, ... (anything else is just acked)
  * HTTP       POST /api/v1/sherpa/req_ack/<req_id>   {"success": true, "response": {...}}
               POST /api/v1/sherpa/trip/reached       (after a move_to leg completes)

FM blocks a request worker on every un-acked request, so every request is acked.
The sherpa must already exist in FM/DM (see provision.py); this client only connects.

Python 3.8+, depends on aiohttp only. FM allows at most 4 WS connects / 60 s per sherpa.
"""
import argparse
import asyncio
import collections
import inspect
import json
import logging
import math
import os
import random
import signal
import sys
import time
from collections import Counter

import aiohttp

from .gates import MapModel, STEP

log = logging.getLogger("mock_sherpa")
EVENT_DIR = None                                  # when set, every mock sherpa appends what it does to <EVENT_DIR>/<sherpa name>.log


def set_event_dir(path):
    """Keep a log per mock sherpa under `path` (a run's sherpa_logs folder): connects, every FM request, legs, gates, obstacles and errors."""
    global EVENT_DIR
    EVENT_DIR = path
    if path:
        os.makedirs(path, exist_ok=True)


ERRORS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherpa_errors.json")


def load_error_catalogue(path=ERRORS_FILE):
    """{error key: catalogue entry} of the errors a real sherpa raises mid-trip (copied from mule's fatal-error catalogue)."""
    try:
        with open(path) as fh:
            return json.load(fh).get("errors") or {}
    except (OSError, ValueError):
        return {}


def chaos_from_args(args):
    """The per-sherpa error / obstacle settings from the command line (None = a well-behaved sherpa, as before)."""
    e, o = float(getattr(args, "errors_per_min", 0) or 0), float(getattr(args, "obstacles_per_min", 0) or 0)
    leak = float(getattr(args, "visa_leak_pct", 0) or 0)
    if e <= 0 and o <= 0 and leak <= 0:
        return None
    return {"error_per_min": e, "obstacle_per_min": o, "obstacle_s": float(getattr(args, "obstacle_seconds", 5.0) or 5.0),
            "error_s": 3.0, "errors": load_error_catalogue() if e > 0 else {},
            "visa_leak_pct": leak, "visa_hold_s": float(getattr(args, "visa_hold_seconds", 60.0) or 60.0)}

STATUS_PERIOD = 2.0
TRIP_STATUS_PERIOD = 1.0
MULE_PERIOD = 10.0
MIN_RECONNECT_GAP = 16.0      # FM closes sherpas that connect > 4x per 60 s
MAX_SAMPLES = 20000           # per-sherpa latency samples kept (a uniform random sample of the whole run once more arrive)


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    lo, hi = int(math.floor(k)), int(math.ceil(k))
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


class MockSherpa:
    """One simulated sherpa speaking the real sherpa<->FM protocol.

    Out of the box it connects, reports status, answers FM's init/reset_pose/switch_mode/pause_resume/terminate_trip,
    runs `move_to` legs (trip_status ~1 Hz, then `reached`), answers the `peripherals` request that follows a leg, and
    acks everything else. Script it with `on()` and the update_* helpers (see mock_sherpa/__init__.py for the public facade).
    """

    def __init__(self, name, api_key, base, session, *, start_pose=(10.0, 10.0, 0.0), leg_seconds=20.0, battery=100.0,
                 sw_tag="mock-sherpa", mode="fleet", on_move_to=None, station_seconds=2.0, tls=None, verify_ssl=False,
                 map_model=None, speed_mps=1.5, visa_retry_s=5.0, client_ip=None, chaos=None, handshake=True):
        self.name, self.api_key, self.base = name, api_key, base      # base = "host:port"
        self.session = session
        self.pose = list(start_pose)
        self.leg_seconds = leg_seconds
        self.battery = battery
        self.sw_tag = sw_tag
        self.handshake_on = handshake
        self.mode = mode
        self.map_model = map_model            # gates.MapModel: follow the lane graph and wait at gates (else straight line)
        self.speed_mps = speed_mps
        self.visa_retry_s = visa_retry_s      # mule re-sends a denied visa request every 5 s
        self.held = {}                        # gate name -> visa type held (cleared by FM's reset_visas_held / revoke_visa)
        self.error, self.error_info = None, None      # reported while mode == "error"
        self.paused = False
        self._bg = set()                      # late gate releases still waiting (cancelled on stop)
        self.chaos = chaos                    # {"error_per_min", "obstacle_per_min", "obstacle_s", "error_s", "errors"} or None
        self.station_seconds = station_seconds
        self.on_move_to = on_move_to          # callback(trip_id, sherpa_name, wall_time) when a move_to arrives
        self.hdr = {"X-API-Key": api_key}
        if client_ip:
            # FM's rate limiters (fastapi-limiter, default identifier) key on IP + path, and nginx forwards the first
            # X-Forwarded-For value. One load-generator host would share a single 15 requests/min budget for the visa
            # endpoint across ALL its sherpas; real vehicles each have their own IP, so give each mock its own.
            self.hdr["X-Forwarded-For"] = client_ip
        self.tls = (base.rsplit(":", 1)[-1] == "443") if tls is None else bool(tls)
        self._ssl = None if verify_ssl else False       # FM uses a self-signed cert: don't verify unless asked
        self.http_base = ("https://" if self.tls else "http://") + base
        self.ws_url = ("wss://" if self.tls else "ws://") + base + "/ws/api/v1/sherpa/"
        self._ts = {}                         # type -> last timestamp sent (strictly increasing)
        self.trip_task = None
        self.ws = None
        self._legs = {}                       # (trip_id, leg_id) -> {"destination_pose", "destination_name"}
        self.stats = Counter()
        self.requests = collections.deque(maxlen=2000)   # the newest FM requests received, oldest first: (wall time, endpoint, msg); see requests_for()
        self.close_codes = Counter()
        self.ack_ms = []
        self.ack_max = 0.0                    # true maximum: the kept samples may be a random subset of a long run
        self._seen = {}                       # id(list) -> how many samples were offered (for the random subset)
        self.recent_ack = collections.deque(maxlen=40)       # newest ack latencies
        self.leg_ms = []                      # move_to received -> reached POST done
        self.connected = False
        self.offline_until = 0.0
        self.stop_event = asyncio.Event()
        self._handlers = {}                   # user overrides: endpoint -> callable
        self._defaults = {"init": self._h_init, "reset_pose": self._h_reset_pose, "switch_mode": self._h_switch_mode,
                          "terminate_trip": self._h_terminate_trip, "pause_resume": self._h_pause_resume,
                          "move_to": self._h_move_to, "peripherals": self._h_peripherals,
                          "revoke_visa": self._h_revoke_visa, "reset_visas_held": self._h_reset_visas_held,
                          "current_sound_status": self._h_current_sound_status, "sound": self._h_sound,
                          "update_sherpa_config": self._h_update_sherpa_config, "lifter_actuator_manual": self._h_ok,
                          "auto_recover": self._h_ok, "powercycle": self._h_ok, "restart_mule_docker": self._h_ok,
                          "restart_all_containers": self._h_ok}
        self.sound = {"volume": 0.5, "sound_type": "beep"}
        self._mule_empty_sent = False         # a real sherpa sends ONE empty mule_message (to clear FM's copy), then only when it has messages

    def ev(self, text):
        """One line in this sherpa's own log (a no-op unless set_event_dir was called). Never raises: a log must not stop a sherpa."""
        if not EVENT_DIR:
            return
        try:
            fh = getattr(self, "_evf", None)
            if fh is None:
                fh = self._evf = open(os.path.join(EVENT_DIR, "%s.log" % self.name), "a", buffering=1, encoding="utf-8")
            now = time.time()
            fh.write("%s.%03d %s\n" % (time.strftime("%H:%M:%S", time.localtime(now)), int((now % 1) * 1000), text))
        except OSError:
            pass

    # ── scripting API ────────────────────────────────────────────────────────
    def on(self, endpoint, handler):
        """Replace the behaviour for one FM request. handler(msg) or handler(msg, sherpa), sync or async. Return a dict
        to use as the ack's response body, False to nack, anything else to ack with an empty body. Use
        `default_handler(endpoint)(msg)` to run the built-in behaviour as part of yours."""
        self._handlers[endpoint] = handler

    def default_handler(self, endpoint):
        return self._defaults[endpoint]

    def update_pose(self, x, y, theta):
        self.pose = [x, y, theta]

    def update_battery(self, battery_pct):
        self.battery = battery_pct

    def update_mode(self, mode):
        self.mode = mode

    def set_error(self, error, info=None):
        """Report an error: sets mode to "error" and sends error/error_info with every sherpa_status."""
        self.mode, self.error, self.error_info = "error", error, info if info is not None else error

    def clear_error(self, mode="fleet"):
        self.mode, self.error, self.error_info = mode, None, None

    def start(self):
        """Coroutine: connect and keep reconnecting until stop() (use asyncio.create_task(sherpa.start()))."""
        return self.run(0)

    async def go_offline(self, seconds):
        """Simulate a network outage: drop the websocket and stay away from FM for `seconds`. The leg being driven carries on underneath; what has to be told
        to FM (that the leg's destination was reached) is sent when the connection is back, like the real sherpa's retried 'reached' event."""
        self.offline_until = time.monotonic() + seconds
        self.ev("going offline for %.0f s (the leg continues)" % seconds)
        if self.ws is not None:
            await self.ws.close()

    async def power_cut(self, off_seconds):
        """Simulate an unclean power-off: unlike go_offline() the leg being driven DIES with the vehicle (no 'reached' is ever sent for it), the
        in-memory state (held visas, pause, error) is lost, and the sherpa stays away from FM for `off_seconds` before booting back up. The pose is kept where
        it stopped, so what FM re-sends afterwards (see requests_for("move_to")) arrives for a vehicle that is still on the spot, like pm-v2-16 after a key-off."""
        self.ev("POWER CUT: leg lost, off for %.0f s" % off_seconds)
        self.cancel_trip()
        for t in list(self._bg):
            t.cancel()
        self.held.clear()
        self.paused = False
        self.error, self.error_info = None, None
        if self.mode == "error":
            self.mode = "fleet"
        await self.go_offline(off_seconds)

    def requests_for(self, endpoint, since=0.0):
        """FM requests of one endpoint received after wall time `since` (e.g. the move_to legs re-sent after a reconnect)."""
        return [(t, m) for t, ep, m in self.requests if ep == endpoint and t > since]

    def stop(self):
        self.stop_event.set()
        self.cancel_trip()
        for t in list(self._bg):
            t.cancel()

    # ── helpers ──────────────────────────────────────────────────────────────
    def ts(self, typ):
        t = max(time.time(), self._ts.get(typ, 0.0) + 0.001)
        self._ts[typ] = t
        return t

    def sample(self, lst, v):
        """Keep v; beyond MAX_SAMPLES replace a random kept one with probability MAX/n (reservoir sampling), so percentiles
        describe the whole run and not only its first MAX_SAMPLES requests."""
        n = self._seen[id(lst)] = self._seen.get(id(lst), 0) + 1
        if len(lst) < MAX_SAMPLES:
            lst.append(v)
        else:
            j = random.randrange(n)
            if j < MAX_SAMPLES:
                lst[j] = v

    async def _send(self, payload):
        ws = self.ws
        if ws is None or ws.closed:
            return False
        await ws.send_json(payload)
        return True

    async def post(self, path, body):
        t0 = time.monotonic()
        try:
            async with self.session.post(self.http_base + path, json=body, headers=self.hdr, ssl=self._ssl) as r:
                await r.read()
                if r.status >= 400:
                    self.stats["http_%dxx" % (r.status // 100)] += 1
                    log.debug("%s POST %s -> %s", self.name, path, r.status)
                    return False, (time.monotonic() - t0) * 1000
                return True, (time.monotonic() - t0) * 1000
        except Exception as e:                                   # noqa: BLE001
            self.stats["http_exc"] += 1
            log.debug("%s POST %s failed: %r", self.name, path, e)
            return False, (time.monotonic() - t0) * 1000

    async def post_json(self, path, body):
        """POST and return (http_status, response_json_or_{}) - the visa answer is in the response body."""
        try:
            async with self.session.post(self.http_base + path, json=body, headers=self.hdr, ssl=self._ssl) as r:
                txt = await r.text()
                try:
                    return r.status, json.loads(txt)
                except ValueError:
                    return r.status, {}
        except Exception as e:                                   # noqa: BLE001
            self.stats["http_exc"] += 1
            log.debug("%s POST %s failed: %r", self.name, path, e)
            return 0, {}

    async def get(self, path):
        """GET an FM endpoint with the sherpa's key: (http status, parsed json or {})."""
        try:
            async with self.session.get(self.http_base + path, headers=self.hdr, ssl=self._ssl) as r:
                txt = await r.text()
                try:
                    return r.status, json.loads(txt)
                except ValueError:
                    return r.status, {}
        except Exception as e:                                   # noqa: BLE001
            self.stats["http_exc"] += 1
            log.debug("%s GET %s failed: %r", self.name, path, e)
            return 0, {}

    async def handshake(self, steps):
        """The requests a real sherpa makes around connecting (mule fleet_bridge): before the websocket check_connection, the version
        compatibility check, the static-file credentials and basic_info; after it verify_fleet_files and the config file hashes."""
        for label, path in steps:
            st, _ = await self.get(path)
            ok = st == 200
            self.stats["handshake_ok" if ok else "handshake_fail"] += 1
            self.ev("handshake %s: %s" % (label, "ok" if ok else "HTTP %s" % st))

    PRE_CONNECT = (("check_connection", "/api/v1/sherpa/check_connection"), ("software compatibility", "/api/v1/sherpa/is_sherpa_version_compatible/%s"),
                   ("static file credentials", "/api/v1/sherpa/get_static_files_auth_credentials"), ("basic info", "/api/v1/sherpa/basic_info"))
    POST_CONNECT = (("verify fleet files", "/api/v1/sherpa/verify_fleet_files"), ("config file hashes", "/api/v1/sherpa/get_config_file_info"))

    # ── sherpa -> FM ─────────────────────────────────────────────────────────
    async def send_status(self, ws=None):
        msg = {"type": "sherpa_status", "timestamp": self.ts("sherpa_status"), "sherpa_name": self.name,
               "current_pose": self.pose, "battery_status": round(self.battery, 1), "mode": self.mode}
        if self.mode == "error":
            msg["error"], msg["error_info"] = True, self.error_info or self.error or "error"     # a real sherpa sends error: true (FM's model is a bool)
        if await self._send(msg):
            self.stats["status_sent"] += 1

    async def send_mule(self, ws=None):
        if await self._send({"type": "mule_message", "sherpa_name": self.name,
                             "timestamp": self.ts("mule_message"), "message_jsons": []}):
            self.stats["mule_sent"] += 1

    async def send_trip_status(self, trip_id, trip_leg_id, destination_pose, destination_name="", progress=0.0,
                               total_length=1.0, stoppage="", cte=0.0, te=0.0, extra=None):
        """One trip_status message (FM wants all of these fields). `extra` overrides fields of stoppages.extra_info."""
        msg = ({
            "type": "trip_status", "timestamp": self.ts("trip_status"), "trip_id": trip_id, "trip_leg_id": trip_leg_id,
            "trip_info": {"current_pose": self.pose, "destination_pose": destination_pose,
                          "destination_name": destination_name, "total_route_length": total_length,
                          "remaining_route_length": total_length * (1 - progress), "cte": cte, "te": te,
                          "eta_at_start": float(self.leg_seconds), "eta": self.leg_seconds * (1 - progress),
                          "progress": progress},
            "stoppages": {"type": stoppage, "extra_info": {
                "velocity_speed_factor": 0.0 if stoppage else 1.0, "obstacle_speed_factor": 0, "local_obstacle": [],
                "time_elapsed_stoppages": 0, "time_elapsed_obstacle_stoppages": 0,
                "time_elapsed_visa_stoppages": 0, "time_elapsed_other_stoppages": 0}}})
        msg["stoppages"]["extra_info"].update(extra or {})
        await self._send(msg)
        self.stats["trip_status_sent"] += 1

    async def release_gate(self, gate):
        """Give a gate back to FM. With chaos on, a share of releases is 'forgotten' like a real sherpa that never sends it: the sherpa keeps
        the visa, FM keeps refusing the others, and the release only happens visa_hold_s later (or never, if FM revokes the visa first)."""
        c = self.chaos
        if c and c.get("visa_leak_pct", 0) > 0 and random.random() * 100.0 < c["visa_leak_pct"]:
            self.stats["chaos_visa_not_released"] += 1
            task = asyncio.ensure_future(self._late_release(gate, c["visa_hold_s"]))
            self._bg.add(task)
            task.add_done_callback(self._bg.discard)
            return
        self.held.pop(gate.name, None)
        self.stats["visa_released"] += 1
        await self.confirm_release(gate)

    async def _late_release(self, gate, seconds):
        await asyncio.sleep(seconds)
        if gate.name in self.held:                                # FM may have revoked / reset it meanwhile
            del self.held[gate.name]
            self.stats["visa_released"] += 1
            self.stats["chaos_visa_late_release"] += 1
            await self.confirm_release(gate)

    async def confirm_release(self, gate, keep_trying_s=1800.0, every_s=5.0):
        """Send a release; if FM does not accept it (anything but HTTP 200 with granted=true) send it again every 5 s, like the real sherpa
        (mule's policy loop keeps the visa in visas_held until a granted release answer arrives, and resends a release every 5 s while the gate
        is clear). FM sometimes does the release but answers 400 "Unable to obtain resource access response from RQ"; a release is safe to repeat."""
        if await self.visa_release_ok(gate):
            return True
        task = asyncio.ensure_future(self._keep_releasing(gate, keep_trying_s, every_s))
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        return False

    async def visa_release_ok(self, gate):
        return await self.visa("release", gate)                   # True only for HTTP 200 with granted=true; visa() counts the HTTP status

    async def _keep_releasing(self, gate, total_s, every_s):
        end = time.monotonic() + total_s
        while time.monotonic() < end:
            await asyncio.sleep(every_s)
            if gate.name in self.held:                            # asked for it again meanwhile: that visa is a new one
                return
            self.stats["visa_release_retry"] += 1
            if await self.visa_release_ok(gate):
                self.stats["visa_release_recovered"] += 1
                return
        self.stats["visa_release_gave_up"] += 1

    # ── random errors and obstacles (the leg carries on after each) ─────────────────────────────────────────
    async def chaos_tick(self, trip_id, leg_id, dest, name, progress, length):
        """Once per leg tick: maybe raise an obstacle (the sherpa stops for obstacle_s seconds, trip_status carries the real
        'Detected obstacle' stoppage) or a random error from the real catalogue (an incident POSTed to FM; for the tracking
        error the cte/te in trip_status spike). The leg then simply continues."""
        c = self.chaos
        if not c:
            return
        if random.random() < c["obstacle_per_min"] * TRIP_STATUS_PERIOD / 60.0:
            await self._obstacle(trip_id, leg_id, dest, name, progress, length, c["obstacle_s"])
        elif c["errors"] and random.random() < c["error_per_min"] * TRIP_STATUS_PERIOD / 60.0:
            await self._error(trip_id, leg_id, dest, name, progress, length, c)

    async def _obstacle(self, trip_id, leg_id, dest, name, progress, length, seconds):
        dist = round(random.uniform(0.8, 2.5), 2)
        ang = random.uniform(-0.4, 0.4)
        local = [round(dist * math.cos(ang), 2), round(dist * math.sin(ang), 2)]
        text = "Detected obstacle - %s m away at [%s, %s] sensor: LIDAR" % (dist, local[0], local[1])
        self.stats["chaos_obstacles"] += 1
        self.ev("obstacle for %.0f s: %s" % (seconds, text))
        t0 = time.monotonic()
        while time.monotonic() - t0 < seconds:
            held = round(time.monotonic() - t0, 1)
            await self.send_trip_status(trip_id, leg_id, dest, name, progress, length, text, extra={
                "obstacle_speed_factor": 0, "local_obstacle": local, "time_elapsed_stoppages": held, "time_elapsed_obstacle_stoppages": held})
            await asyncio.sleep(TRIP_STATUS_PERIOD)
        self.stats["chaos_obstacle_s"] += time.monotonic() - t0

    async def _error(self, trip_id, leg_id, dest, name, progress, length, c):
        key = random.choice(sorted(c["errors"]))
        e = c["errors"][key]
        code = "1-103-%s%s-%s%s" % (e.get("error_classification_code", "1"), e.get("error_module_code", "01"),
                                    e.get("error_sub_module_code", "03"), e.get("error_description_code", "01"))
        ok, _ = await self.post("/api/v1/sherpa/add_fm_incidents", {
            "type": "mule_error", "code": key, "module": e["module"], "sub_module": e["sub_module"], "message": e["err_msg"],
            "display_message": e["err_disp_msg"], "recovery_message": e["recovery_msg"], "error_code": code,
            "incident_id": "%s_%d_%s" % (self.name, int(time.time()), key), "data_uploaded": False,
            "other_info": {"yelli_pose": [round(v, 3) for v in self.pose]}})
        self.stats["chaos_errors"] += 1
        self.ev("error %s (%s) %s: %s" % (key, code, "sent to FM" if ok else "REFUSED by FM", e["err_disp_msg"]))
        self.stats["chaos_error_%s" % ("sent" if ok else "refused")] += 1
        self.stats["chaos_error_key_%s" % key] += 1
        t0 = time.monotonic()
        while time.monotonic() - t0 < c["error_s"]:           # the vehicle holds briefly, then the trip carries on
            spike = key == "high_cte_te"
            await self.send_trip_status(trip_id, leg_id, dest, name, progress, length, e["err_disp_msg"],
                                        cte=round(random.uniform(0.6, 1.5), 2) if spike else 0.0, te=round(random.uniform(0.2, 0.6), 2) if spike else 0.0,
                                        extra={"time_elapsed_other_stoppages": round(time.monotonic() - t0, 1)})
            await asyncio.sleep(TRIP_STATUS_PERIOD)

    async def report_trip_reached(self, trip_id, trip_leg_id, destination_pose=None, destination_name=None):
        """POST trip/reached. FM requires destination_pose and destination_name; they default to the move_to
        this leg came from."""
        leg = self._legs.get((trip_id, trip_leg_id), {})
        pose = destination_pose if destination_pose is not None else (leg.get("destination_pose") or self.pose)
        name = destination_name if destination_name is not None else leg.get("destination_name", "")
        waited = 0
        while self.offline_until > time.monotonic() or not self.connected:          # not connected to FM: the real sherpa retries every second until it is
            if self.stop_event.is_set() or waited > 600:
                break
            if waited == 0:
                self.ev("reached %s but FM cannot be reached: will tell it when reconnected" % name)
            waited += 1
            await asyncio.sleep(1.0)
        ok, _ = await self.post("/api/v1/sherpa/trip/reached", {
            "timestamp": self.ts("reached"), "trip_id": trip_id, "trip_leg_id": trip_leg_id,
            "destination_pose": pose, "destination_name": name})
        self.stats["reached_ok" if ok else "reached_fail"] += 1
        self.ev("reached %s (trip %s leg %s): %s" % (name, trip_id, trip_leg_id, "reported" if ok else "FM did not accept it"))
        return ok

    async def periodic(self, ws):
        last_mule = 0.0
        while not self.stop_event.is_set() and not ws.closed:
            await self.send_status()
            if time.monotonic() - last_mule >= MULE_PERIOD:
                if not self._mule_empty_sent:
                    await self.send_mule()
                    self._mule_empty_sent = True
                last_mule = time.monotonic()
            await asyncio.sleep(STATUS_PERIOD)

    # ── FM -> sherpa ─────────────────────────────────────────────────────────
    async def ack(self, req_id, response=None, success=True):
        ok, ms = await self.post("/api/v1/sherpa/req_ack/%s" % req_id,
                                 {"success": success, "response": response or {}})
        self.stats["acks_ok" if ok else "acks_fail"] += 1
        self.sample(self.ack_ms, ms)
        self.ack_max = max(self.ack_max, ms)
        self.recent_ack.append(ms)

    send_http_ack = ack                       # name used by the original mock_sherpa.py

    @staticmethod
    async def _call(handler, msg, sherpa):
        try:
            need = [p for p in inspect.signature(handler).parameters.values()
                    if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is p.empty]
        except (TypeError, ValueError):
            need = [None]
        r = handler(msg, sherpa) if len(need) >= 2 else handler(msg)
        return await r if inspect.isawaitable(r) else r

    async def on_request(self, ws, msg):
        ep, req_id = msg.get("endpoint"), msg.get("req_id")
        msg["_rx_wall"], msg["_rx_mono"] = time.time(), time.monotonic()
        self.stats["req_rx"] += 1
        self.stats["req_" + str(ep)] += 1
        extra = ""
        if ep == "move_to":
            extra = " trip %s leg %s -> %s" % (msg.get("trip_id"), msg.get("trip_leg_id"), msg.get("destination_name"))
        elif ep == "reset_pose":
            extra = " to %s" % (msg.get("station_name") or [round(v, 1) for v in (msg.get("pose") or [])])
        elif ep in ("revoke_visa", "reset_visas_held"):
            extra = " %s" % (msg.get("zone_name") or "")
        self.ev("FM request %s%s" % (ep, extra))
        self.requests.append((time.time(), ep, msg))
        if ep == "move_to" and self.on_move_to:
            self.on_move_to(msg.get("trip_id"), self.name, msg["_rx_wall"])
        handler = self._handlers.get(ep) or self._defaults.get(ep)
        resp, success = {}, True
        if not handler:                       # a real sherpa refuses a request type it does not know (and still acks it: FM blocks a worker on an un-acked request)
            success = False
        if handler:
            try:
                r = await self._call(handler, msg, self)
                if isinstance(r, dict):
                    resp = r
                elif r is False:
                    success = False
            except Exception as e:                               # noqa: BLE001
                success = False
                self.stats["handler_errors"] += 1
                self.ev("handler for %s FAILED: %r" % (ep, e))
                log.warning("%s handler for %s failed: %r", self.name, ep, e)
        if msg.get("ack_reqd", True) and req_id:
            await self.ack(req_id, resp, success)

    # default behaviours (replace any of them with on())
    def _h_init(self, msg):
        return {"display_name": self.name, "hwid": self.name, "last_updated": str(time.time()),
                "sw_tag": self.sw_tag, "sw_date": "", "sw_id": ""}

    def _h_reset_pose(self, msg):
        if msg.get("pose"):
            self.pose = list(msg["pose"])
        return {}

    def _h_switch_mode(self, msg):
        if msg.get("mode"):
            self.mode = msg["mode"]
        return {}

    def _h_terminate_trip(self, msg):
        self.cancel_trip()
        return {}

    def _h_pause_resume(self, msg):
        self.paused = bool(msg.get("pause"))
        return {}

    def _h_move_to(self, msg):
        self.cancel_trip()
        self._legs[(msg.get("trip_id"), msg.get("trip_leg_id"))] = {
            "destination_pose": msg.get("destination_pose"), "destination_name": msg.get("destination_name", "")}
        self.trip_task = asyncio.ensure_future(self.run_leg_on_map(msg) if self.map_model else self.run_leg(msg))
        return {}

    def _h_ok(self, msg):
        return {}

    def _h_current_sound_status(self, msg):
        return dict(self.sound)

    def _h_sound(self, msg):
        self.sound = {"volume": msg.get("volume", self.sound["volume"]), "sound_type": msg.get("sound_type", self.sound["sound_type"])}
        return {}

    def _h_update_sherpa_config(self, msg):
        return {"status": "success", "message": "Sherpa config updated and published to FM"}

    def _h_revoke_visa(self, msg):
        """FM took a visa away (manual release): the sherpa no longer holds that gate and must ask again."""
        if self.held.pop(msg.get("zone_name"), None) is not None:
            self.stats["visa_revoked"] += 1
        return {}

    def _h_reset_visas_held(self, msg):
        self.held.clear()
        return {}

    # ── gates: what the real sherpa does (mule ez_policy / visa_utils), against FM's visa decisions ──────────────
    async def visa(self, access_type, gate):
        """POST access/resource. Returns True if FM granted (request) - FM answers {"granted": bool, ...} in the body."""
        st, resp = await self.post_json("/api/v1/sherpa/access/resource", {
            "access_type": access_type, "to_fm": True, "timestamp": self.ts("resource"),
            "visa": {"zone_id": str(gate.id), "zone_name": gate.name, "visa_type": "transit"}})
        self.stats["visa_%s_http_%s" % (access_type, "ok" if st == 200 else st)] += 1
        ok = st == 200 and bool(resp.get("granted"))
        self.ev("gate %s %s -> %s%s" % (gate.name, access_type, ("granted" if ok else "NOT granted") if access_type == "request" else ("released" if ok else "release NOT accepted"),
                                         "" if st == 200 else " (HTTP %s)" % st))
        return ok

    async def run_leg_on_map(self, msg):
        """A leg that follows the lane graph at speed_mps and stops at a gate FM has not granted (another sherpa holds
        it), re-asking every visa_retry_s, then releases the gate once clear of it."""
        trip_id, leg_id = msg["trip_id"], msg["trip_leg_id"]
        dest = [float(v) for v in msg["destination_pose"]]
        name = msg.get("destination_name", "")
        dense = self.map_model.path(self.pose, dest)
        if not dense or len(dense) < 2:
            self.stats["path_fallback"] += 1                      # no lane path: straight line, no gates
            self.ev("no lane path to %s: driving a straight line" % name)
            return await self.run_leg(msg)
        spans = self.map_model.zones_on(dense)
        total = float(len(dense) - 1)
        per_tick = max(1.0, self.speed_mps * TRIP_STATUS_PERIOD / STEP)
        length = total * STEP
        pos, last_req, waited = 0.0, {}, False
        try:
            while pos < total:
                t_tick = time.monotonic()
                await asyncio.sleep(TRIP_STATUS_PERIOD)
                if self.paused:
                    await self.send_trip_status(trip_id, leg_id, dest, name, pos / total, length, "paused")
                    continue
                nxt = min(total, pos + per_tick)
                blocked = None
                for z in spans:                                   # a gate we are about to enter (or are in) and do not hold
                    if z.gate.name in self.held or nxt < z.enter or pos > z.exit:
                        continue
                    now = time.monotonic()
                    if z.gate.name not in last_req or now - last_req[z.gate.name] >= self.visa_retry_s:
                        last_req[z.gate.name] = now
                        self.stats["visa_requests"] += 1
                        if await self.visa("request", z.gate):
                            self.held[z.gate.name] = "transit"
                            self.stats["visa_granted"] += 1
                            continue
                        self.stats["visa_denied"] += 1
                    blocked = z
                    break
                if blocked is None:
                    await self.chaos_tick(trip_id, leg_id, dest, name, pos / total, length)
                if blocked is not None:                           # hold position until FM grants the gate
                    if not waited:
                        self.ev("waiting at gate %s (FM has not granted it)" % blocked.gate.name)
                    waited = True
                    await self.send_trip_status(trip_id, leg_id, dest, name, pos / total, length, "Waiting for visa")
                    self.stats["visa_wait_s"] += time.monotonic() - t_tick      # measured: includes the visa HTTP call, which can take seconds
                    continue
                pos = nxt
                i = int(pos)
                x, y, th = dense[i]
                self.pose = [x, y, th]
                for z in spans:                                   # clear of a gate we hold -> release it
                    if z.gate.name in self.held and pos > z.exit and pos < total:
                        await self.release_gate(z.gate)
                if pos < total:
                    await self.send_trip_status(trip_id, leg_id, dest, name, pos / total, length)
                    self.battery = max(1.0, self.battery - 0.01)
            self.pose = dest
            for z in spans:                                       # parked inside a gate: keep its visa until we drive out
                if z.gate.name in self.held and z.exit < total:
                    await self.release_gate(z.gate)
            if await self.report_trip_reached(trip_id, leg_id, dest, name):
                self.sample(self.leg_ms, (time.monotonic() - msg["_rx_mono"]) * 1000)
        except asyncio.CancelledError:
            self.stats["legs_cancelled"] += 1
        except Exception as e:                                   # noqa: BLE001
            self.stats["leg_errors"] += 1
            log.debug("%s map leg error %r", self.name, e)

    def _h_peripherals(self, msg):
        asyncio.ensure_future(self.respond_peripherals(msg))
        return {}

    async def respond_peripherals(self, msg):
        """After `reached`, FM sends a peripherals request (dispatch button / hitch / lift / conveyor) and holds the
        trip in `waiting_station` until the sherpa answers with its own peripherals message (POST
        /api/v1/sherpa/peripherals) -- what the real sherpa and FM's fleet_simulator do. Without it every trip stalls."""
        await asyncio.sleep(self.station_seconds)
        body = {"timestamp": self.ts("peripherals")}
        if msg.get("dispatch_button"):
            body["dispatch_button"] = {"value": True}
        if msg.get("auto_hitch"):
            body["auto_hitch"] = {"hitch": bool(msg["auto_hitch"].get("hitch"))}
        if msg.get("lifter_actuator"):
            body["lifter_actuator"] = {"lift": bool(msg["lifter_actuator"].get("lift"))}
        if msg.get("conveyor"):
            cv = msg["conveyor"]
            body["conveyor"] = {"direction": cv.get("direction"), "num_units": cv.get("num_units", 1)}
        if msg.get("platform_operation"):
            body["platform_operation"] = msg["platform_operation"]
        if len(body) == 1:                    # speaker / indicator only: nothing to confirm
            return
        ok, _ = await self.post("/api/v1/sherpa/peripherals", body)
        self.stats["peripherals_ok" if ok else "peripherals_fail"] += 1

    def cancel_trip(self):
        if self.trip_task and not self.trip_task.done():
            self.trip_task.cancel()
        self.trip_task = None

    async def run_leg(self, msg):
        trip_id, leg_id = msg["trip_id"], msg["trip_leg_id"]
        dest = [float(v) for v in msg["destination_pose"]]
        name = msg.get("destination_name", "")
        start = list(self.pose)
        length = math.hypot(dest[0] - start[0], dest[1] - start[1]) or 1.0
        steps = max(1, int(self.leg_seconds / TRIP_STATUS_PERIOD))
        i = 1
        try:
            while i < steps:
                await asyncio.sleep(TRIP_STATUS_PERIOD)   # first status one period after move_to: FM drops same-type msgs < 500 ms apart
                if self.paused:                           # hold position, keep reporting (FM watches trip_status)
                    await self.send_trip_status(trip_id, leg_id, dest, name, (i - 1) / float(steps), length, "paused")
                    continue
                await self.chaos_tick(trip_id, leg_id, dest, name, (i - 1) / float(steps), length)
                frac = i / float(steps)
                self.pose = [start[k] + (dest[k] - start[k]) * frac for k in range(3)]
                await self.send_trip_status(trip_id, leg_id, dest, name, frac, length)
                self.battery = max(1.0, self.battery - 0.01)
                i += 1
            self.pose = dest
            if await self.report_trip_reached(trip_id, leg_id, dest, name):
                self.sample(self.leg_ms, (time.monotonic() - msg["_rx_mono"]) * 1000)
        except asyncio.CancelledError:
            self.stats["legs_cancelled"] += 1
        except Exception as e:                                   # noqa: BLE001
            self.stats["leg_errors"] += 1
            log.debug("%s leg error %r", self.name, e)

    async def reader(self, ws):
        async for m in ws:
            if m.type == aiohttp.WSMsgType.TEXT:
                try:
                    msg = json.loads(m.data)
                except ValueError:
                    self.stats["bad_json"] += 1
                    continue
                if isinstance(msg, dict) and msg.get("endpoint"):
                    asyncio.ensure_future(self.on_request(ws, msg))
                else:
                    self.stats["other_rx"] += 1
            elif m.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    # ── connection lifecycle ─────────────────────────────────────────────────
    async def run(self, start_delay):
        try:
            await asyncio.wait_for(self.stop_event.wait(), start_delay)
            return
        except asyncio.TimeoutError:
            pass
        last_connect = 0.0
        while not self.stop_event.is_set():
            gap = MIN_RECONNECT_GAP - (time.monotonic() - last_connect)
            if gap > 0 and last_connect:                  # FM closes sherpas that connect > 4x per 60 s
                try:
                    await asyncio.wait_for(self.stop_event.wait(), gap)
                    return
                except asyncio.TimeoutError:
                    pass
            last_connect = time.monotonic()
            try:
                if self.handshake_on:
                    await self.handshake([(l, p % self.sw_tag if "%s" in p else p) for l, p in self.PRE_CONNECT])
                async with self.session.ws_connect(self.ws_url, headers=self.hdr, heartbeat=30, ssl=self._ssl) as ws:
                    self.stats["ws_connects"] += 1
                    self.ws, self.connected = ws, True
                    self.ev("connected to FM (websocket)")
                    self._mule_empty_sent = False
                    if self.handshake_on:
                        await self.handshake(self.POST_CONNECT)
                    tasks = [asyncio.ensure_future(self.periodic(ws)), asyncio.ensure_future(self.reader(ws)),
                             asyncio.ensure_future(self.stop_event.wait())]
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for t in tasks:
                        t.cancel()
                    code = ws.close_code
                    self.ev("websocket closed (code %s)" % code)
                    if code is not None:
                        self.close_codes[str(code)] += 1
            except Exception as e:                               # noqa: BLE001
                self.stats["ws_errors"] += 1
                self.ev("websocket error: %r" % (e,))
                log.debug("%s ws error %r", self.name, e)
            finally:
                self.connected, self.ws = False, None          # the vehicle keeps driving its leg while it cannot reach FM (the real sherpa does)
            while self.offline_until > time.monotonic() and not self.stop_event.is_set():
                await asyncio.sleep(0.5)                           # a simulated network outage: no reconnect until it is over
            if not self.stop_event.is_set():
                self.stats["ws_reconnects"] += 1

    def summary(self):
        d = dict(self.stats)
        d["close_codes"] = dict(self.close_codes)
        d["ack_ms_p50"], d["ack_ms_p95"], d["ack_ms_max"] = pct(self.ack_ms, 50), pct(self.ack_ms, 95), self.ack_max
        d["leg_ms_p50"], d["leg_ms_p95"] = pct(self.leg_ms, 50), pct(self.leg_ms, 95)
        return d


def load_sherpas(args):
    if args.sherpas:
        with open(args.sherpas) as f:
            data = json.load(f)
        items = data.get("sherpas", data) if isinstance(data, dict) else data
        return [(s["name"], s.get("api_key", s["name"]), s.get("map_zip")) for s in items]     # map_zip: this sherpa's own map
    return [("%s-%03d" % (args.prefix, i + 1), "%s-%03d" % (args.prefix, i + 1), None) for i in range(args.count)]


def aggregate(sherpas):
    tot = Counter()
    acks, legs, recent, ack_max = [], [], [], 0.0
    codes = Counter()
    for s in sherpas:
        recent += list(s.recent_ack)
        tot.update(s.stats)
        codes.update(s.close_codes)
        acks += s.ack_ms
        ack_max = max(ack_max, s.ack_max)
        legs += s.leg_ms
    return {"sherpas": len(sherpas), "connected_now": sum(1 for s in sherpas if s.connected),
            "totals": dict(tot), "close_codes": dict(codes),
            "ack_ms": {"p50": pct(acks, 50), "p95": pct(acks, 95), "p99": pct(acks, 99), "max": ack_max, "n": len(acks)},
            "leg_ms": {"p50": pct(legs, 50), "p95": pct(legs, 95), "n": len(legs)},
            "recent_ack_ms": {"p50": pct(recent, 50), "p95": pct(recent, 95), "n": len(recent)}}


async def main_async(args, on_move_to=None, stop_event=None, on_snapshot=None, out_sherpas=None):
    entries = load_sherpas(args)
    if not entries:
        sys.exit("no sherpas to run")
    base = "%s:%d" % (args.host, args.port)
    conn = aiohttp.TCPConnector(ssl=False, limit=0)
    timeout = aiohttp.ClientTimeout(total=30)
    started = time.time()
    models = {}                                                  # map zip -> MapModel (shared by every sherpa on that map)
    for z in {e[2] or getattr(args, "map_zip", None) for e in entries} - {None}:
        try:
            models[z] = MapModel.from_zip(z)
            log.info("realistic motion: %d stations, %d gates from %s", len(models[z].stations), len(models[z].gates), z)
        except Exception as e:                                   # noqa: BLE001
            log.warning("could not load %s for realistic motion (%r): straight-line legs, no gates", z, e)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        sherpas = [MockSherpa(n, k, base, session, start_pose=[10.0 + 2 * i, 10.0, 0.0],
                              leg_seconds=args.leg_seconds, battery=90.0, sw_tag=args.sw_tag, mode=args.mode, on_move_to=on_move_to,
                              station_seconds=getattr(args, 'station_seconds', 2.0),
                              map_model=models.get(mz or getattr(args, "map_zip", None)),
                              speed_mps=getattr(args, "speed", 1.5),
                              client_ip=("10.250.%d.%d" % (i // 250, i % 250 + 1)) if getattr(args, "distinct_ips", False) else None,
                              chaos=chaos_from_args(args))
                   for i, (n, k, mz) in enumerate(entries)]
        if out_sherpas is not None:                                  # the caller (a test) may want to reach the sherpas: close a websocket, read counters
            out_sherpas.extend(sherpas)
        loop = asyncio.get_event_loop()
        stop = stop_event or asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, ValueError, RuntimeError):   # not the main thread / not supported
                pass
        step = args.ramp / float(len(sherpas)) if args.ramp else 0.0
        log.info("starting %d mock sherpas -> %s (ramp %.0fs, duration %s)", len(sherpas), base, args.ramp,
                 "until stopped" if not args.duration else "%ds" % args.duration)
        runners = [asyncio.ensure_future(s.run(i * step)) for i, s in enumerate(sherpas)]

        async def watch_stop():
            await stop.wait()
            for s in sherpas:
                s.stop()
        watcher = asyncio.ensure_future(watch_stop())
        deadline = time.monotonic() + args.duration if args.duration else None
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), args.report_every)
                break
            except asyncio.TimeoutError:
                pass
            snap = aggregate(sherpas)
            t = snap["totals"]
            if on_snapshot:
                on_snapshot(snap)
            log.info("connected %d/%d | status %d trip_status %d | req_rx %d acks ok/fail %d/%d | ack p95 %s ms | reached %d | closes %s",
                     snap["connected_now"], snap["sherpas"], t.get("status_sent", 0), t.get("trip_status_sent", 0),
                     t.get("req_rx", 0), t.get("acks_ok", 0), t.get("acks_fail", 0),
                     "-" if snap["ack_ms"]["p95"] is None else "%.0f" % snap["ack_ms"]["p95"],
                     t.get("reached_ok", 0), snap["close_codes"] or "{}")
            if deadline and time.monotonic() >= deadline:
                stop.set()
        for s in sherpas:
            s.stop()
        await asyncio.gather(*runners, return_exceptions=True)
        watcher.cancel()
    result = aggregate(sherpas)
    result.update({"host": args.host, "started": started, "ended": time.time(),
                   "per_sherpa": {s.name: s.summary() for s in sherpas}})
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        log.info("results written to %s", args.out)
    return result


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Mock sherpas speaking the real FM protocol (load testing)")
    p.add_argument("--host", required=True, help="FM host/IP (nginx)")
    p.add_argument("--port", type=int, default=443, help="443 => wss/https, anything else => ws/http")
    p.add_argument("--sherpas", help="JSON file: [{name, api_key}] (written by provision.py)")
    p.add_argument("--count", type=int, default=5, help="without --sherpas: generate perf-sherpa-001..N (api_key = name)")
    p.add_argument("--prefix", default="perf-sherpa")
    p.add_argument("--ramp", type=float, default=30.0, help="seconds over which to stagger the connects")
    p.add_argument("--duration", type=int, default=0, help="seconds to run, 0 = until SIGINT/SIGTERM")
    p.add_argument("--leg-seconds", type=float, default=20.0, help="simulated time per trip leg")
    p.add_argument("--station-seconds", type=float, default=2.0, help="time at a station before answering a peripherals request")
    p.add_argument("--map-zip", help="map zip: follow its lane graph and wait at its gates (visas) like a real sherpa")
    p.add_argument("--speed", type=float, default=1.5, help="m/s along the lanes when --map-zip is used")
    p.add_argument("--distinct-ips", action="store_true", help="send a per-sherpa X-Forwarded-For so FM's per-IP rate limits apply per sherpa, like real vehicles")
    p.add_argument("--errors-per-min", type=float, default=0.0, help="per sherpa, random errors from the real error catalogue (sent to FM as incidents; the trip carries on)")
    p.add_argument("--obstacles-per-min", type=float, default=0.0, help="per sherpa, random obstacle detections (the sherpa stops for --obstacle-seconds, then carries on)")
    p.add_argument("--obstacle-seconds", type=float, default=5.0, help="how long a detected obstacle holds the sherpa")
    p.add_argument("--visa-leak-pct", type=float, default=0.0, help="percent of gate releases a sherpa forgets (it keeps the gate, others wait)")
    p.add_argument("--visa-hold-seconds", type=float, default=60.0, help="how long a forgotten gate is held before the sherpa finally releases it")
    p.add_argument("--sw-tag", default="mock-sherpa")
    p.add_argument("--mode", default="fleet", choices=["fleet", "simulation"])
    p.add_argument("--report-every", type=float, default=10.0)
    p.add_argument("--out", help="write a JSON summary here at the end")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    try:
        asyncio.get_event_loop().run_until_complete(main_async(a))
    except KeyboardInterrupt:
        pass
