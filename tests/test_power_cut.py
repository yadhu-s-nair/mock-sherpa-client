"""power_cut(): the leg dies with the vehicle, the sherpa leaves FM and comes back, and FM's re-sent leg is recorded.

Runs against a tiny in-process fake FM (aiohttp): a websocket that sends one move_to when the sherpa connects and
again on every reconnect, plus the HTTP acks the sherpa posts back.
"""
import asyncio
import time

import aiohttp
from aiohttp import web

from mock_sherpa import MockSherpa


async def _fake_fm(sent):
    async def ws_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        sent.append("connect")
        await ws.send_json({"endpoint": "move_to", "req_id": "r%d" % len(sent), "ack_reqd": True, "trip_id": 1,
                            "trip_leg_id": 7, "destination_name": "Drop_02", "destination_pose": [20.0, 10.0, 0.0],
                            "final_destination": True, "task": "UNLIFT"})
        async for _ in ws:
            pass
        return ws

    async def ok(request):
        return web.json_response({"status": "ok"})

    app = web.Application()
    app.router.add_get("/ws/api/v1/sherpa/", ws_handler)
    app.router.add_post("/api/v1/sherpa/{tail:.*}", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, site._server.sockets[0].getsockname()[1]


async def _until(cond, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        await asyncio.sleep(0.05)
    return False


def test_power_cut_kills_leg_and_fm_resend_is_recorded():
    async def scenario():
        sent = []
        runner, port = await _fake_fm(sent)
        async with aiohttp.ClientSession() as session:
            sherpa = MockSherpa("pc-01", "pc-01", "127.0.0.1:%d" % port, session, tls=False, leg_seconds=30.0)
            task = asyncio.create_task(sherpa.start())
            try:
                assert await _until(lambda: sherpa.requests_for("move_to")), "first move_to never arrived"
                assert sherpa.trip_task is not None and not sherpa.trip_task.done()
                cut_at = time.time()
                await sherpa.power_cut(off_seconds=1.0)
                assert sherpa.trip_task is None, "the leg must die with the vehicle"
                assert not sherpa.connected
                assert await _until(lambda: sherpa.requests_for("move_to", since=cut_at), timeout=20.0), \
                    "FM's re-sent leg never arrived after the reboot"
                resent = sherpa.requests_for("move_to", since=cut_at)[0][1]
                assert resent["trip_leg_id"] == 7
                assert len(sent) >= 2
            finally:
                sherpa.stop()
                task.cancel()
        await runner.cleanup()

    asyncio.run(scenario())


def test_runner_passes_options_and_runs_power_cut():
    """The runner (sync tests) forwards MockSherpa options and can drive the async primitives."""
    from mock_sherpa.runner import start_mock_sherpas

    sent = []
    loop = asyncio.new_event_loop()
    fm, port = loop.run_until_complete(_fake_fm(sent))
    import threading
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    runner = start_mock_sherpas([{"name": "rn-01", "api_key": "rn-01"}], "127.0.0.1:%d" % port, tls=False,
                                leg_seconds=30.0, handshake=False)
    try:
        runner.start()
        assert runner.wait_all_connected(15)
        sh = runner.sherpas[0]
        assert sh.handshake_on is False
        cut_at = time.time()
        runner.run(sh.power_cut(1.0))
        end = time.monotonic() + 20
        while time.monotonic() < end and not sh.requests_for("move_to", since=cut_at):
            time.sleep(0.1)
        assert sh.requests_for("move_to", since=cut_at)
    finally:
        runner.stop()
        loop.call_soon_threadsafe(loop.stop)
