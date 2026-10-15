# mock_sherpa

A programmable mock sherpa that speaks the real sherpa <-> Fleet Manager protocol (WebSocket status + HTTP acks,
`reached`, `peripherals`, gate visas) without the mule stack. One engine, used by load tests, UI/API automation and
resilience tests.

## Use it as a submodule

```
git submodule add <this repo's url> third_party/mock_sherpa
```
Then either `pip install -e third_party/mock_sherpa`, or put `third_party/mock_sherpa` on `sys.path` and
`import mock_sherpa`. Only dependency: `aiohttp`.

## API

```python
from mock_sherpa import MockSherpa, MapModel          # async engine, see mock_sherpa/__init__.py for the full guide
from mock_sherpa.runner import start_mock_sherpas     # sync wrapper: own event loop on a daemon thread (for pytest)

runner = start_mock_sherpas([{"name": "s1", "api_key": "s1"}], "fm-host:443", map_zip="map.zip", handshake=False)
runner.start(); runner.wait_all_connected()
sh = runner.sherpas[0]
sh.on("diagnostics", lambda msg: {"cpu": 12})        # script any FM request
runner.run(sh.power_cut(30))                          # unclean power-off: leg dies, off 30 s, reboots
sh.requests_for("move_to", since=t)                   # what FM re-sent afterwards
runner.stop()
```

Scenario primitives on the sherpa: `go_offline(s)` (network outage, leg continues), `power_cut(s)` (leg lost),
`set_error()/clear_error()`, `update_pose/battery/mode`, `chaos=` (random errors/obstacles), `requests`/`requests_for()`.
CLI load generator: `python -m mock_sherpa.engine --help`.

## Tests
`PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python3 -m pytest tests` (the flag avoids broken pytest plugins in some ROS setups).

## History
Merged from `validation-asset-control/perf` (engine, gates, facade; newer: handshake, go_offline, sound/config handlers,
nack unknown requests) and `fleet-manager-automation/test_provisioning` (runner; its older engine copy was a strict
subset and is superseded). Behaviour kept from the perf engine: leg keeps driving while disconnected, `error: true`,
unknown FM requests are nacked.

## Contributing
Keep it protocol-faithful: read behaviour from FM's `sherpa_ws.py`/`sherpa_http.py` and mule's `fleet_bridge_utils.py`
before adding a handler, and say in the docstring what is approximated. Add a test in `tests/` for every new primitive.
