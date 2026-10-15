# mock_sherpa: project rules
Shared mock sherpa, consumed as a git submodule by several repos (validation-asset-control/perf, Automated_Testing/fleet-manager-automation, ...).
- Never import from a consumer repo; the package depends on aiohttp only.
- Backwards compatible: new behaviour is opt-in (constructor option / new method) unless it fixes a protocol-fidelity bug; note it in README "History".
- Engine = `mock_sherpa/engine.py`, public facade = `mock_sherpa/__init__.py`, sync adapter = `mock_sherpa/runner.py`, map/gates = `mock_sherpa/gates.py`.
- Every new primitive gets a test in `tests/` (fake FM pattern in `tests/test_power_cut.py`).
