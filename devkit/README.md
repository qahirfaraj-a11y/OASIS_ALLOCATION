# OASIS Dev Toolkit

Tooling that is **outside the operational scope of a client install**.

Nothing in this directory ships. The release packager uses a strict whitelist
(`oasis/logic/release_packager.py`): only the named root files, the whitelisted
`oasis/` sub-packages and `migrations/` go into a client zip, so `devkit/` is
excluded structurally rather than by a rule someone has to remember to update.
`tests/test_release_zip.py` asserts this.

## Why this directory exists

These scripts used to sit in the repo root next to `app.py` and
`ops_dashboard.py`, which made two things impossible to tell apart:

1. **What a client actually receives** vs. what only we run. `--mode simulation`
   looked like a product feature but shelled out to a script that was never in
   the zip, so on a client install it just exited 1.
2. **Dead code vs. dev-only code.** `oasis/simulation/simulation_engine.py`
   reads as unreferenced if you only look at what ships — it has three importers,
   and they are all in here.

## Contents

| Script | What it is |
|---|---|
| `run_simulation_scenario.py` | Scenario runner (black-swan events, supplier failure, competitor entry). Backs `entrypoint.py --mode simulation`. |
| `run_all_tiers_simulation.py` | Runs the scenario across every store-size tier. |
| `run_batch_sims.py` | Batch driver over `run_simulation_scenario.py`. |
| `run_scaled_simulation_tiers.py` | Scaled tier sweep; imports `run_simulation`. |
| `run_supplier_failure_scenario.py` | Supplier-failure scenario driver. |
| `shadow_monitor.py` | Shadow-audit daemon. Backs `--mode shadow`. |
| `approval_dashboard.py` | Legacy standalone approval console (Streamlit). |
| `generate_showcase_scenario.py` | Seeds the demo narrative. Backs `--mode showcase`. |
| `production_diagnostic.py` | Pre-flight diagnostics. |
| `modes.bat` | The dev menu. The client menu is `OASIS.bat`. |
| `chain_siting.py` | Best next sites for ONE named chain, using no revenue at all — for competitors and for prospects, where sales data does not and will not exist. Ranks on net-new people and sizes against the chain's own people-per-sqft habit. Produces no capital figure, deliberately. |
| `pov_sweep.py` | Every chain's view of the same city. Capture is identical for all of them at a point; only cannibalisation is theirs. |
| `build_market_pack.py` | Rebuilds the shipped national competitor matrix from Overpass. The PACK ships (ODbL, with its notice); this script does not. Read its header before publishing a new one. |
| `retail_sandbox/` | Browser sandbox + backtest harness for the replenishment and transfer engines, built on the real SKU/supplier data. Playable model of *The Algorithmic Retailer*. See its own README. |

## Running these

Run them from the repo root:

```bash
python devkit/run_simulation_scenario.py --scenario Baseline --days 30
```

Every script here resolves the repo root itself via

```python
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

so `import oasis...` works from `devkit/`. If you add a script, use that exact
line — `tests/test_devkit.py` checks for it, because the failure mode
(`ModuleNotFoundError: oasis`) only shows up when a human runs the script.

## The modules these keep alive

Two modules live under `oasis/` but are reachable **only** from here, so the
packager excludes them by name (`_OASIS_DEV_ONLY`):

- `oasis/simulation/simulation_engine.py` — the scenario engine. The simulation
  surface that *does* ship is `oasis/simulation/retail_simulator.py`, behind the
  Command Center's Simulation Lab.
- `oasis/logic/simulation_pipeline.py` — used by `shadow_monitor.py` and
  `approval_dashboard.py`. Despite the class name, unrelated to the above.

Delete either one and you break this toolkit, not the product.
