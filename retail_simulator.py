"""Compatibility shim — the simulator now lives in ``oasis.simulation``.

It sat at the repo root, so it was excluded from every client release by the
default-deny whitelist. Two SHIPPED scripts import it: ``ops_dashboard.py``
(the Simulation Lab tab) and ``intraday_sim.py`` (at module level, so that
script was entirely broken on a client install). Moving the implementation into
``oasis/simulation/`` makes it shippable; this shim keeps the eleven existing
``from retail_simulator import ...`` call sites — including the Streamlit
console, which is the untouched reference — working unchanged.

New code should import from ``oasis.simulation.retail_simulator`` directly.
"""

from oasis.simulation.retail_simulator import *          # noqa: F401,F403
from oasis.simulation.retail_simulator import (           # noqa: F401
    DATA_DIR,
    SCORECARD_FILE,
    STORE_UNIVERSES,
    DailyLog,
    OrderEngine,
    RetailSimulator,
    SKUState,
    SimulationResult,
    SimulationOrderUtil,
    calculate_demand_cv,
    export_to_excel,
    load_sales_forecasting,
    load_scorecard_data,
    load_supplier_patterns,
    main,
    print_simulation_summary,
    save_feedback,
)

if __name__ == "__main__":
    main()
