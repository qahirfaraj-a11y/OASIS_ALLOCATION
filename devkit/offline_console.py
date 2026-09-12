"""Launch a Command Center that is genuinely offline, on a spare port.

WHY THIS EXISTS RATHER THAN JUST RUNNING THE .BAT
    The .bat launchers are the operator's entry point and use the real port
    (8501). Verifying a UI change means starting and stopping a console
    repeatedly, which would fight whatever the operator already has open. This
    runs the same script against the same snapshot on 8607.

WHAT "OFFLINE" REQUIRES, WHICH IS MORE THAN ONE VARIABLE
    OASIS_DB_PATH redirects OASIS's OWN store (users, audit, config, PO and
    transfer queues). It does NOT choose the POS *source* -- OASIS_POS_DB_URL
    does, and a machine-level live-POS URL will happily survive into the
    console and send every product and organisation read to MSSQL. With no
    network that dies at module scope, in load_orgs(), before a single tab
    draws:

        [08001] ... localhost,1433 ... Login timeout expired (258)

    So both are cleared here, exactly as the .bat launchers now clear them.

USAGE
    python devkit/offline_console.py [--multi] [--port N] [--no-live]

    --multi    the 5-store network DB instead of the single store; this is
               where transfer decisions have donors to choose between
    --no-live  drop OASIS_LIVE_MODE, replacing the auto-accruing clock with
               the time-of-day slider. Only affects the clock: the auto-refresh
               that used to starve the ordering pipeline defaults off now, so
               LIVE is testable as shipped and this flag is rarely needed.
"""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SINGLE_DB = os.path.join(ROOT, "oasis", "data", "rhapta_pos.db")
MULTI_DB = os.path.join(ROOT, "oasis", "data", "rhapta_multi_store.db")


def main(argv) -> int:
    db = MULTI_DB if "--multi" in argv else SINGLE_DB
    port = argv[argv.index("--port") + 1] if "--port" in argv else "8607"
    live = "--no-live" not in argv

    if not os.path.exists(db):
        print(f"missing store: {db}\n"
              f"build it first: entrypoint.py --mode "
              f"{'build-multi-store-db' if db == MULTI_DB else 'build-pos-db'}")
        return 1

    env = dict(os.environ)
    env["OASIS_DB_PATH"] = db
    env["OASIS_LIVE_MODE"] = "true" if live else "false"
    # The whole point. See the module docstring.
    env.pop("OASIS_POS_DB_URL", None)
    env.pop("OASIS_DB_URL", None)

    print(f"store : {os.path.basename(db)}")
    print(f"live  : {env['OASIS_LIVE_MODE']}")
    print(f"pos   : cleared (reads come from the snapshot, not a server)")
    print(f"url   : http://127.0.0.1:{port}")

    return subprocess.call([
        sys.executable, "-m", "streamlit", "run", "ops_dashboard.py",
        "--server.port", str(port),
        "--server.address", "127.0.0.1",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
    ], cwd=ROOT, env=env)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
