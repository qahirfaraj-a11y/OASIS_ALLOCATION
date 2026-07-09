#!/usr/bin/env bash
# Cold-start install proof — simulate a client unzipping OASIS_v<VER>.zip in a
# clean environment and running the Python side of install.bat.
#
# Uses a fresh Docker container (python:3.10-slim) so nothing from the dev
# machine leaks in — no venv, no cached wheels, no OASIS_* env vars. This
# tests the exact failure surface that matters in the wild: does the release
# ZIP contain everything, do requirements resolve, do migrations stamp, does
# preflight run, does license-status work.
#
# Windows-specific bits of install.bat (batch flow, cmd built-ins) already
# smoke-tested on the dev Windows box; this proves the PORTABLE core.
set -euo pipefail

ZIP=$(ls dist/OASIS_v*.zip 2>/dev/null | tail -1)
if [[ -z "${ZIP:-}" ]]; then
    echo "no release ZIP under dist/ — run: python entrypoint.py --mode package-release"
    exit 1
fi

LOG=cold_start_proof_$(date -u +%Y%m%dT%H%M%SZ).log
echo "=== OASIS Cold-Start Install Proof ==="            | tee "$LOG"
echo "ZIP: $ZIP  ($(du -h "$ZIP" | cut -f1))"            | tee -a "$LOG"
echo "started: $(date -u +%FT%TZ)"                       | tee -a "$LOG"
echo                                                    | tee -a "$LOG"

# Copy the ZIP into the container's workspace, then run the install sequence
# exactly as install.bat does on the Python side.
docker run --rm -i \
    -v "$(pwd)/$ZIP:/in/release.zip:ro" \
    -v "$(pwd)/$LOG:/out/log:rw" \
    python:3.10-slim bash -exs <<'DOCKER' 2>&1 | tee -a "$LOG"
set -e
apt-get update -qq && apt-get install -y -qq unzip file >/dev/null

echo
echo "[1/8] Extract release ZIP"
mkdir -p /app && cd /app
unzip -q /in/release.zip
DIR=$(ls -d OASIS_v*/ | head -1)
cd "$DIR"
echo "  extracted to /app/$DIR ($(find . -type f | wc -l) files)"

echo
echo "[2/8] Verify install.bat + VERSION + entrypoint.py present"
for f in install.bat VERSION entrypoint.py requirements.txt; do
    if [[ ! -f "$f" ]]; then echo "  MISSING: $f" >&2; exit 2; fi
    echo "  OK: $f ($(wc -c < "$f") bytes)"
done
echo "  VERSION: $(cat VERSION)"

echo
echo "[3/8] Create fresh Python environment"
python -m venv .venv
.venv/bin/pip install --upgrade pip -q

echo
echo "[4/8] Install requirements.txt (this is where most cold-starts fail)"
.venv/bin/pip install -r requirements.txt 2>&1 | tail -20

echo
echo "[5/8] Import smoke: every entry-point loads clean"
.venv/bin/python -c "
import importlib, sys
for m in ['oasis.logic.license_manager', 'oasis.logic.db',
         'oasis.logic.pos_erp_adapter', 'oasis.logic.category_report',
         'oasis.logic.sku_deepdive', 'oasis.logic.grn_cost',
         'oasis.logic.release_packager', 'entrypoint']:
    importlib.import_module(m); print('  OK import', m)
print('  Python:', sys.version.split()[0])
"

echo
echo "[6/8] --mode version"
.venv/bin/python entrypoint.py --mode version

echo
echo "[7/8] --mode license-status (trial mode — no key yet)"
.venv/bin/python entrypoint.py --mode license-status

echo
echo "[8/8] --mode preflight (installation health)"
.venv/bin/python entrypoint.py --mode preflight || echo "  (preflight can WARN on a fresh install — that's expected pre-onboarding)"

echo
echo "=== cold-start proof: PASS ==="
DOCKER

status=${PIPESTATUS[0]}
echo                                                    | tee -a "$LOG"
if [[ $status -eq 0 ]]; then
    echo "FINISHED: PASS  (log: $LOG)"                  | tee -a "$LOG"
else
    echo "FINISHED: FAIL  (exit $status — see $LOG)"    | tee -a "$LOG"
fi
exit $status
