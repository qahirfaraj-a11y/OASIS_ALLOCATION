#!/usr/bin/env bash
# Cold-start install proof — LOCAL variant (no Docker required).
#
# Same 8 verification steps as the Docker script, but runs in a temp directory
# with a fresh venv on the system Python — proving the ZIP + install path work
# without any of the dev venv's cached wheels or environment variables. Safe
# to run repeatedly; the temp dir is deleted at the end.
set -euo pipefail

ZIP=$(ls dist/OASIS_v*.zip 2>/dev/null | tail -1)
[[ -z "${ZIP:-}" ]] && { echo "no release ZIP under dist/"; exit 1; }

# System python (NOT the project venv) — must not be OASIS_* polluted
export -n OASIS_DB_PATH OASIS_LICENSE_KEY OASIS_LICENSE_SALT OASIS_LIVE_MODE 2>/dev/null || true
unset OASIS_DB_PATH OASIS_LICENSE_KEY OASIS_LICENSE_SALT OASIS_LIVE_MODE 2>/dev/null || true

WORKDIR=$(mktemp -d)
LOG="$PWD/cold_start_proof_local_$(date -u +%Y%m%dT%H%M%SZ).log"
trap 'rm -rf "$WORKDIR"; echo; echo "log: $LOG"' EXIT
exec > >(tee "$LOG") 2>&1
{
echo "=== OASIS Cold-Start Install Proof (LOCAL) ==="
echo "ZIP:     $ZIP  ($(du -h "$ZIP" | cut -f1))"
echo "workdir: $WORKDIR"
echo "python:  $(which python) — $(python --version 2>&1)"
echo "started: $(date -u +%FT%TZ)"
echo

ZIP_ABS=$(cd "$(dirname "$ZIP")" && pwd)/$(basename "$ZIP")
# On MSYS/Git-Bash convert /c/... -> C:/... for the Windows-native Python
if command -v cygpath >/dev/null 2>&1; then
    ZIP_ABS=$(cygpath -m "$ZIP_ABS")
    WORKDIR_WIN=$(cygpath -m "$WORKDIR")
else
    WORKDIR_WIN=$WORKDIR
fi
cd "$WORKDIR"

echo "[1/8] Extract release ZIP"
python -c "import zipfile; zipfile.ZipFile(r'$ZIP_ABS').extractall(r'$WORKDIR_WIN')"
DIR=$(ls -d OASIS_v*/ | head -1)
cd "$DIR"
echo "  extracted to $WORKDIR/$DIR ($(find . -type f | wc -l) files)"
echo

echo "[2/8] Verify install.bat + VERSION + entrypoint.py present"
for f in install.bat VERSION entrypoint.py requirements.txt; do
    [[ -f "$f" ]] || { echo "  MISSING: $f"; exit 2; }
    echo "  OK: $f ($(wc -c < "$f") bytes)"
done
echo "  VERSION: $(cat VERSION)"
echo

echo "[3/8] Create fresh Python environment"
python -m venv .venv
if [[ -f .venv/Scripts/python.exe ]]; then
    PY=.venv/Scripts/python.exe
else
    PY=.venv/bin/python
fi
$PY -m pip install --upgrade pip -q
echo "  venv: $($PY -c 'import sys; print(sys.prefix)')"
echo

echo "[4/8] Install requirements.txt (this is where most cold-starts fail)"
$PY -m pip install -r requirements.txt -q 2>&1 | tail -5 || {
    echo "  FAIL: pip install returned $?"
    $PY -m pip install -r requirements.txt 2>&1 | tail -30
    exit 4
}
echo "  installed $($PY -m pip list --format=freeze | wc -l) packages"
echo

echo "[5/8] Import smoke: every entry-point loads clean"
$PY -c "
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
$PY entrypoint.py --mode version
echo

echo "[7/8] --mode license-status (should show evaluation trial — no key yet)"
$PY entrypoint.py --mode license-status
echo

echo "[8/8] --mode preflight"
# What this step proves is that preflight RUNS, not that it is happy. A fresh
# unpack has no POS connected, so a FAIL verdict here is correct and expected
# — onboarding is what fixes it.
#
# The old guard was `|| echo "(preflight can WARN...)"`, which swallowed every
# non-zero exit. That could not distinguish the expected FAIL verdict from
# "the command never ran at all", and for an unknown period it hid the latter:
# `preflight` was missing from --mode choices, so argparse exited 2 and this
# proof still printed PASS.
#
# So: require the REPORT, not the exit code. If preflight cannot execute there
# is no "OVERALL:" line, and that is a real failure of the release.
PREFLIGHT_OUT=$($PY entrypoint.py --mode preflight 2>&1) || true
echo "$PREFLIGHT_OUT"
if ! grep -q "OVERALL:" <<<"$PREFLIGHT_OUT"; then
    echo
    echo "  preflight did not produce a report — it could not run at all."
    echo "=== cold-start proof: FAIL (preflight did not execute) ==="
    exit 1
fi
echo "  (preflight ran. A FAIL verdict pre-onboarding is expected: no POS is"
echo "   connected on a fresh unpack.)"

echo
echo "=== cold-start proof: PASS ==="
} 2>&1 | tee "$OLDPWD/$LOG"

echo
echo "log: $LOG"
