---
description: Inspect the methodology graph — status, blast radius, gate report, frontier
argument-hint: "[node id, or blank for the whole picture]"
allowed-tools: Read, Bash
---

Inspect the methodology graph. Node: `$1`

With no argument, give the whole picture:

```bash
python devkit/methodology/cli.py build       # integrity + gate report
python devkit/methodology/cli.py status      # claims by status
python devkit/methodology/cli.py frontier    # what to interrogate next
python devkit/methodology/cli.py stale       # dry run: what would fall
```

With a node id:

```bash
python devkit/methodology/cli.py blast $1
```

Then read the node's own file under `oasis_vault/` and its evidence series under
`oasis_vault/Evidence/$1/`.

## How to read the report

- **GATE** — a parameter feeds a live decision but rests on claims that have not
  earned the right to drive money. This is the state of the system, not a bug list.
- **INFO · gated to a no-op value** — the parameter is pinned so the claims beneath
  it cannot reach money. `OASIS_GNN_ORDERING_WEIGHT = 0` is the case that matters:
  the GNN claims are falsified, the weight is zero, and the fall stops there.
- **WARN · evidence stale** — a measurement outlived its TTL. Not wrong; unrefreshed.
- **WARN · asserted claim with downstream dependents** — something is being relied
  on that was never measured. The four unfitted siting constants live here.

Report what is true, what is merely asserted, and where the money is exposed.
Do not soften the gate report.
