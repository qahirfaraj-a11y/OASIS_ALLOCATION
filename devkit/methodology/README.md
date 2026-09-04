# The methodology loops

Turns the interrogation of OASIS's ordering and site-selection methodology from
episodic prose into a standing machine. Design: `../../OASIS_Methodology_Loop_Architecture.md`.

## Quick start

```bash
python devkit/methodology/cli.py build       # rebuild the graph, integrity + gate report
python devkit/methodology/cli.py status      # every claim, by status
python devkit/methodology/cli.py frontier    # what to interrogate next, ranked
python devkit/methodology/cli.py blast <id>  # what falls if this node is wrong
python devkit/methodology/cli.py stale       # dry run: what has outlived its evidence
python devkit/methodology/cli.py traps --scan --path oasis
python devkit/methodology/cli.py probe probe.trap-scan
python devkit/methodology/seed.py            # idempotent re-seed
```

From Claude Code: `/interrogate`, `/probe`, `/graph`, `/gate`, `/attribute`.

## What is here

| File | What it does |
|---|---|
| `vault.py` | node files ↔ objects; the status lattice; a frontmatter parser in eighty lines |
| `graph.py` | the DAG, blast radius, staleness propagation, firebreaks, the gate |
| `traps.py` | the seven detectors, each for a failure this repo already paid for |
| `harness.py` | probe verdict schema, append-only evidence, status transitions, the four gate checks |
| `cli.py` | the commands above |
| `seed.py` | the ~21 claims already established in the repo's own documents |

`../../oasis/logic/decision_ledger.py` is the other half: every PO, transfer and
site recommendation written with the inputs it saw and the parameters in force.
Loop B is impossible without it.

## Stdlib only

No PyYAML, no NetworkX, no pandas. A loop that fails to import is a loop that
silently stops running, and this has to work under the project's Windows venv,
the Linux dev VM, and CI with no install step.

## The rule the whole thing enforces

Nothing drives money until it has beaten a baseline on data it has not seen.
`OASIS_GNN_ORDERING_WEIGHT = 0` is that rule, already enforced, for one model.
This generalises it to every parameter — and marks as a **firebreak** any
parameter pinned to a no-op value, so a falsified claim beneath it does not cry
wolf about the surface above it.
