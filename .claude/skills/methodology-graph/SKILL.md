---
name: methodology-graph
description: Read and write the OASIS methodology graph — claims, parameters, decision surfaces, probes, evidence. Use when adding or amending a claim, checking what depends on a parameter, propagating staleness after a code change, or answering "is this allowed to drive money".
---

# The methodology graph

A deterministic DAG over OASIS's own methodology. **Not a neural graph** — no
learned weights, no embeddings. OASIS already has a GNN whose risk head turned out
to be a static attribute prior trained on synthetic labels; the corrective for that
is auditability, not a second learned structure.

## Node types

| Type | Directory | What it is |
|---|---|---|
| `claim` | `Claims/` | a falsifiable statement about the methodology or the world |
| `parameter` | `Parameters/` | a number the engine actually reads |
| `decision` | `Surfaces/` | an output that costs or earns money |
| `probe` | `Probes/` | an executable that returns a verdict on claims |
| `trap` | `Traps/` | a known failure mode with a mechanical detector |

`Evidence/` holds append-only dated runs. `_graph/methodology.json` is derived.

`oasis_vault/Decisions/` is the existing **narrative** record — untouched. Decision
nodes live in `Surfaces/` so the two never collide.

## Edges and what they mean for staleness

```
claim ──supports──▸ parameter ──feeds──▸ decision
  │
  └──(reverse depends_on)──▸ dependent claims
```

`contradicts` is deliberately **not** a staleness edge. Two claims in tension is a
state worth holding and reporting, not resolving by propagation.

## Firebreaks

A parameter with `neutralised: true` is pinned to a no-op value. Staleness marks it
and stops. `param.gnn-ordering-weight` is the case that matters:
`OASIS_GNN_ORDERING_WEIGHT = 0`, the GNN claims are falsified, and no purchase
order is in question. Propagating past it would cry wolf about the whole ordering
surface and teach everyone to ignore the alarm.

A decision surface with `money: false` is display-only and is not gated —
`surface.store-risk-display` shows the blended GNN prior honestly and moves nothing.

## Writing a node

```markdown
---
id: claim.ordering.sigma-L-missing
type: claim
status: measured
domain: ordering
title: sigma_L belongs in the safety term
worth: 2.22d spread on a 2.29d mean
ttl_days: 90
last_evidence: 2026-08-25
supports: [param.sigma_L]
tested_by: [probe.term-attribution]
guarded_by: [trap.denominator-sanity]
---

The argument, for humans. Frontmatter is the machine-readable skeleton.
```

Frontmatter dialect is deliberately small: scalars, inline lists, block lists.
If a node needs more than that, the node is doing too much.

## Commands

```bash
python devkit/methodology/cli.py build            # rebuild + integrity + gate
python devkit/methodology/cli.py status           # claims by status
python devkit/methodology/cli.py blast <id>       # what falls if this is wrong
python devkit/methodology/cli.py stale [--apply]  # propagate staleness
python devkit/methodology/cli.py frontier         # what to interrogate next
python devkit/methodology/seed.py [--force]       # re-seed (idempotent)
```

## Rules

- **Never set a status by hand.** Status is derived from evidence via the harness.
- **Never hand-edit `_graph/methodology.json`.** It is rebuilt by `build`.
- **Stdlib only.** A loop that fails to import is a loop that silently stops running.
- A duplicate node id is refused loudly — it is the vault form of the
  stale-duplicate loader bug.
