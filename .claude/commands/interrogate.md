---
description: Run one adversarial critique cycle (Loop A) against a claim or parameter
argument-hint: "[node id, or blank to take the frontier's top row]"
allowed-tools: Read, Grep, Glob, Bash, Task, Write, Edit
---

Run one **Loop A** cycle. Target: `$1`

## 1 · SELECT

If `$1` is empty, take the top row of:

```bash
python devkit/methodology/cli.py frontier
```

The frontier ranks by `(money touched) × (staleness) × (unexamined-ness)`, so a
parameter feeding live purchase orders whose claim is `asserted` and unprobed
outranks a well-measured sweep constant.

Then establish the target's position before attacking it:

```bash
python devkit/methodology/cli.py blast <node>
```

State the target, its status, what depends on it, and whether any of that is money.

## 2 · ATTACK

Launch all three adversaries **in parallel, in one message**, each with the target
node, its body, and its blast radius:

- `methodology-challenger` — the derivation
- `data-adversary` — the inputs, and the seven-trap register
- `ops-realist` — the consequence

They are separate contexts on purpose. Do not summarise one to another.

## 3 · TRIAGE

Deduplicate the returned objections against the ledger:

- Already `falsified`? Close it with a pointer to the evidence.
- Names no executable probe? It is an **opinion**. Log it as one; it does not
  enter the graph.
- New and falsifiable? It becomes a `claim` at `asserted` with a required probe.

## 4 · PROBE

Run every probe the surviving objections name:

```bash
python devkit/methodology/cli.py probe <probe id>
```

If the probe does not exist yet, write it under `devkit/`, make it emit one JSON
verdict object per line, and guard it with the traps it needs. A probe that judges
nothing is not a probe.

## 5 · RULE

Hand the verdicts to `evidence-scribe`, which records them, moves status, and
propagates staleness. Then:

```bash
python devkit/methodology/cli.py build
```

## Report

```
## Cycle <date> — <target>

TARGET      node, status, blast radius, money at stake
OBJECTIONS  n raised · n survived triage · n probed
VERDICTS    claim · probe · verdict · status before → after
FALLEN      anything now in question, with its blast radius
OPINIONS    objections that named no probe
NEXT        the frontier's top row after this cycle
```

A cycle that produces no falsifiable objection is a **valid outcome**. Record it —
that is how a claim earns its way toward `trusted`.
