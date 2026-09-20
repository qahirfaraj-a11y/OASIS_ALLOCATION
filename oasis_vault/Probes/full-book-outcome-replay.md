---
id: probe.full-book-outcome-replay
type: probe
status: measured
domain: ordering
title: Full-book outcome replay — does the extra stock pay?
entrypoint: devkit/full_book_replay.py
guards: [trap.circularity, trap.like-for-like, trap.category-error]
tests: [claim.ordering.statistical-target-beats-the-heuristic]
---

**Entrypoint:** `devkit/full_book_replay.py`

`probe.rop-variants` scores a SIZING: at an empty shelf, does the cover ordered
reach the protection interval, and how far past it. That cannot settle whether
the extra cover PAYS, because safety stock only earns its keep under variance,
and a one-shot score has none.

This replays the whole book day by day. Both arms make every order through the
shipped four-stage path (`prepare_sku_data` → `calculate_order_quantity` →
`finalize_orders` → `apply_minimum_order_gate`) via the sandbox's
`engine_bridge`, on the real closing position, with the supplier calendar
deciding order days. The arms differ only in `OASIS_ORDER_MODEL`
(`classic` | `order_up_to`). Scored on what each EARNS: realised gross profit,
less expiry at cost, less holding on the average stock carried (15/25/35% a
year).

**Guards.**
* *Circularity (T5)* — the world's shelf lives may not be the engine's own:
  order-up-to plans against those lives, so a world built from them rewards it
  by construction. `WORLD_LIFE` is an independent physical table; `--world
  engine` runs the circular variant deliberately, for comparison.
* *Like-for-like (T4)* — common random numbers: both arms see the same demand
  path and the same lead-time draw, on the same lines and days.
* *Category error (T7)* — the metric is declared `replenishment`.

**What is assumed, and swept.** Demand variability `phi` is not observed for
this book (no per-line daily sales exist in the repo), so it is swept
0.2 / 0.4 / 0.8 and a verdict that flips across it is reported as resting on
it. Only fresh and frozen lines carry a life: the operator's rule is that
everything else is 90+ days, negligible in an FMCG shop. `--life-file` takes
the store's expiry book once it exists and replaces the asserted table.

`held_out` is **false**: the demand is simulated from observed average rates,
not replayed from observed daily sales. That is the ceiling on what this probe
can claim, and it is why the bread shelf — where real deliveries and real
sales exist — remains the stronger evidence for anything bread-shaped.
