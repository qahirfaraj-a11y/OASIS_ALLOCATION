---
id: probe.comparable-store-correlation
type: probe
status: measured
domain: siting
title: Comparable-store correlation
entrypoint: devkit/probe_comparable_stores.py
guards: []
tests: [claim.siting.estate-has-a-usable-revenue-label, claim.siting.size-exponent-unfitted, claim.siting.distance-decay-unfitted, claim.siting.catchment-km-unfitted, claim.siting.ml-artefacts-unvalidated]
---

**Entrypoint:** `devkit/probe_comparable_stores.py`

Scores every store in the existing estate at its own location with **itself
removed from the competitive field** — leave-one-out, because a store that
competes with itself is not a comparable — and correlates predicted capture
against realised performance, sweeping `alpha × beta × catchment`.

This is the only held-out test siting can have.

**It interrogates the label before it fits anything.** A correlation is worth no
more than the number it correlates against, and the way the GNN went wrong was
training on labels generated from its own inputs. So the probe first asks
whether the label sits on a fixed ladder, whether it is a function of the
model's own features, and whether the available labels agree with each other.
Reporting "no defensible label" is a real result.

It also re-checks whether `beta` is identified at all, or whether the fit simply
runs monotone to the edge of the swept range — which `score_band`'s own
docstring already says it does.
