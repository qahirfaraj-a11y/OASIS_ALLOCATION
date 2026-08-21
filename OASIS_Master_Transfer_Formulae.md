# OASIS — Master Transfer Formulae

> **THE SPECIFICATION.** What the engine does, as maths. Authoritative — where
> any other document disagrees, this one wins.
> Evidence: `OASIS_Transfer_Methodology_Deep_Dive_2026-08.md` ·
> Measurement: `OASIS_Transfer_Methodology_Position_2026-08.md`


**The single authoritative specification of the transfer engine.**
Engine: `oasis/logic/consolidated_transfer_service.py` +
`oasis/logic/fulfillment_decider.py` at `a5bcba25`.
Supersedes every earlier statement of these rules.

The engine does **two jobs and only two**:

> **J1 — plug gaps.** A store will run out before replenishment lands; move
> stock from somewhere that can spare it.
> **J2 — clear dead stock.** Capital is frozen at a node that does not sell the
> line; move it to a node that does.

Every rule below exists to serve J1 or J2. Anything that serves neither is not
a transfer.

---

## 1. Inputs

### 1.1 Per store-SKU, from the ERP

$$
q_{s,i}\ \text{on-hand}, \quad
d_{s,i}\ \text{ADS over 90 days}, \quad
p_{s,i}\ \text{price}, \quad
c_{s,i}\ \text{cost}, \quad
a_{s,i}\ \text{days since last receipt}
$$

### 1.2 Per supplier, from **LATA** (`supplier_patterns_2025.json`, GRN history)

$$
g_v\ \text{median gap between deliveries},\quad
\ell_v\ \text{lead time},\quad
m_v \in [0.8,\,3.0]\ \text{variance multiplier}
$$

### 1.3 Per category, from **AMIT** (`engines.dead_stock`)

$$
\tau_k\ \text{perishability threshold} \quad (76\ \text{tiers; default }45)
$$

### 1.4 Constants — the only four

$$
\rho = 0.5\ \text{(release fraction)}, \quad
\kappa = 500\ \text{KES (transfer cost)}, \quad
\Theta = 90\ \text{days (dead silence)}, \quad
R_{\max} = 45\ \text{days}
$$

Everything else is derived. If a fifth constant appears, it is a defect.

> **There was a fifth for a long time: $\sigma$, the safety floor.** It sat
> inline as a literal `14` at both places that compute excess, while every
> store record carried an unread `safety_days` — so it never appeared in this
> list to be challenged.
>
> **It is now derived, and is no longer a constant.** $\sigma_{s,i} = R_{s,i}$:
> the safety floor *is* the relief horizon, from the same LATA rhythm and under
> the same AMIT shelf-life ceiling. Measured over 46,830 store-SKUs on the
> depot: **147 distinct values**, min 2.2d, median 17.2d, max 45d, against the
> single 14 it replaces. The floor now tracks the goods — bread 3.5d, fresh
> milk 3.7d, footwear 40.7d.
>
> `default_safety_days` (14) survives only as the last resort for a line whose
> supplier is unknown to LATA, to the delivery calendar *and* to the network
> median. An explicit per-store policy from a client ERP still overrides both.

---

## 2. Primitives

**Days of cover**
$$
\theta_{s,i} = \frac{q_{s,i}}{d_{s,i}}, \qquad \theta_{s,i} \equiv 999 \ \text{ when } d_{s,i}=0
$$

**Unit margin**
$$
\mu_{s,i} =
\begin{cases}
p_{s,i} - c_{s,i} & 0 < c_{s,i} < p_{s,i}\\
0.25\,p_{s,i} & \text{otherwise} \quad (\text{estimated; counted and reported})
\end{cases}
$$

**Category threshold**
$$
T_k = \tau_k \ \text{ if } k \in \text{AMIT}, \ \text{ else } 45
$$

**Relief horizon — the master horizon.** Days until replenishment actually lands:

$$
\boxed{\;
R_{s,i} =
\operatorname{clip}\!\Big(
g_{v(i)} + \ell_{v(i)}\, m_{v(i)},\;
1,\;
\min\big(T_{k(i)},\, R_{\max}\big)
\Big)
\;}
$$

with the fallback, for a supplier LATA has never seen,

$$
R_{s,i} = \operatorname{clip}\Big(\operatorname*{median}_{u \in \mathcal{L}} \big(g_u + \ell_u m_u\big),\ 1,\ \min(T_{k(i)}, R_{\max})\Big)
$$

Three properties, each deliberate:

- **the multiplier applies to the lead, not the cadence** — $g_v$ is an
  observed fact, only $\ell_v$ is uncertain. $(g+\ell)m$ puts the median at
  45.7 d and pins half the book against the ceiling; $g + \ell m$ gives 23.0 d.
- **the ceiling is shelf life** — never hold more cover than the goods survive.
- **an unknown supplier resembles the book**, not a constant.

---

## 3. Committed flows

Open transfers (`REQUESTED` / `IN_TRANSIT`) are real stock, so they adjust both
views before anything is computed:

$$
q^{\text{don}}_{s,i} = \max\big(0,\ q_{s,i} - \mathrm{out}_{s,i}\big),
\qquad
q^{\text{rec}}_{s,i} = q^{\text{don}}_{s,i} + \mathrm{in}_{s,i}
$$

$q^{\text{don}}$ is used wherever $s$ acts as a **donor**; $q^{\text{rec}}$
wherever $s$ acts as a **recipient**. Without this the scan re-recommends stock
already on a lorry.

---

## 4. Classification

**Deficit** (J1 recipient):
$$
\mathrm{def}(s,i) \iff
\big(d>0 \wedge (\theta^{\text{rec}}_{s,i} < R_{s,i} \ \vee\ q^{\text{rec}}_{s,i} \le \mathrm{ROP}_{s,i})\big)
\ \vee\ (d=0 \wedge q^{\text{rec}} < 1)
\ \vee\ i \in \mathrm{MOQfail}_s
$$

**Excess** — units above the safety floor, with an overstock gate and a buffer:
$$
E_{s,i} =
\begin{cases}
q^{\text{don}} - \sigma_s d & d>0 \ \wedge\ \theta^{\text{don}} > \gamma_i \ \wedge\ q^{\text{don}} - \sigma_s d > 7d\\
q^{\text{don}} & d = 0 \wedge q^{\text{don}} > 0\\
0 & \text{otherwise}
\end{cases}
\qquad
\gamma_i = \begin{cases}14 & \text{fresh}\\ 30 & \text{dry}\end{cases}
$$

> $\sigma_{s,i}$ is the **safety floor in days**, and it is $R_{s,i}$ — the
> relief horizon itself:
> $$\sigma_{s,i} = R_{s,i}$$
>
> **Why the floor and the fill target are one quantity.** A recipient is filled
> *to* $R$. Protecting a donor to a shallower depth let the engine drain a
> store to 14 days of cover in order to lift another to 23, so the donor became
> next week's deficit and the same units came back the other way. You must not
> take a store below the depth you would fill it to; $\sigma$ and the target
> are the two ends of one lorry.
>
> It is per store-SKU, not per store, because a supplier's cadence belongs to
> the **line**, not the building — the same outlet needs a fortnight of cover
> on a fortnightly line and two days on a daily one. **Store size enters
> through $d$, not through $\sigma$**: a forecourt and a 22,500 sqft anchor
> both hold 3.7 days of milk, which is a different number of *units* because
> their ADS differs. The pipeline audit's M4 framed this as the two being
> "protected identically"; they are protected to the same depth, which is
> correct, and to different volumes, which is also correct.
>
> Remaining gap: `_relief_days` keys on **supplier only**, so a site that is
> genuinely served on a different cadence from its siblings is not yet
> distinguished. That needs GRN history per store, not per supplier.

**Dead** (J2 donor) — zero demand, sustained, and worth a lorry:
$$
\mathrm{dead}(s,i) \iff d_{s,i}=0 \ \wedge\ q_{s,i}>0 \ \wedge\ a_{s,i} \ge \Theta \ \wedge\ q_{s,i}c_{s,i} \ge \kappa
$$

**Overstock** (J2 donor) — still selling, but past what the category survives:
$$
\mathrm{over}(s,i) \iff d_{s,i}>0 \ \wedge\ \theta^{\text{don}}_{s,i} > T_{k(i)}
$$

> The age clause in $\mathrm{dead}$ is load-bearing: $d=0$ alone also describes
> a line ranged last week that has not had its first sale. The capital clause
> is the transfer cost, not AMIT's floor — AMIT asks whether trapped capital
> deserves attention, a transfer asks whether moving it beats the van.

---

## 5. The donor ledger — the global invariant

One book $B_{s,i}$ of units already promised from $s$ for $i$, shared by every
pass and every entry point, keyed on the **donor's canonical item code**.

$$
\boxed{\;\forall s,i:\quad \sum_{\text{all passes}} y_{s\to\cdot,i} \;=\; B_{s,i} \;\le\; E_{s,i}\;}
$$

Three claimants draw on it (`decide()`, PULL, PUSH). Before it existed they
tracked takings privately and one donor holding 600 units promised 1,568.

---

## 6. Donor selection

**Eligibility.** With $r_{s,i} = 1.5$ if $d>5$; $2.5$ if $d \le 1$; else $2.0$:
$$
E_{s,i} - B_{s,i} > 0
\quad\wedge\quad
q^{\text{don}}_{s,i} - B_{s,i} \ \ge\ \sigma_s\,d_{s,i}\,r_{s,i}
$$

**Ranking.**
$$
\sigma_{s \to t,\,i} =
\frac{E_{s,i} - B_{s,i}}{\Delta_{s,t} + 0.1}
\;\cdot\; 3^{\,\mathbb{1}[s \in \mathrm{Hubs}]}
\;\cdot\; 2^{\,\mathbb{1}[a_{s,i} > 45 \,\wedge\, d_{s,i}/\max(1,q_{s,i}) < 0.05]}
$$

Total order: $(-\sigma_{s\to t,i},\ \mathrm{org\_cd}(s))$. The tiebreak is not
cosmetic — without it equally-scored donors keep dict order and 0.5% of volume
moves when the store order is reversed.

**Releasable pool.**
$$
P_{s,i} =
\begin{cases}
E_{s,i} - B_{s,i} & \mathrm{dead}(s,i) \quad \text{— no demand} \Rightarrow \text{safety } \sigma_s d = 0 \Rightarrow \text{nothing to protect}\\[4pt]
\rho\,E_{s,i} - B_{s,i} & \text{otherwise}
\end{cases}
$$

**The pool is a hard bound, not a target.** Any quantity drawn from it is
floored to a shippable unit before it ships:
$$
\mathrm{ship}(x, P) = \min\big(\lceil x \rceil,\ \lfloor P \rfloor\big)
\quad\text{(EA)},
\qquad
\min\big(\mathrm{round}_1(x),\ \lfloor 10P \rfloor / 10\big)
\quad\text{(KG)}
$$

> Ceiling is correct for a recipient's *need* — 3.2 units of a boxed item ships
> as 4 — and wrong for a donor's *pool*. Applying it to the pool let a
> releasable 0.44 ship a whole unit, so $\rho$ was advisory rather than binding
> on exactly the small lines that make up most of the plan: **1,321 of 1,725
> donor/SKU pairs were over the cap**. PUSH breached the same way one unit
> higher, its $P<1$ gate catching only the sub-unit case.

---

## 7. J1 — PULL

**Need.** For every deficit line:
$$
x_{s,i} = \max\big(0,\ d_{s,i}R_{s,i} - q^{\text{rec}}_{s,i},\ \mathrm{moq}_{s,i}\big)
$$

A transfer restores cover **to the relief horizon and no further**. It plugs a
gap; it is not a replenishment.

**Priority.** Margin at risk if the line goes unserved:
$$
w_{s,i} = \max\big(x_{s,i}\,\mu_{s,i},\ 0.01\,x_{s,i}\big)
$$

**Allocation.** Collect *every* deficit in the network first; then each
contended donor splits its pool proportionally to need:

$$
\boxed{\;
y_t = \Big\lceil\, \min\Big(x_t,\ \ P_{s,i}\cdot\frac{w_t}{\textstyle\sum_{u \in \mathcal{R}} w_u}\Big) \Big\rceil_{k}
\;}
$$

over $\mathcal{R}$ = recipients contending for donor $s$ and item $i$, for
$\mathrm{MAXROUNDS}=3$ rounds (a recipient whose preferred donor is exhausted
falls back to its next best), followed by a remainder sweep in descending $w$
so rounding does not strand usable stock. After each award
$B_{s,i} \mathrel{+}= y_t$ and $x_t \mathrel{-}= y_t$.

**Rounding.**
$$
\lceil z \rceil_k =
\begin{cases}
\mathrm{round}(z, 1) & k \in \mathrm{KG}\ \text{(meat, cheese, deli, produce…)}\\
\lceil z \rceil & \text{otherwise}
\end{cases}
$$

---

## 8. J2 — PUSH

**Donors:** $\{s : \mathrm{dead}(s,i) \vee \mathrm{over}(s,i)\}$ with
$P_{s,i} \ge 1$ and $P_{s,i}\,c_{s,i} \ge \kappa$.

**Recipients:** every **active** store, $d_{t,i} > 0$ — *not* only short ones.
J2 relocates capital to where it sells; requiring a gap is a J1 criterion.

**Priority.** Cash released per day of shelf life consumed:
$$
w^{\text{push}}_t = \max\big(d_{t,i}\,\mu_{t,i},\ 0.01\big)
$$

**Absorption** — the most $t$ can trade out before the line dies *there*:
$$
\boxed{\;
\mathrm{abs}_t =
\begin{cases}
d_{t,i}\,T_{k(i)} - q^{\text{rec}}_{t,i} & \text{from a } \mathrm{dead}\ \text{donor}\\[4pt]
d_{t,i}\,R_{t,i} - q^{\text{rec}}_{t,i} & \text{from an } \mathrm{over}\ \text{donor}
\end{cases}
\;}
$$

Asymmetric on purpose. From a dead donor the alternative is that the stock stays
dead, so fill as deep as the category survives. From an overstock donor the
alternative is that a store still selling it keeps it, so fill only to relief —
more would merely relocate the surplus.

**Allocation:** the same proportional split as §7, over $w^{\text{push}}$ and
$\mathrm{abs}$, drawing on the same $B$.

---

## 9. Guards

$$
\text{fresh}(i) \Rightarrow \texttt{manual\_only} = \text{True}
$$

Perishables are surfaced for a human to dispatch and **never auto-queued**:
transit shortens shelf life, and each store orders fresh to its own
sell-through.

$$
s \ne t \quad \text{(no self-transfer)}
\qquad
\mathrm{company}(s) = \mathrm{company}(t) \quad \text{(ERP will not confirm otherwise)}
$$

Caps `max_pull_per_store` and `max_push` default to **0 = uncapped**: a
truncated list is worse than a short one because it looks complete. When set,
ranking happens **before** truncation, so what is dropped is the least valuable
rather than the last read.

---

## 10. Invariants — what must hold

**I1 — Order independence.**
$$
\forall \pi \in \mathrm{Sym}(S):\quad Y(\pi(S)) = Y(S)
$$
Proportional allocation depends only on the multiset $\{(x_t, w_t)\}$, and the
donor list is totally ordered. *Verified: 0.00% divergence, reversed and
sorted.*

**I2 — Ledger conservation.**
$$
\sum_{t} y_{s \to t,\,i} = B_{s,i} \quad \text{for every claimant, every pass}
$$

**I3 — Donor solvency.**
$$
B_{s,i} \le E_{s,i} \le q_{s,i}
$$
A donor never promises more than it has spare, nor more than it holds.

**I4 — Horizon sanity.**
$$
1 \le R_{s,i} \le \min\big(T_{k(i)},\, R_{\max}\big)
$$
Never hold, nor ship, more cover than the goods survive.

**I5 — Determinism.** All hashing is md5 over stable keys; no reliance on
`hash()`, which is salted per process.

---

## 11. Degraded mode

When LATA or AMIT data is absent the engine still runs, on strictly weaker
terms. This is a **configuration difference, not a code difference** — and it
is what the Command Center runs today:

| | full | degraded |
|---|---|---|
| $R$ | $g_v + \ell_v m_v$ | $n_v + \ell_v$ (calendar), else 7 / 14 |
| $T_k$ | 76 tiers | 45 for every category |
| unknown supplier | book median (23.0 d) | constant |
| median $R$ | 16.0 d | 7.0 d |
| supplier coverage | 599 | 288 |

**Only $R$ and $T$ differ.** Six of fourteen stages carry them; the rest are
bit-identical between the two configurations:

| carries $R$ | carries $T$ | carries neither |
|---|---|---|
| trigger, shortfall $x$, weight $w$, PULL split, overstock absorption | overstock gate, dead absorption, relief ceiling | excess $E$, eligibility, **ranking $\sigma$**, pool $P$, dead test, PUSH pool |

Because **$\sigma$ carries no configuration**, the two agree on *which store to
take from* 90–100% of the time. They differ on which lines exist at all, and on
how large each one is.

Measured consequence at 14 stores: **4,001 PULL lines / 19,546 units** versus
**3,437 / 26,300** — fewer genuine gaps found, each over-filled — and
**5,450 perishable units versus 12,518**, because $\mathrm{abs}_t$ for bakery
uses $45$ instead of $\tau_k = 5$, a 9× overstatement.

The engine logs a warning naming `lata_shield` when it starts degraded.

---

## 12. Out of scope — the ordering path

`decide()` is **not** a configuration of the above and **not a transfer
engine**. It is the ORDERING decision: given a shortfall, is it met by a
supplier order, a transfer, or both? A transfer is one branch of its answer,
not its subject.

$$
y = \min\Big(\rho\big(E_{\text{best}} - B\big),\ \min\big(\mathrm{gap}\cdot d,\ x\big)\Big),
\qquad \mathrm{gap} = \min(\ell,\ \mathrm{ETA}) - \theta
$$

Greedy over one donor rather than proportional over all; horizon is the raw
lead $\ell$ rather than $R$; no concept of dead stock, because clearing it is
not an ordering question.

**The one contract it must honour** is the ledger: it draws on the same
$B_{s,i}$, so it cannot promise units the transfer passes have already
promised. That invariant is tested (`TestCrossPathLedger`). Everything else
about it belongs to the ordering workstream.
