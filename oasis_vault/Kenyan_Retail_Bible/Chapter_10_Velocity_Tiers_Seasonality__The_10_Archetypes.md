---
type: Chapter
chapter: 10
title: "Chapter 10: Velocity Tiers, Seasonality & The 10 Archetypes"
---
# Chapter 10: Velocity Tiers, Seasonality & The 10 Archetypes

To the absolute beginner, forecasting inventory is an exercise in historical averages. If a beginner sold 300 bales of SOKO FLOUR last month, they order 300 bales for next month. When the flour runs out on the 28th, they blame the supplier. When the flour sits untouched on the 15th, they blame the economy.
To the algorithmic operator, the "30-day average" is the most dangerous lie in retail.
Products possess a genetic code. They react violently to external stimuli. If you try to manage a store using flat averages, your working capital will be out of sync with consumer demand. The expert operator discards the average and instead codes every SKU into strict Velocity Tiers and Behavioral Archetypes.
10.1 The Flaw of the Average (Dynamic Velocity)
In the Kenyan matrix, capital flows in pulses, not streams. We established the "Payday Pulse" in Chapter 1. Now, we must apply the mathematics of that pulse to individual SKUs.
The Scenario: You sell 300 units of a premium 2L cooking oil in a 30-day month.
The Beginner Math (Flat Velocity): The beginner calculates $300 / 30 = 10$ units per day. They set their Reorder Point (ROP) based on an assumption that 10 people will buy oil every single day.
The Financial Reality: Sales do not distribute evenly. In reality, you sold 5 units a day from the 10th to the 24th (The Famine), and you sold 50 units a day from the 25th to the 3rd (The Payday Feast).
If the beginner uses the flat average of 10/day during Payday, they will stock out in exactly four hours.
If they use the flat average of 10/day during the Famine, they will over-order by 100%, trapping their cash in dead stock for two weeks.
The OASIS engine never uses a flat average. It dynamically recalculates the velocity tier of every SKU based on its proximity to the 25th of the month.
10.2 The Mathematics of the Reorder Point
To automate procurement, you must calculate exactly when a SKU needs to trigger a new Purchase Order. This is the Reorder Point (ROP).
The standard logistical formula is:
$$ROP = (V_{avg} \times LT) + SS$$
(Where $V_{avg}$ is Average Daily Velocity, $LT$ is Supplier Lead Time in days, and $SS$ is Safety Stock).
However, the algorithmic operator injects a Micro-Seasonality Multiplier ($M$) into this formula to account for the Payday Pulse.
$$ROP = (V_{avg} \times M \times LT) + SS$$
If the date is the 26th, the multiplier $M$ for a bulk staple like 10kg flour might be 4.5. The system automatically quadruples the reorder threshold. If the date is the 14th, the multiplier $M$ drops to 0.5, intentionally starving the backroom to free up cash.
10.3 Macro vs. Micro Seasonality
Beyond the monthly pulse, the expert maps SKUs to external environmental triggers.
Macro-Seasonality (Calendar Events): Back-to-School (January/May/September) creates massive, predictable spikes in [[STATIONERY]], [[UHT MILK CARTONS]], and [[BREAD]]. December creates a 300% volume spike in [[BAKING FLOUR]] and [[COOKING FAT]] (The Chapati Index).
Micro-Seasonality (Weather Triggers): A sudden, unseasonal three-day cold front in Nairobi will instantly crash the velocity of [[CARBONATED SOFT DRINKS]] by 40%, while simultaneously spiking [[PACKAGED TEA]], [[DRINKING CHOCOLATE]], and [[MANDAZI FLOUR]]. If your auto-replenishment engine does not ingest basic weather data, you will be caught holding the wrong inventory.
10.4 The Core Archetypes (The Genetic Code of the Shelf)
Once you strip away the Cannibals and map the seasonal multipliers, the purified network clusters into strict operational Archetypes. Every single item in your store must be assigned one of these codes.
The Traffic Engine (The Black Hole):
Profile: Negative or zero margin. Extreme velocity. Flawless micro-seasonality (sells every day, regardless of the date).
Examples: BROOKSIDE 500ML, SUPER LOAF BREAD.
Rule: Never stock out. Ever.
The Profit Stabilizer (The Margin Sanctuary):
Profile: 15% to 25% margin. Steady velocity. Highly responsive to the Payday Pulse multiplier.
Examples: KERINGET WATER, PREMIUM RICE.
Rule: Expand their facings during the Famine to capture mid-month top-up cash.
The Keystone Catalyst (The Web Weaver):
Profile: Low absolute price, massive Link Density. Triggers larger basket purchases.
Examples: 16 KES SWEETS, MATCHBOXES.
Rule: Place exclusively at high-friction points (checkouts) to execute impulse extraction.
The Payday Anchor:
Profile: Heavy, bulky, high absolute cost. Zero sales mid-month, explosive sales end-of-month.
Examples: 10KG MAIZE MEAL, 20L COOKING OIL.
Rule: Use Anchor Dispersal floorplans. Demand 30-day supplier credit to capture the positive float.
The Decoy (The Asymmetric Triage):
Profile: A high-priced Premium item that you do not actually want to sell in high volumes.
Examples: 4,000 KES IMPORTED OLIVE OIL.
Rule: You stock exactly two bottles. You place them directly next to the 1,500 KES local olive oil. The Decoy makes the 1,500 KES oil look like a bargain, accelerating the local oil's velocity. You are using the expensive SKU purely as a psychological pricing anchor.

Interactive Tool: The Dynamic ROP Engine
To train operators on why flat averages fail, this simulator visually proves the necessity of the Micro-Seasonality Multiplier during the Kenyan Payday Pulse.
Show me the visualization
This concludes Part III: The 23,000-Node Neural Network. We have mapped the universe, executed the vendor purge, engineered the Halo effect, walled off the Cannibals, and written the genetic code for the exact behavior of the remaining items.
We have built the perfect, theoretical machine.
Are you ready to compile Part III into our master document, and then step into the grand finale? Part IV: From Data to Engine, where we take all of this logic and plug it directly into the architecture of the OASIS Auto-Replenishment System.
We have now arrived at the summit. You have the floorplans, the economic laws, the receiving protocols, and the mathematical DNA of every SKU in the Kenyan market.
But a human being cannot manage 23,000 nodes across 415,000 edges using a clipboard and a prayer. To survive the modern trade environment, you must delegate the cognitive load to a machine.
Here is the masterclass deep dive into Part IV: From Data to Engine. This is the blueprint for OASIS (The Optimal Assortment & Systemic Inventory Solution).

Part IV: From Data to Engine (The Future)
