---
type: Chapter
chapter: 7
title: "Chapter 7: Mapping the SKU Universe (Algorithmic Interrogation)"
---
# Chapter 7: Mapping the SKU Universe (Algorithmic Interrogation)

To the traditional retailer, an inventory list is a flat spreadsheet. They look at a column labeled "Units Sold" and make their purchasing decisions. This is why they go bankrupt.
To the algorithmic operator running the OASIS system, an inventory list is a multi-dimensional graph. You do not interrogate a supplier by asking them how much profit they offer; you interrogate their catalog by running their SKUs through three specific network algorithms: Degree Centrality Matrix, Community Detection, and K-Core Decomposition.
7.1 The Interrogation Matrix: Centrality vs. Velocity
When a Tier 1 supplier (like Brookside or Unilever) demands that you stock their entire catalog of 150 items, you run those items through the Interrogation Matrix.
You plot every single SKU on a two-axis graph.
The Y-Axis (POS Velocity): How many physical units move through the till in 30 days.
The X-Axis (Degree Centrality): How many link edges this node shares with other products in the store (its Halo Power).
When the algorithmic operator maps a supplier's catalog onto this matrix, the SKUs instantly shatter into four strict quadrants, dictating exactly how you treat the supplier.
Quadrant 1: The Core (High Velocity, High Centrality)
Data Signature: BROOKSIDE 500ML POUCH. Sells 500 units a day, maps to 94 other items.
The Treatment: Submission. You concede to the supplier on these specific items. You accept their 7-day credit terms because this node is the structural foundation of your store's cash flow.
Quadrant 2: The Keystones (Low Velocity, High Centrality)
Data Signature: MR. BERRYS LOLLIPOP or SPECIFIC BRAND OF MATCHBOX. Sells 20 units a day, but maps to 600+ other items.
The Treatment: Protection. The supplier will often ignore these items because they are low-ticket. You must force the supplier to prioritize them. You mandate high safety-stock levels in the OASIS auto-replenisher, because if a Keystone stocks out, the massive web of attached sales collapses.
Quadrant 3: The Lone Wolves (High Velocity, Low Centrality)
Data Signature: AQUAMIST 20L REFILL. Sells 100 units a day, but maps to 0 other items.
The Treatment: Capital Leverage. Because this item brings cash but doesn't build basket size, you treat it purely as a financial instrument. You demand 30-day credit terms from the supplier. You use the Lone Wolf purely to generate the Positive Float we discussed in Chapter 2.
Quadrant 4: The Dead Weight (Low Velocity, Low Centrality)
Data Signature: The obscure 250ml flavored milks, the 14th brand of imported pasta.
The Treatment: The Guillotine. This is the bloat the supplier is trying to force onto your balance sheet. These items do not sell, and they do not link to anything that sells. You immediately delist them.
7.2 Community Detection: Unmasking the "Brand Equity" Lie
Suppliers will often try to leverage their success in one category to force you to stock their failures in another. A massive dairy supplier might launch a new line of fruit juices and tell you, "Our brand is trusted; the juice will sell."
The algorithmic operator does not trust marketing; they run a Community Detection Algorithm (like the Louvain method) across the edges.csv file.
The Math: The algorithm groups SKUs into highly dense "Communities" based on how frequently they are bought together (e.g., the "Morning Tea Community," the "Weekend Nyama Choma Community," the "Baby Care Community").
The Ground Truth: When you run the dairy supplier's new fruit juice through the network, you discover it possesses zero edges connecting it to the "Morning Tea Community" where their milk dominates. The juice is an orphaned node.
The Execution: You pull up the data and tell the distributor: "Your brand equity does not transfer mathematically. Your juice is sitting in a dead zone outside of any purchasing community. I will not stock it on my main shelves. If you want it in the building, you pay a slotting fee for an end-cap."
7.3 K-Core Decomposition (The Mathematical Purge)
In Chapter 1, we established that Minimarts and Mega-Stores are infected by Cannibal Zones (like the [[WINES]] or [[COSMETICS]] departments, which can hold 900+ SKUs that merely steal sales from each other).
How do you safely cut 80% of a department without accidentally deleting a crucial product? You do not just cut the bottom 80% of sales. You use K-Core Decomposition.
Think of the network as a massive onion. K-Core Decomposition strips away the outermost, weakest layers of the network one by one.
The 1-Core Strip: The algorithm identifies and deletes every SKU that is only connected to one other item in the entire store (usually a direct substitute). These are the absolute parasites.
The 2-Core Strip: It then deletes every SKU connected to only two items.
The Core Result: As you peel away the dead layers, you are left with the dense, highly interconnected center of the department. Out of 915 wines, you are left with the 45 "Predator" wines that possess high velocity and true customer loyalty.
You execute the purge based on this K-Core mapping. You box up the hundreds of peeled-away nodes, return them to the Tier 3 suppliers, and reclaim your working capital. The store’s total revenue remains identical, but the capital required to run the aisle drops by 80%.
