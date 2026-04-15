# replenishment Logic (Golden State v9.1)
The primary goal is functional and formatting parity with the historical stable version.

## ⚓ Historical Baseline Priority
For **Fresh Departments**, the `historical_avg_order_qty` is the primary anchor. 
- If `historical_avg_order_qty > 0`, the engine prioritizes this over aggressive ADS-based jumps.
- This prevents over-ordering based on single-day sales spikes.

## 📈 Velocity Scaling
- **Demand Smoothing**: The engine uses a moving average to calculate sales velocity.
- **Trend Modulation**: `growing` trends receive a 15% boost, while `declining` trends receive a 10% reduction.

## 🛡️ Strategic Caps
Final coverage depth is capped to prevent waste:
- **Daily Fresh**: 3.0 days.
- **Chilled/General Fresh**: 6.0 days.
- **UHT/Long Life**: 7.0 days.
- **General Dry**: 25.0 days.
