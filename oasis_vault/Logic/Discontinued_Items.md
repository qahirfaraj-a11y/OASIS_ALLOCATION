# Discontinued Items Logic
Logic introduced to handle items that are no longer being delivered or sold in specific branches.

## 📅 The 21-Day Rule (Fresh)
For items in **Fresh Departments**:
- **Check**: The engine calculates `days_since_last_grn` based on historical synchronization.
- **Threshold**: 如果 `days_since_last_grn > 21` days:
	- Item is flagged as `is_discontinued = True`.
	- **Recommended Qty** is forced to `0`.
	- **Reasoning**: `"Discontinued/No Sales (Last GRN > 21d)"`.

## 🎯 Implementation Points
- **IntelligenceMixin**: Performs the aging check during data enrichment.
- **RuleBasedLLM**: Respects the flag and zeroes out the order.
- **ProcurementMixin**: Excludes these items from Pass 1 width allocation in Greenfield mode.
