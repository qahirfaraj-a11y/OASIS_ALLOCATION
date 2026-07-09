import os
import sys
import torch

sys.path.insert(0, os.getcwd())
from ops_dashboard import get_gnn_resources, get_all_store_risks

final_scores = get_all_store_risks(14)
print("Final Risk Scores:")
for k, v in final_scores.items():
    print(f"{k}: {v}")
