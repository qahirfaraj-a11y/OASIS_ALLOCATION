
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import os

def generate_synthetic_data(n_samples=5000):
    np.random.seed(42)
    
    # Features
    # 1. Huff Probability (0.0 to 1.0)
    huff_prob = np.random.uniform(0, 1, n_samples)
    
    # 2. Competitor Density (counts within 5km)
    comp_density = np.random.randint(0, 25, n_samples)
    
    # 3. Min Distance to Competitor (km)
    min_dist_comp = np.random.uniform(0.1, 10.0, n_samples)
    
    # 4. Regional Affluence Index (1.0 to 5.0)
    affluence = np.random.uniform(1.0, 5.0, n_samples)
    
    # 5. Traffic Friction (0.0 to 1.0)
    traffic_friction = np.random.uniform(0.0, 1.0, n_samples)
    
    # 6. Store Type Preference (0: Express/Small, 1: Medium, 2: Hyper/Large)
    store_type = np.random.randint(0, 3, n_samples)
    
    # GROUND TRUTH LOGIC (What we want the model to learn)
    # Success Score (0.0 to 1.0)
    
    # Base success from Huff and Affluence
    success = (huff_prob * 0.4) + (affluence / 5.0 * 0.3)
    
    # Non-linear interaction: 
    # High comp density is POSITIVE for Express (store_type=0) but NEGATIVE for Hyper (store_type=2)
    for i in range(n_samples):
        if store_type[i] == 0: # Express
            # Synergy effect in high-density areas (footfall)
            success[i] += (comp_density[i] / 25.0) * 0.2
            success[i] -= (traffic_friction[i] * 0.1) # Less sensitive to traffic
        elif store_type[i] == 1: # Medium
            success[i] -= (comp_density[i] / 25.0) * 0.1
            success[i] -= (traffic_friction[i] * 0.15)
        else: # Hyper
            # Big stores need their own space
            success[i] -= (comp_density[i] / 25.0) * 0.3
            if min_dist_comp[i] < 2.0: success[i] -= 0.2
            success[i] -= (traffic_friction[i] * 0.2) # Very sensitive to traffic
            
    # Add noise
    success += np.random.normal(0, 0.05, n_samples)
    success = np.clip(success, 0, 1)
    
    df = pd.DataFrame({
        'huff_prob': huff_prob,
        'comp_density': comp_density,
        'min_dist_comp': min_dist_comp,
        'affluence': affluence,
        'traffic_friction': traffic_friction,
        'store_type': store_type,
        'target': success
    })
    
    return df

def train_model():
    print("Generating synthetic expansion dataset...")
    df = generate_synthetic_data(10000)
    
    X = df.drop('target', axis=1)
    y = df['target']
    
    print("Training RandomForestRegressor...")
    model = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42)
    model.fit(X, y)
    
    model_path = "expansion_model.joblib"
    joblib.dump(model, model_path)
    print(f"Model saved to {model_path}")
    
    # Verify importance
    importances = model.feature_importances_
    for name, imp in zip(X.columns, importances):
        print(f"Feature: {name:15} | Importance: {imp:.4f}")

if __name__ == "__main__":
    train_model()
