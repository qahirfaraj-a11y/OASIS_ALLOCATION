
import sys
import os

# Add the project root to sys.path so 'oasis' can be imported
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

# Standard import path for the OrderEngine
try:
    from oasis.logic.order_engine import OrderEngine
except ImportError as e:
    print(f"Critical: Could not find OrderEngine. Error: {e}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)


def test_init():
    try:
        # Create a dummy data directory
        data_dir = "C:/Users/iLink/.gemini/antigravity/scratch/oasis/data"
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
            
        engine = OrderEngine(data_dir=data_dir)
        print("OrderEngine initialized successfully.")
    except Exception as e:
        print(f"Initialization failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_init()
