import sys
import os

# Try to find where pitch_data_ingestor_v2 is coming from
try:
    import pitch_data_ingestor_v2
    print(f"DEBUG: pitch_data_ingestor_v2 path: {pitch_data_ingestor_v2.__file__}")
except ImportError as e:
    print(f"DEBUG: Could not import pitch_data_ingestor_v2: {e}")

print(f"DEBUG: sys.path: {sys.path}")
