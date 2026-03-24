import sys
import os

# Adds the project root to sys.path so pyspark and other deps are correctly found
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
