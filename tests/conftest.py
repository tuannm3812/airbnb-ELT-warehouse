import os
import sys

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")
sys.path.insert(0, os.path.abspath(DAGS_DIR))
