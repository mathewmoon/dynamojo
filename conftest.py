import os
import sys

# Ensure tests run against the local source tree, not any system-installed
# `dynamojo` package that might shadow it.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
