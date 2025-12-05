import os
import sys

print(f"Python Version: {sys.version}")
print(f"Environment: {os.environ.get('VIRTUAL_ENV', 'No virtual environment active')}")
print(f"Current Working Directory: {os.getcwd()}")
print(f"System Path: {sys.path}")
