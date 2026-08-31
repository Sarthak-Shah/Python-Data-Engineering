"""
Make the `pipeline` package importable from inside Airflow.

docker-compose mounts `./src` at `/opt/airflow/src` and sets PYTHONPATH, so this
is belt-and-braces for anyone running Airflow another way.  Keeping it in one
place means the DAG files themselves stay free of path plumbing.
"""

import sys
from pathlib import Path

SRC = Path("/opt/airflow/src")
if SRC.exists() and str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
