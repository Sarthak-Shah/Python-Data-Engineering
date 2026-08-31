"""Tiny logging helper so every container produces the same, greppable format."""

from __future__ import annotations

import logging
import os
import sys


def configure_logging(component: str) -> logging.Logger:
    """
    Configure root logging once and return a named logger.

    Every line is prefixed with the component name, so when you run
    `docker compose logs -f` the interleaved output from the ingestor, the API
    and Airflow stays readable.
    """
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format=f"%(asctime)s | {component} | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        force=True,  # override Airflow's own handlers when running inside a task
    )
    return logging.getLogger(component)
