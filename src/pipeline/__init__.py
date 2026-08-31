"""
A minimal, end-to-end data engineering pipeline built around the medallion
architecture (bronze -> silver -> gold).

Read the modules in this order to follow the data:

    config.py          where everything lives on disk
    schemas.py         the data contract for a log event
    sources/           how data arrives (MQTT stream, rotated log files)
    layers/bronze.py   raw capture into the lake
    layers/silver.py   cleansing, typing, de-duplication into the warehouse
    layers/gold.py     star schema + business-ready marts
    quality.py         the checks that decide whether gold gets published
    demo.py            runs the whole flow in one process, no Docker required
"""

__version__ = "0.1.0"
