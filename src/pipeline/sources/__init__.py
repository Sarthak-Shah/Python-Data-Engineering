"""
The two ways data enters this pipeline.

    mqtt_stream.py  STREAMING source -- live log events published to an MQTT
                    broker by services/devices, consumed continuously and written
                    to the landing zone as micro-batches.

    log_files.py    BATCH source -- yesterday's rotated `.log` files, picked up
                    once a day by a scheduled Airflow DAG.

Both write into the SAME landing zone in the SAME line format, which is the
reason a single bronze/silver/gold code path serves both.  That convergence --
"stream and batch differ only in arrival, not in meaning" -- is the practical
core of the medallion (and lambda/kappa) architectures.
"""
