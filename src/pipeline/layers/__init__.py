"""The three medallion layers, one module each.

    bronze.py  landing files  ->  raw Parquet in the data lake
    silver.py  bronze Parquet ->  cleansed, typed, de-duplicated warehouse table
    gold.py    silver table   ->  star schema + denormalised marts (+ Parquet export)

Each module exposes plain functions that take/return simple values, so they can be
called from an Airflow task, from a unit test, or from `python -m pipeline.demo`
without any framework in the way.
"""
