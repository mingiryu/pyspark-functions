#!/bin/bash
export PYSPARK_DRIVER_PYTHON=ipython
export PYARROW_IGNORE_TIMEZONE=1

pyspark \
--executor-memory 1G \
--driver-memory 1G \
