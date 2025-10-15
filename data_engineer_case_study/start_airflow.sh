#!/bin/bash
airflow scheduler -D &
airflow api-server -D &
airflow dag-processor &