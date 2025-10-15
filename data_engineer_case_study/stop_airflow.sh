#!/bin/zsh
echo "Stopping all Airflow processes..."
pkill -f "airflow"
echo "All Airflow processes stopped."