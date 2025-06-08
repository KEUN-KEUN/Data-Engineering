for dag in $(airflow dags list | awk 'NR>2 {print $1}'); do
    echo "=== DAG: $dag ==="
    airflow dags list-runs -d "$dag" --state running
done
