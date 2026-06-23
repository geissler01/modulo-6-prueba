from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='test_cluster_workers_bash',
    start_date=datetime(2023, 1, 1),
    schedule=None, # <-- ¡El estándar de Airflow 3!
    catchup=False,
    tags=['test', 'keppler']
) as dag:
    
    # Creamos 6 tareas independientes y en paralelo
    for i in range(1, 7):
        BashOperator(
            task_id=f'worker_saluda_{i}',
            # El comando 'hostname' saca el ID del contenedor/máquina actual.
            # 'sleep 5' retiene al worker para que los otros asuman las demás tareas.
            bash_command=f'echo "¡Hola! Soy el worker: $(hostname) ejecutando la tarea {i}" && sleep 5'
        )