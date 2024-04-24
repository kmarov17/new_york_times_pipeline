import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

minio_access_key = "gggx0XlLbw1kA4h7"
minio_secrete_key = "KfzW8JudTbKNonEsy6RCoq42QiEoGZSK"

def get_datas():
    api_key = "9X5XpWS91ddEQJ2Otd7jys8tZmL7kOGc"
    res = requests.get(f"https://api.nytimes.com/svc/news/v3/content/all/all.json?api-key={api_key}")
    res = res.json()
    res = res['results'][0]

    return res

def stream_data():
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    try:
        res = get_datas()

        producer.send('last_post_retrieved', json.dumps(res).encode('utf-8'))
    except Exception as e:
        logging.error(f'An error occured: {e}')

def write_to_file():
    from kafka import KafkaConsumer
    file_name = datetime.now().strftime("%y%m%d_%H:%M:%S")

    # Créez un consommateur Kafka pour lire les messages du topic 'last_post_retrieved'
    consumer = KafkaConsumer('last_post_retrieved', bootstrap_servers=['broker:29092'])
    print(consumer)

    if consumer is not None:
        with open(f'{file_name}.txt', 'w') as file:
            # Lire chaque message du topic
            for message in consumer:
                # Décoder le message JSON
                data = json.loads(message.value.decode('utf-8'))
                # Écrire les données dans le fichier texte
                file.write(json.dumps(data) + '\n')
    else:
        print("No data")

with DAG('post_automation',
         default_args=default_args,
         schedule_interval='*/1 * * * *',  # Exécute toutes les 5 minutes
         catchup=False) as dag:

    # Tâche pour récupérer les données de l'API et les envoyer à Kafka
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    # Tâche pour lire les données depuis Kafka et les écrire dans un fichier texte
    write_to_file_task = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file,
        # Attendez que la tâche de streaming soit terminée avant de démarrer celle-ci
        trigger_rule='all_done'
    )

    # Définir la dépendance entre les tâches
    streaming_task >> write_to_file_task

# write_to_file()