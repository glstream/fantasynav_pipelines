from datetime import datetime, timedelta
from airflow.decorators import dag # DAG and task decorators for interfacing with the TaskFlow API
from datetime import datetime
from tasks.sleeper import (players_pull, 
players_transform, 
players_surrogate_key_clean, 
edge_case_names,
remove_conflicting_players)
from airflow.models import Variable

dag_owner = 'dynasty_superflex_db'
# dynasty_sf_config = Variable.get(dag_owner, deserialize_json=True)

@dag(
    default_args={
        'owner': dag_owner,
        'depends_on_past': False,
        'email': ['grayson.stream@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='Sleeper API Player names and metadata pull',
    schedule_interval="@weekly",
    start_date=datetime(2022, 6, 8),
    catchup=False,
    tags=['requests', 'database'])

def incr_players_load():
    sleeper_players = players_pull()
    sleeper_transform = players_transform(sleeper_players)
    players_surrogate_key_clean(sleeper_transform)
    edge_case_names(sleeper_transform)
    remove_conflicting_players(sleeper_transform)

incr_players_load = incr_players_load()
