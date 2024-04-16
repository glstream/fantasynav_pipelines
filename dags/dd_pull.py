from datetime import datetime, timedelta
from airflow.decorators import dag
from datetime import datetime
from tasks.dd import (
    dd_redraft_players_pull,
    insert_redraft_data,
    dd_dynasty_players_pull,
    insert_dynasty_data
)

dag_owner = "grayson.stream"

@dag(
    default_args={
        "owner": dag_owner,
        "depends_on_past": False,
        "email": ["grayson.stream@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
     description="API Call to Dyansty Daddy",
     schedule_interval="55 14 * * *",
     start_date=datetime(2023,7,29),
     catchup=False,
     tags=["api_call", "database"],
     )

def dd_players_pull():

    redraft_players = dd_redraft_players_pull()
    insert_redraft_data(redraft_players)
    dynasty_players = dd_dynasty_players_pull()
    insert_dynasty_data(dynasty_players)
    
dd_players_pull = dd_players_pull()