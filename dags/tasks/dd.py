import requests
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime

enrty_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")


@task()
def dd_redraft_players_pull():
    redraft_url = "https://dynasty-daddy.com/api/v1/values/adp/redraft"
    redraft_data = requests.get(redraft_url).json() if requests.get(redraft_url).status_code == 200 else None
    return redraft_data


@task()
def insert_redraft_data(redraft_data, rank_type="redraft"):
    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    insert_query = '''
    INSERT INTO dynastr.dd_player_ranks VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (name_id, rank_type) DO UPDATE SET
    trade_value = EXCLUDED.trade_value,
    sf_trade_value = EXCLUDED.sf_trade_value,
    sf_position_rank = EXCLUDED.sf_position_rank,
    position_rank = EXCLUDED.position_rank,
    all_time_high_sf = EXCLUDED.all_time_high_sf,
    all_time_low_sf = EXCLUDED.all_time_low_sf,
    all_time_high = EXCLUDED.all_time_high,
    all_time_low = EXCLUDED.all_time_low,
    three_month_high_sf = EXCLUDED.three_month_high_sf,
    three_month_high = EXCLUDED.three_month_high,
    three_month_low_sf = EXCLUDED.three_month_low_sf,
    three_month_low = EXCLUDED.three_month_low,
    last_month_value = EXCLUDED.last_month_value,
    last_month_value_sf = EXCLUDED.last_month_value_sf,
    all_time_best_rank_sf = EXCLUDED.all_time_best_rank_sf,
    all_time_best_rank = EXCLUDED.all_time_best_rank,
    all_time_worst_rank_sf = EXCLUDED.all_time_worst_rank_sf,
    all_time_worst_rank = EXCLUDED.all_time_worst_rank,
    three_month_best_rank_sf = EXCLUDED.three_month_best_rank_sf,
    three_month_best_rank = EXCLUDED.three_month_best_rank,
    three_month_worst_rank_sf = EXCLUDED.three_month_worst_rank_sf,
    three_month_worst_rank = EXCLUDED.three_month_worst_rank,
    last_month_rank = EXCLUDED.last_month_rank,
    last_month_rank_sf = EXCLUDED.last_month_rank_sf,
    sf_overall_rank = EXCLUDED.sf_overall_rank,
    overall_rank = EXCLUDED.overall_rank
    '''
    for item in redraft_data:
        cursor.execute(insert_query, (
            item['name_id'], rank_type, item['trade_value'], item['sf_trade_value'], 
            item['sf_position_rank'], item['position_rank'], item['all_time_high_sf'],
            item['all_time_low_sf'], item['all_time_high'], item['all_time_low'],
            item['three_month_high_sf'], item['three_month_high'], item['three_month_low_sf'],
            item['three_month_low'], item['last_month_value'], item['last_month_value_sf'],
            item['all_time_best_rank_sf'], item['all_time_best_rank'], item['all_time_worst_rank_sf'],
            item['all_time_worst_rank'], item['three_month_best_rank_sf'], item['three_month_best_rank'],
            item['three_month_worst_rank_sf'], item['three_month_worst_rank'], item['last_month_rank'],
            item['last_month_rank_sf'], item['sf_overall_rank'], item['overall_rank']
        ))
        # Commit changes and close the connection
    conn.commit()
    return


@task()
def dd_dynasty_players_pull():
    dynasty_url = "https://dynasty-daddy.com/api/v1/values/adp/dynasty"
    dynasty_data = requests.get(dynasty_url).json() if requests.get(dynasty_url).status_code == 200 else None
    return dynasty_data

@task()
def insert_dynasty_data(dynasty_data, rank_type="dynasty"):
    pg_hook = PostgresHook(postgres_conn_id="postgres_akv")
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")

    insert_query = '''
    INSERT INTO dynastr.dd_player_ranks VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (name_id, rank_type) DO UPDATE SET
    trade_value = EXCLUDED.trade_value,
    sf_trade_value = EXCLUDED.sf_trade_value,
    sf_position_rank = EXCLUDED.sf_position_rank,
    position_rank = EXCLUDED.position_rank,
    all_time_high_sf = EXCLUDED.all_time_high_sf,
    all_time_low_sf = EXCLUDED.all_time_low_sf,
    all_time_high = EXCLUDED.all_time_high,
    all_time_low = EXCLUDED.all_time_low,
    three_month_high_sf = EXCLUDED.three_month_high_sf,
    three_month_high = EXCLUDED.three_month_high,
    three_month_low_sf = EXCLUDED.three_month_low_sf,
    three_month_low = EXCLUDED.three_month_low,
    last_month_value = EXCLUDED.last_month_value,
    last_month_value_sf = EXCLUDED.last_month_value_sf,
    all_time_best_rank_sf = EXCLUDED.all_time_best_rank_sf,
    all_time_best_rank = EXCLUDED.all_time_best_rank,
    all_time_worst_rank_sf = EXCLUDED.all_time_worst_rank_sf,
    all_time_worst_rank = EXCLUDED.all_time_worst_rank,
    three_month_best_rank_sf = EXCLUDED.three_month_best_rank_sf,
    three_month_best_rank = EXCLUDED.three_month_best_rank,
    three_month_worst_rank_sf = EXCLUDED.three_month_worst_rank_sf,
    three_month_worst_rank = EXCLUDED.three_month_worst_rank,
    last_month_rank = EXCLUDED.last_month_rank,
    last_month_rank_sf = EXCLUDED.last_month_rank_sf,
    sf_overall_rank = EXCLUDED.sf_overall_rank,
    overall_rank = EXCLUDED.overall_rank
    '''
    for item in dynasty_data:
        cursor.execute(insert_query, (
            item['name_id'], rank_type, item['trade_value'], item['sf_trade_value'], 
            item['sf_position_rank'], item['position_rank'], item['all_time_high_sf'],
            item['all_time_low_sf'], item['all_time_high'], item['all_time_low'],
            item['three_month_high_sf'], item['three_month_high'], item['three_month_low_sf'],
            item['three_month_low'], item['last_month_value'], item['last_month_value_sf'],
            item['all_time_best_rank_sf'], item['all_time_best_rank'], item['all_time_worst_rank_sf'],
            item['all_time_worst_rank'], item['three_month_best_rank_sf'], item['three_month_best_rank'],
            item['three_month_worst_rank_sf'], item['three_month_worst_rank'], item['last_month_rank'],
            item['last_month_rank_sf'], item['sf_overall_rank'], item['overall_rank']
        ))
        # Commit changes and close the connection
    conn.commit()
    return