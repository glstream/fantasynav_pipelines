from airflow.decorators import task
import requests
from psycopg2.extras import execute_batch
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def players_pull():
    player_url = "https://api.sleeper.app/v1/players/nfl"

    req = requests.get(player_url)
    players = req.json()
    return players

@task
def players_transform(players):
    players_df = pd.DataFrame.from_dict(players)

    all_players_df = players_df.T
    all_players_df = all_players_df.reset_index()
    all_players_df = all_players_df.drop(columns=["index"])


    trimmed_players_df = all_players_df[["player_id", "full_name", "position", "age","team"]]
    ap_list = trimmed_players_df.values.tolist()
    # ap_list = [i.extend([i[1]]) for i in ap_list]
    ap_list_filtered = [i for i in ap_list if i[2] in ['QB', 'RB', 'WR', 'TE']]
    ap_list = [[i[0],i[1].split(" ")[0], i[1].split(" ")[-1], i[1], i[2], i[3], i[4]] for i in ap_list_filtered]
    print(ap_list[0])

    pg_hook = PostgresHook(postgres_conn_id='postgres_akv')
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    cursor.execute("DELETE FROM dynastr.players;")
    conn.commit()

    cursor = conn.cursor()
    execute_batch(cursor, """
    INSERT INTO dynastr.players 
    (player_id, first_name, last_name, full_name, player_position, age, team)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (player_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    full_name = EXCLUDED.full_name,
    player_position = EXCLUDED.player_position,
    age = EXCLUDED.age,
    team = EXCLUDED.team;
    """
    , tuple(ap_list)
    , page_size=1000)
    conn.commit()
    cursor.close()
    conn.close()
    return "dynastr.players"

@task
def players_surrogate_key_clean(table_name:str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_akv')
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(f"""UPDATE {table_name}
                    SET first_name = replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(first_name,'.',''), ' Jr', ''), ' III',''),'Jeffery','Jeff'), 'Joshua','Josh'),'William','Will'), ' II', ''),'''',''),'Kenneth','Ken'),'Mitchell','Mitch'),'DWayne','Dee')
                        """)
    conn.commit() 
    cursor.close()
    conn.close()   
    return

@task
def edge_case_names(table_name:str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_akv')
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(f"""UPDATE dynastr.players SET first_name = 'Gabriel' WHERE player_id = '6943'""")
    conn.commit() 
    cursor.close()
    conn.close()   
    return

@task
def remove_conflicting_players(table_name:str):
    pg_hook = PostgresHook(postgres_conn_id='postgres_akv')
    conn = pg_hook.get_conn()

    cursor = conn.cursor()
    print("Connection established")
    cursor.execute(f"""DELETE FROM dynastr.players where player_id = '4634'""")
    conn.commit() 
    cursor.close()
    conn.close()   
    return



    
