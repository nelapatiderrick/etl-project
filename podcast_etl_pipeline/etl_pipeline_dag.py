import requests
import pendulum
import xmltodict
import os
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024,8,6),
    catchup=False
)
def podcast_summary():
    # Creates database on first run, otherwise task is skipped.
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r'''
        CREATE TABLE IF NOT EXISTS episodes(
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT
            )
        ''',
        sqlite_conn_id='podcasts'
    )
    
    # Gets podcast data
    @task()
    def get_episodes():
        data = requests.get('https://www.marketplace.org/feed/podcast/marketplace/')
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']['channel']['item']
        print(f'Found {len(episodes)} episodes.')
        return episodes
    
    # Loads each column for each podcast record
    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id='podcasts')
        stored = hook.get_pandas_df('SELECT * FROM episodes')
        new_episodes = []
        for episode in episodes:
            if episode['link'] not in stored['link'].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode['link'], episode['title'], episode['pubDate'], episode['description'], filename])
        hook.insert_rows(table='episodes',rows=new_episodes,target_fields=['link','title','published','description','filename'])

    # Downloads mp3 files for each record
    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join('/home/derrick/airflow/dags/episodes', filename)
            if not os.path.exists(audio_path):
                print(f'Downloading {filename}')
                audio = requests.get(episode['enclosure']['@url'])
                with open(audio_path, 'wb+') as f:
                    f.write(audio.content)


    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)
    load_episodes(podcast_episodes)
    download_episodes(podcast_episodes)

summary = podcast_summary()