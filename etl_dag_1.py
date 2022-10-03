import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# подключения к разным бд:
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw',
    'password':'656e2b0c9c'
}


# дефолтные параметры для dag'a:
default_args = {
    'owner': 'd.kandy',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 9),
}


# устанавливаем расписание:
schedule_interval = '0 12 * * *'


# запросы для выгрузки нужных данных:

query_actions = """
select
toDate(time) as event_date,
user_id as user,
gender,
age,
os,
countIf(action='view') as views,
countIf(action='like') as likes
from simulator_20220720.feed_actions
--from {db}.feed_actions
where toDate(time) = yesterday()
group by event_date, user_id, gender, age, os
"""

query_messages = """
select * 
from
(
select toDate(time) as event_date,
        user_id as user,
        gender,
        age,
        os,
        count(reciever_id) as messages_sent,
        count (distinct reciever_id) as users_sent

from simulator_20220720.message_actions
where toDate(time) = yesterday()
group by event_date, user, gender, age, os
)a

FULL JOIN

(
select toDate(time) as event_date,
        reciever_id as user,
        count() as messages_received,
        count (distinct user_id) as users_received


from simulator_20220720.message_actions
where toDate(time) = yesterday()
group by event_date, user
)b

using event_date, user

"""

# Функция для получения df из бд:
def get_df(query='SELECT 1', connection=connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df


# Функция для загрузки датафрейма в бд в таблицу test.dkandy_test (она уже создана отдельно)
def load_df(df, connection=connection_test):
    ph.to_clickhouse(df, 'dkandy_test', connection=connection, index=False)

    
    
# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_dkandy():

    # Функция для получения данных
    @task
    def extract(query):
        df = get_df(query=query)
        return df

    # Функция для объединения двух таблиц в одну 
    @task
    def merge(df_actions, df_messages):
        df_all = pd.merge(df_actions,
                df_messages,
                how = 'outer',
                on = ['event_date','user'])

        df_all['gender'] = df_all['gender_x'].mask(pd.isnull, df_all['gender_y'])
        df_all['age'] = df_all['age_x'].mask(pd.isnull, df_all['age_y'])
        df_all['os'] = df_all['os_x'].mask(pd.isnull, df_all['os_y'])

        df_all = df_all[['event_date','user','gender','age','os','views','likes','messages_sent','users_sent','messages_received','users_received']].fillna(0)

        return df_all
    
    # Функция для преобразования данных
    @task
    def transform(metric, df_all):
        df_transform = df_all.groupby(['event_date',metric]).sum().reset_index()[['event_date', metric, 'views', 'likes',
           'messages_sent', 'users_sent', 'messages_received', 'users_received']]\
                                        .rename(columns = {metric :'dimension_value'})
        df_transform.insert(1, 'dimension', metric)
        return df_transform

    # Функция для сохранения данных в таблицу
    @task
    def load(df_gender, df_age, df_os):
        df_to_load = pd.concat([df_gender, df_age, df_os],ignore_index=True)
        df_to_load[[i for i in df_to_load][3:]] = df_to_load[[i for i in df_to_load][3:]].astype('int64')
        load_df(df_to_load)

    # Задачи DAG'а:
    df_actions = extract(query_actions)
    df_messages = extract(query_messages)
    
    df_all = merge(df_actions, df_messages)
    
    df_gender = transform ('gender', df_all)
    df_age = transform('age', df_all)
    df_os = transform('os', df_all)
    
    load(df_gender, df_age, df_os)


dag_dkandy = dag_dkandy()
