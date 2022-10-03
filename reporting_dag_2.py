import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import telegram
import io


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# дефолтные параметры для dag'a:
default_args = {
    'owner': 'd.kandy',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 12),
}


# устанавливаем расписание:
schedule_interval = '0 11 * * *'


def report(chat_id = None):
    chat_id = chat_id or 149956060
    
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220720',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

    bot = telegram.Bot(token = '5556378163:AAF2Pd7mB5mGSLcP12sFeYuX21UfzoWj6PM')
    
    # top-15 viewed posts for the last 7 days

    query = '''
SELECT post_id AS post_id,
           countIf(action='view') AS views,
           countIf(action='like') AS likes,
           countIf(action='like') / countIf(action='view') AS "CTR",
           count(DISTINCT user_id) AS "unique users"
FROM simulator_20220720.feed_actions
where toDate(time) between  today() -7 and today() - 1
GROUP BY post_id
ORDER BY views DESC
LIMIT 15;
'''

    top_posts = ph.read_clickhouse(query, connection=connection)
    top_posts.CTR = top_posts.CTR.round(3)
    top_posts.index += 1

    query_1 = '''
SELECT * FROM 
(SELECT toDate(time) AS day,
          count(action) as all_actions,
          countIf(action='view') AS views,
          countIf(action='like') AS likes,
          countIf(action='like') / countIf(action='view') AS "CTR",
          count(DISTINCT user_id) AS unique_users_feed,
          count(DISTINCT post_id) as viewed_posts
FROM simulator_20220720.feed_actions 
where toDate(time) between today() -30 and today() - 1
GROUP BY toDate(time)) f 
FULL JOIN 
(SELECT toDate(time) AS day,
            count(user_id) as messages,
            count(DISTINCT user_id) AS unique_users_mess
FROM simulator_20220720.message_actions
where toDate(time) between today() -30 and today() - 1
GROUP BY toDate(time)) m USING day
'''

    data = ph.read_clickhouse(query_1, connection=connection)

    query_2 = '''
SELECT day AS day,
                  action AS action,
                  AVG(actions) AS avg_actions
FROM
    (select toDate(time) as day,
              user_id,
              action,
              count(action) as actions
      FROM simulator_20220720.feed_actions
      where toDate(time) between today() -30 and today() - 1
      group by day, user_id, action
    UNION ALL select toDate(time) as day,
              user_id,
              'message' as action,
              count(user_id) as actions
      FROM simulator_20220720.message_actions
      where toDate(time) between today() -30 and today() - 1
      group by day, user_id, action)
    GROUP BY day, action
    ORDER BY day, action
'''

    actions = ph.read_clickhouse(query_2, connection=connection)

    actions = actions.pivot(index='day', columns='action', values='avg_actions').reset_index() \
                    .rename(columns={'like':'likes_per_user','message':'messages_per_user','view':'views_per_user'})

    report = f'''Newsfeed & Messenger\nreport for {data.iloc[-1,:].day.strftime('%d-%m-%Y')}: \n 
DAU feed: {data.iloc[-1,5]} 
Views & Likes: {data.iloc[-1,1]}
Views per user:{int(actions.iloc[-1,-1])}
Likes per user:{int(actions.iloc[-1,1])}
Viewed posts: {data.iloc[-1,6]}\n
DAU messenger: {data.iloc[-1,-1]}
Messages: {data.iloc[-1,-2]}
Messages per user:{int(actions.iloc[-1,-2])}\n
'''

    sns.set(font_scale=1,
           style="whitegrid",
           rc={'figure.figsize':(15,20)})

    fig, axes = plt.subplots(nrows=4, ncols=1, sharex=True)


    sns.lineplot(x='day', y='unique_users_feed', data = data, ax = axes[0], color = 'cornflowerblue', marker="o")
    sns.lineplot(x='day', y='unique_users_mess', data = data, ax = axes[1], color = 'deepskyblue', marker="o")
    sns.lineplot(x='day', y='all_actions', data = data, ax = axes[2], color = 'orange', marker="o")
    sns.lineplot(x='day', y='messages_per_user', data = actions, ax = axes[3], color = 'gold', marker="o")


    for name, ax in zip(['Newsfeed DAU','Messenger DAU','Total views&likes','Messages per user'], axes.flatten()):
        ax.set_frame_on(False)
        ax.grid(True, color='#e2e2e2', alpha=0.6)
        ax.set(title = name, ylabel='')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %b'))

    ax.set_xlabel('\n')

    fig.suptitle('\n\nNewsfeed & Messenger metrics', fontsize=14)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'newsfeed_messenger_metrics.png'
    plot_object.seek(0)


    file_object = io.StringIO()
    top_posts.to_csv(file_object)
    file_object.seek(0)

    bot.sendMessage(chat_id = chat_id, text=report)
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    bot.sendDocument(chat_id = chat_id, document = file_object, filename = 'top-15 viewed posts for the last 7 days.csv')
    
    
# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_dkandy_report_2():
    
    # Функция для отправки репорта
    @task
    def send_report():
        report(chat_id = -770113521)

    # Задачи DAG'а:
    send_report()

        
dag_dkandy_report_2 = dag_dkandy_report_2()
# reports_chat_id = -770113521
