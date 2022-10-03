import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# дефолтные параметры для dag'a:
default_args = {
    'owner': 'd.kandy',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 11),
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
    
    query = '''
select toDate(time) as day, user_id, action
from simulator_20220720.feed_actions
where toDate(time) between  today() - 7 and today()-1;
'''

    df = ph.read_clickhouse(query, connection=connection)
    
    data = pd.merge(df.groupby('day')['user_id'].nunique().reset_index(),
         df.groupby(['day','action']).count().unstack().droplevel(level=0, axis=1).reset_index(),
         how = 'inner', 
         on = 'day')

    data.columns = ['day','DAU','likes','views']
    data['CTR'] = data['likes'] / data['views']
    
    report = f'''NEWSFEED \nreport for {data.iloc[-1,:].day.strftime('%d-%m-%Y')}: \n 
DAU: {data.iloc[-1,1]} 
Views: {data.iloc[-1,3]}  
Likes: {data.iloc[-1,2]}  
CTR: {data.iloc[-1,4].round(4)}  \n
'''
    sns.set(font_scale=1,
       style="whitegrid",
       rc={'figure.figsize':(10,13)})

    fig, axes = plt.subplots(nrows=4, ncols=1, sharex=True)


    sns.lineplot(x='day', y='DAU', data = data, ax = axes[0], color = 'lightpink', marker="^")
    sns.lineplot(x='day', y='likes', data = data, ax = axes[1], color = 'purple', marker="^")
    sns.lineplot(x='day', y='views', data = data, ax = axes[2], color = 'orchid', marker="^")
    sns.lineplot(x='day', y='CTR', data = data, ax = axes[3], color = 'tomato', marker="^")


    for name, ax in zip([i for i in data][1:], axes.flatten()):
        ax.set_frame_on(False)
        ax.grid(True, color='#e2e2e2', alpha=0.6)
        ax.set(title = name, ylabel='')

    ax.set_xlabel('\n')

    ax.set_xticklabels(list(data['day'].apply(lambda x: x.strftime('%d %b'))))

    fig.suptitle('\n\nNEWSFEED METRICS', fontsize=14)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'newsfeed_metrics.png'
    plot_object.seek(0)

    plt.close()
    
    bot.sendMessage(chat_id = chat_id, text=report)
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    # bot.sendMessage(chat_id = reports_chat_id, text=report)
    # bot.sendPhoto(chat_id = reports_chat_id, photo = plot_object)
    
    
# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_dkandy_report():
    
    # Функция для отправки репорта
    @task
    def send_report():
        report(chat_id = -770113521)

    # Задачи DAG'а:
    send_report()

        
dag_dkandy_report = dag_dkandy_report()
# reports_chat_id = -770113521
