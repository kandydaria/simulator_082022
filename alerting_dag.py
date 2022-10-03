import pandas as pd
import pandahouse as ph

import io
import telegram
import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime, timedelta
import sys
import os 


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# дефолтные параметры для dag'a:
default_args = {
    'owner': 'd.kandy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 8, 15),
}


# устанавливаем расписание:
schedule_interval = '*/15 * * * *'



def check_anomaly(df, metric):
#     функция вычисляет 25 и 75 квартили для каждой 15-минутки в течение дня на основе данных за предыдущие 15 дней и
#     проверяет вхождение последней сегодняшней 15-минутки в рассчитанный интервал 

# Берем всего лишь последние 15 дней, т.к. если взять, например, 30, то интервал становится шире - наша аудитория и просмотры сильно возросли с того момента,
# и алерт может не сработать.

#  Для расчета межквартильного размаха задаем коэффициент а для каждой метрики:
    coef = {'users_feed':2,
           'views':1.3,
           'likes':1.4,
           'CTR':2,
           'users_messenger':2.5,
           'messages':2.5}
    
    a = coef[metric]

    df_previous = df[df['date'] < df['date'].iloc[-1]]
    df_previous = pd.merge(df_previous.groupby('hm')[[metric]].quantile(0.25).reset_index(),
                             df_previous.groupby('hm')[[metric]].quantile(0.75).reset_index(),
                             how= 'outer',
                             on='hm',
                             suffixes=('_q25', '_q75'))
    
    df_previous = pd.merge(df_previous,
                          df[df['date'] < df['date'].iloc[-1]].groupby('hm')[[metric]].quantile(0.5).reset_index().rename(columns={metric:'median'}),
                          how='outer',
                          on='hm')
    
    df_previous['iqr'] = df_previous[f'{metric}_q75'] - df_previous[f'{metric}_q25']
        
    df_previous['up'] = df_previous[f'{metric}_q75'] + a * df_previous['iqr']
    df_previous['low'] = df_previous[f'{metric}_q25'] - a * df_previous['iqr']
    
    df_previous['up'] = df_previous['up'].rolling(3, center=True, min_periods=1).mean()
    df_previous['low'] = df_previous['low'].rolling(3, center=True, min_periods=1).mean()
    df_previous['median'] = df_previous['median'].rolling(3, center=True, min_periods=1).mean()
    
    df1 = pd.merge(df[df['date'] == df['date'].iloc[-1]],
                    df_previous,
                    on='hm',
                    how='outer')
    
#     если наше значение ниже нижней границы или выше верхней, то такое значение считаем аномалией
    if (df1[df1.ts.notna()][metric].iloc[-1] < df1[df1.ts.notna()]['low'].iloc[-1]) or (df1[df1.ts.notna()][metric].iloc[-1] > df1[df1.ts.notna()]['up'].iloc[-1]):
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df1


def run_alerts(chat=None):
#     system of alerts
    chat_id = chat or 149956060
    
    connection = {'host': '',
                      'database':'',
                      'user':'', 
                      'password':''
                     }

    
    bot = telegram.Bot(token = '')
    
    query = '''
select * from
(SELECT
       toStartOfFifteenMinutes(time) as ts
     , toDate(time) as date
     , formatDateTime(ts, '%R') as hm
     , uniqExact(user_id) as users_feed
     , countIf(user_id, action='view') as views
     , countIf(user_id, action='like') as likes
     , countIf(user_id, action='like')/countIf(user_id, action='view') as CTR
 FROM simulator_20220720.feed_actions
 WHERE ts >=  today() - 15 and ts < toStartOfFifteenMinutes(now())
 GROUP BY ts, date, hm) f
full join 
(SELECT 
        toStartOfFifteenMinutes(time) as ts
        , toDate(time) as date
        , formatDateTime(ts, '%R') as hm
        , uniqExact(user_id) as users_messenger
        , count(user_id) as messages
FROM simulator_20220720.message_actions
WHERE ts >=  today() - 15 and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm) m 
USING (ts, date, hm)
ORDER BY ts

'''

    data = ph.read_clickhouse(query, connection=connection)

    
    metrics_list = ['users_feed', 'views', 'likes', 'CTR', 'users_messenger', 'messages']

    for metric in metrics_list:
        
        df = data[['ts','date','hm',metric]].copy()
        
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1:
            
            curr_value = df[df.ts.notna()][metric].iloc[-1] 
            
# отклонение метрики будем считать от медианы значений за предыдущие 15 дней
            diff = abs(1 - curr_value / df[df.ts.notna()]['median'].iloc[-1])*100
                
            if metric == 'CTR':
                curr_value = curr_value.round(3)
            else:
                curr_value = int(curr_value)
                
            msg = f'''❗️Alert for: {df[df.ts.notna()]['ts'].iloc[-1].strftime('%d %b %H:%M')} ❗️\nMetric:  {metric} \nCurr value: {curr_value} \nDiff: {diff.round()}% \n@kandydaria\nhttps://superset.lab.karpov.courses/superset/dashboard/1484/'''
            
            
            sns.set(rc={'figure.figsize': (16, 10)},
                   style="whitegrid",
                   font_scale=1,)
            
            
            plt.tight_layout()
            
            ax= sns.lineplot(x=df['hm'], y = df[metric], label=metric, linewidth = 1.7, color = 'darkorchid')
            ax= sns.lineplot(x=df['hm'], y = df['up'], label='up', color = 'limegreen')
            ax= sns.lineplot(x=df['hm'], y = df['low'], label='low', color = 'orange')
            ax= sns.lineplot(x=df['hm'], y = df['median'], label='median', alpha=0.9, linestyle='--', color = 'pink')
            
            
            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 8 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
                    
            ax.set(xlabel='time', ylabel='')
            ax.set_frame_on(False)
            ax.set_title(metric.upper() +'  ' + df[df.ts.notna()]['ts'].iloc[-1].strftime('%d %b %H:%M'), fontsize=18)
            ax.grid(True, color='#e2e2e2', alpha=0.6)
            
                    # формируем файловый объект
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric}.png'
            plt.close()
            
            bot.sendMessage(chat_id = chat_id, text = msg)
            bot.sendPhoto(chat_id = chat_id, photo = plot_object)
            
            bot.sendMessage(chat_id = 149956060, text = '!Alert! Check channel')
    
    print(f"All metrics have been checked for: {data['ts'].iloc[-1].strftime('%d %b %H:%M')}")
    bot.sendMessage(chat_id = 149956060, text = f"All metrics have been checked for: {data['ts'].iloc[-1].strftime('%d %b %H:%M')}")      
    


# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_dkandy_alert():
    
    # Функция для отправки алерта
    @task
    def alert():
        run_alerts(-788021703)
        
    alert()
    
        
dag_dkandy_alert = dag_dkandy_alert()


# alerts_chat_id = -788021703
