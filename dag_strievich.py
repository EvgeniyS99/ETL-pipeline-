from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20221020'
}
connection_to_load = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c', 
    'user': 'student-rw',
    'database': 'test'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-strievich',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 6),
}

schedule_interval = '0 06 * * *' # каждый день в 6 утра

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def the_main_dag():

    @task
    def extract_feed():
        """
        Таск, который выгружает данные из таблицы feed_actions
        """
        q = """
        select
            user_id,
            toDate(time) as day,
            gender,
            case
                when age < 18 then '0-18'
                when age >= 18 and age < 25 then '18-24'
                when age >= 25 and age < 35 then '25-34'
                when age >= 35 and age < 45 then '35-44'
                else '45+'
            end as age,
            os,
            countIf(action = 'view') as views,
            countIf(action = 'like') as likes
          from
            simulator_20221020.feed_actions
          where
            day = yesterday()
          group by
            user_id,
            gender,
            age,
            os,
            day
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df

    @task
    def extract_messages():
        """
        Таск, который выгружает данные из таблицы feed_actions
        """
        q = """
        --сколько юзер получает сообщений
        with recieve_user as (
          select
            t1.reciever_id,
            messages_received,
            t1.day,
            users_received
          from
            (
              select
                reciever_id,
                count(reciever_id) as messages_received,
                toDate(time) as day
              from
                simulator_20221020.message_actions
              where
                day = yesterday()
              group by
                reciever_id,
                day
            ) t1
            join (
              select
                reciever_id,
                count(distinct user_id) as users_received, -- от скольких пользователей получил сообщение
                toDate(time) as day
              from
                simulator_20221020.message_actions
              where
                day = yesterday()
              group by
                reciever_id,
                day
            ) t2 using(reciever_id)
        ),
        --сколько юзер отсылает сообщений
        send_user as (
          select
            t1.user_id,
            messages_sent,
            t1.day,
            gender,
            age,
            os,
            users_sent
          from
            (
              select
                user_id,
                count(user_id) as messages_sent,
                toDate(time) as day,
                gender,
                case
                    when age < 18 then '0-18'
                    when age >= 18 and age < 25 then '18-24'
                    when age >= 25 and age < 35 then '25-34'
                    when age >= 35 and age < 45 then '35-44'
                    else '45+'
                end as age,
                os
              from
                simulator_20221020.message_actions
              where
                day = yesterday()
              group by
                user_id,
                gender,
                age,
                os,
                day
            ) t1
            join (
              select
                user_id,
                count(distinct reciever_id) as users_sent, -- скольким пользователям отправил сообщение
                toDate(time) as day
              from
                simulator_20221020.message_actions
              where
                day = yesterday()
              group by
                user_id,
                day
              order by
                user_id
            ) t2 using(user_id)
        ),
        messages as (
          select
            if(t1.user_id = 0, t2.reciever_id, t1.user_id) as user_id,
            if(t1.day = '1970-01-01', t2.day, t1.day) as day,
            os,
            gender,
            age,
            messages_sent,
            users_sent,
            messages_received,
            users_received
          from
            send_user t1 full
            outer join recieve_user t2 on t1.user_id = t2.reciever_id
        )

        select * from messages
        """
        df = ph.read_clickhouse(q, connection=connection)
        idx_of_empty_string = np.where(df.apply(lambda x: x == ''))[0].tolist()
        df.iloc[idx_of_empty_string, 3] = ''
        return df

    @task
    def make_join(df_feed, df_mes):
        """
        Таск, который джойнит таблицы feed_actions и message_actions
        """
        merge_df = df_mes.merge(df_feed, left_on='user_id', right_on='user_id', how='outer')
        merge_df = merge_df.replace('', np.nan)
        # заполняем пропущенные значения 
        merge_df['os_x'].fillna(merge_df['os_y'], inplace=True)
        merge_df['day_x'].fillna(merge_df['day_y'], inplace=True)
        merge_df['gender_x'].fillna(merge_df['gender_y'], inplace=True)
        merge_df['age_x'].fillna(merge_df['age_y'], inplace=True)
        # заменяем 0 и 1 на пол 
        merge_df['gender_x'] = merge_df['gender_x'].map({0: 'male', 1: 'female'})
        # заполняем пропущенные значения в срезах
        merge_df['gender_x'] = merge_df['gender_x'].fillna('no_info')
        merge_df['os_x'] = merge_df['os_x'].fillna('no_info')
        merge_df['day_x'] = merge_df['day_x'].fillna('no_info')
        merge_df['age_x'] = merge_df['age_x'].fillna('no_info')
        # заполняем оставшиеся пропущенные числовые значения
        merge_df.fillna(0, inplace=True)
        # удаляем ненужные колонки и переименовываем оставшиеся колонки
        merge_df.drop(['gender_y', 'os_y', 'age_y', 'day_y'], axis=1, inplace=True)
        merge_df.rename(columns={'day_x': 'event_date', 
                                 'age_x': 'age',
                                 'os_x': 'os',
                                 'gender_x': 'gender'
                                }, inplace=True)
        
        return merge_df

    @task
    def gender_slice(df):
        """
        Таск, который считает метрики по полу
        """
        gender_df = df.copy()
        gender_metrics = gender_df.drop(['user_id', 'age', 'os'], axis=1)\
            .groupby(['event_date', 'gender'], as_index=False).sum()\
            .rename(columns={'gender': 'dimension_value'})
        gender_metrics.insert(1, 'dimension', 'gender')
        
        return gender_metrics

    @task
    def age_slice(df):
        """
        Таск, который считает метрики по возрасту
        """
        age_df = df.copy()
        age_metrics = age_df.drop(['user_id', 'gender', 'os'], axis=1)\
            .groupby(['event_date', 'age'], as_index=False).sum()\
            .rename(columns={'age': 'dimension_value'})
        age_metrics.insert(1, 'dimension', 'age')
    
        return age_metrics

    @task
    def os_slice(df):
        """
        Таск, который считает метрики по ОС
        """
        os_df = df.copy()
        os_metrics = os_df.drop(['user_id', 'gender', 'age'], axis=1)\
            .groupby(['event_date', 'os'], as_index=False).sum()\
            .rename(columns={'os': 'dimension_value'})
        os_metrics.insert(1, 'dimension', 'os')
        
        return os_metrics

    @task 
    def concat_slices(gender_df, age_df, os_df):
        """
        Таск, который объединяет все срезы по полу, возрасту и ОС
        """
        final_df = pd.concat([gender_df, age_df, os_df])\
            .reset_index()\
            .drop('index', axis=1)
        
        final_df = final_df[['event_date', 'dimension', 'dimension_value', 'views', 'likes',
                             'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        final_df = final_df.astype({'views': 'int64', 'likes': 'int64', 'messages_received': 'int64',
                                    'messages_sent': 'int64', 'users_received': 'int64',
                                    'users_sent': 'int64'})
        return final_df 

    @task
    def load(final_df):
        """
        Таск, делающий выгрузку в clickhouse
        """
        q = """
        create table if not exists test.strievich_etl_data_2
        (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
        ) ENGINE = MergeTree()
        order by event_date
        """
        ph.execute(query=q, connection=connection_to_load)
        ph.to_clickhouse(df=final_df, table='strievich_etl_data_2', index=False, connection=connection_to_load)
        print('ok')
        return final_df
    
    df_feed = extract_feed()
    df_mes = extract_messages()
    joint_df = make_join(df_feed, df_mes)
    gender_metrics = gender_slice(joint_df)
    age_metrics = age_slice(joint_df)
    os_metrics = os_slice(joint_df)
    final_df = concat_slices(gender_metrics, age_metrics, os_metrics)
    load(final_df)
    
the_main_dag = the_main_dag()
    