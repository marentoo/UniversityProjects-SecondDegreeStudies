from datasets import load_dataset
import pandas as pd
import json
import csv
# from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error
import time
from dateutil import parser

##-------------------------------------------------------------------------------------------
##loading
##loading first dataset
dataset = load_dataset('enryu43/twitter100m_tweets', split='train[:1%]')
df = dataset.to_pandas()
df = df.iloc[0:13500]
df = df.drop(columns=['user'])
df = df.drop(columns=['id'])
df = df.rename(columns={'date': 'creation_date'})
df['creation_date'] = df['creation_date'].apply(lambda x: parser.parse(x).strftime('%Y-%m-%d %H:%M:%S'))
## df = df.sample(frac=1).reset_index(drop=True)
## df.to_csv('sample_dataset.csv', index=False, header = True, sep=',',encoding="utf-8")

##loading dataset to replace text tweet
to_replace_dataset = pd.read_csv('test_dataset_text_only.csv',sep = '\t', encoding='utf-8')
num_rows_to_replace = min(len(df), len(to_replace_dataset))
df.loc[0:num_rows_to_replace - 1, 'tweet'] = to_replace_dataset.iloc[0:num_rows_to_replace, 0]
df = df.sample(frac=1).reset_index(drop=True)
df.to_csv('dataset.csv',index=False)


##-------------------------------------------------------------------------------------------
## batching

CSV_FILE = 'dataset.csv'
BATCH_SIZE = 150 #150
SLEEP_TIME = 5 #3600 # 1 hour in seconds

DB_HOST = 's154.cyber-folks.pl'
DB_USER = 'suppmed_dw'
DB_PASSWORD = '***'
DB_NAME = 'suppmed_istan'
TABLE_NAME = 'tweet_raw'

def load_batches(file_name, batch_size):
    for chunk in pd.read_csv(file_name, chunksize=batch_size):
        yield chunk

    
def insert_batch_to_db(connection, data, table_name):
    cursor = connection.cursor()
    columns = ', '.join(data.columns)  
    placeholders = ', '.join(['%s'] * len(data.columns))
    data_tuples = [tuple(x) for x in data.to_numpy()]  
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    try:
        cursor.executemany(sql, data_tuples)
        connection.commit()
        print(sql)
    except mysql.connector.Error as e:
        print(sql)
        print(f"SQL Error: {e}")
        connection.rollback()
    except Exception as e:
        print(f"Other Error: {e}")
        connection.rollback()
    finally:
        cursor.close()


try:
    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    print("Connected to database")

    for batch in load_batches(CSV_FILE, BATCH_SIZE):
        print(f"Inserting batch:\n{batch}")
        insert_batch_to_db(connection, batch, TABLE_NAME)
        time.sleep(SLEEP_TIME)

    connection.close()
    print("Connection closed")

except mysql.connector.Error as err:
    print(f"Error: {err}")
except Exception as e:
    print(f"An error occurred: {e}")