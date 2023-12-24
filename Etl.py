import numpy as np
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    conn = requests.get(url).text
    soup = BeautifulSoup(conn, 'html.parser')
    table = soup.find_all('tbody')
    rows = table[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        coll = row.find_all('td')
        if len(coll) != 0:
            data_dict = {
                "Name": coll[1].contents[2].text,
                "MC_USD_Billion": float((coll[2].text).strip('\n'))
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):
    df['MC_GBP_Billion'] = round(df["MC_USD_Billion"] * 0.8, 2)
    df['MC_EUR_Billion'] = round(df["MC_USD_Billion"] * 0.93, 2)
    df['MC_INR_Billion'] = round(df["MC_USD_Billion"] * 82.95, 2)
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)


def load_to_db(df, table_name, sql_connection):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_name = 'Largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'

#log_progress('Initial process completed. ETL process has started...')
 
df = extract(url, table_attribs)

'''log_progress('Data Extraction completed. Data Transformation has started...')'''

df = transform(df)
print(df)
'''log_progress('Data transformation completed. Data loading process has started...')

load_to_csv(df, csv_path)
log_progress('Data saved to csv file.')

db_name = 'Banks.db'
sql_connection = sqlite3.connect(db_name)
log_progress('Database connection initiated.')

load_to_db(df, table_name, sql_connection)
log_progress('Data loaded to the database as a table. Running the queries...')

query_statement_1 = f"SELECT * from {table_name}"
query_statement_2 = f"SELECT avg(MC_GBP_Billion) from {table_name}"
query_statement_3 = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement_1, sql_connection)
log_progress('First query ran. Running the second query...')
run_query(query_statement_2, sql_connection)
log_progress('Second query ran. Running the third query...')
run_query(query_statement_3, sql_connection)
log_progress('Third query ran. Closing the database connection...')
sql_connection.close()
log_progress('ETL process completed.')'''
