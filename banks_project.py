# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Bank", "MC_USD_Billion"]
csv_path = "exchange_rate.csv"
csv_path_output = "Largest_banks_data.csv"
sql_connection = sqlite3.connect("Banks.db")
table_name = "Largest_banks"

query_1 = "SELECT * FROM Largest_banks"
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_3 = "SELECT Bank FROM Largest_banks LIMIT 5"

def log_progress(message):
    time_stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_entry = f"{time_stamp}: {message}\n"

    with open("code_log.txt", "a") as file:
        file.write(log_entry)

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    # Find all tables with class wikitable
    tables = data.find_all('table', class_='wikitable')

    # The FIRST table is the one under "By market capitalization"
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')

        if len(col) >= 3:
            bank = col[1].text.strip()
            market_cap = col[2].text.strip()

            # Skip invalid rows
            if market_cap != '':
                data_dict = {
                    "Bank": bank,
                    "MC_USD_Billion": market_cap
                }

                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    # Clean column
    df["MC_USD_Billion"] = df["MC_USD_Billion"].str.replace(r"[^\d.]", "", regex=True)
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)
    return df

def transform(df, csv_path):
    # Read exchange rate CSV
    exchange_df = pd.read_csv(csv_path)
    
    # Convert to dictionary
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    
    # Convert rates to float
    gbp_rate = float(exchange_rate['GBP'])
    eur_rate = float(exchange_rate['EUR'])
    inr_rate = float(exchange_rate['INR'])
    
    # Create new columns
    df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print("\nExecuting query:")
    print(query_statement)
    
    output = pd.read_sql(query_statement, sql_connection)
    print(output)

# Extraction
log_progress("Preliminaries complete. Initiating ETL process")
df = extract (url, table_attribs)
print(df)
log_progress("Data extraction complete. Initiating Transformation process")

# Transformation
df = transform(df, csv_path)
print(df)
log_progress("Data transformation complete. Initiating Loading process")

# Loading to CSV
load_to_csv(df, csv_path_output)
log_progress("Data saved to CSV file")

# Loading to DB
log_progress("SQL Connection initiated")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Executing queries
run_query(query_1, sql_connection)
run_query(query_2, sql_connection)
run_query(query_3, sql_connection)
log_progress("Process Complete")

# Closing connection
sql_connection.close()
log_progress("Server Connection closed")