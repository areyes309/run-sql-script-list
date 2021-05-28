import os
import sys
import time
from datetime import datetime
import threading
import requests
import concurrent.futures
import pandas as pd
from simple_salesforce import Salesforce
import sqlalchemy as sa

# Setup DateTime and Path
thread_local = threading.local()
dt = datetime.now()
datepath = dt.strftime("\\%Y\\%m-%b\\%#m-%#d-%y\\")
path = r"C:\Users\Desktop"
mypath = path+datepath
start_time = time.time()
duration = time.time() - start_time

# Connecting to SQL Server database
engine = sa.create_engine('mssql+pyodbc://database.dns')
df_cred = pd.read_sql('login 111111',engine)
sfc = df_login.iloc[0]
sf_org = 'domain.example.salesforce.com'
sf = Salesforce(instance_url=sf_org, session_id='')
sf = Salesforce(username=sfc['username'], password=sfc['password'], version='40.0')

# Run the list of SQL Scripts
def run_job(script):
    sql = open(script, "r").read()
    soql = sql.replace('    ', '')
    sf_results = sf.query_all(soql)
    df = pd.DataFrame(sf_results['records']).drop(columns=['attributes'])

    # related columns
    related_cols = df[['Account', 'Product2']]
    for col in related_cols:
        temp_df = pd.DataFrame(related_cols[col])
        for obj in temp_df:
            obj_df = pd.DataFrame(temp_df[obj].to_dict()).transpose()
            new_columns = dict()
            for col_name in obj_df.columns:
                col_dict = {}
                item_name = obj + col_name
                new_columns[col_name] = obj + col_name
            obj_df = obj_df.rename(columns=new_columns)
            df = df.drop(columns=obj).merge(obj_df[[x for x in obj_df.columns if not x.endswith('attributes')]]
                                            , how='inner', right_index=True, left_index=True)
                                            
    # Save the data pulled in a csv file                                        
    task = script.split(".", 1)
    csv_file = '%s_%s.csv' % (task[0], dt.strftime("%Y%m%d"))
    print(csv_file)
    df.to_csv(mypath+csv_file)
    
# Go through list of sql scripts
def run_all_jobs(jobs):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(run_job, jobs)

if __name__ == "__main__":
    
    # take filename input
    try:
        filename = sys.argv[2]
    except IndexError:
        filename = 'scripts.txt'
        pass

    # Open the text file containing a list of scripts
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            jobs = f.readlines()
            jobs = [l.replace('\n', '') for l in jobs]
  
    except IOError:
        sys.exit(0)

    run_all_jobs(jobs)
    print(f"{len(jobs)} tasks")
