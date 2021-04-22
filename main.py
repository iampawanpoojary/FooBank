import pandas as pd
import numpy as np

visits_url = 'http://rocker-data-engineering-task.storage.googleapis.com/data/visits.csv'
customers_url = 'http://rocker-data-engineering-task.storage.googleapis.com/data/customers.json'
loan_csv_list_url = 'http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-10.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-11.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2017-12.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-1.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-10.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-11.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-12.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-2.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-3.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-4.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-5.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-6.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-7.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-8.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2018-9.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-1.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-10.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-2.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-3.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-4.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-5.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-6.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-7.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-8.csv, http://rocker-data-engineering-task.storage.googleapis.com/data/loan-2019-9.csv'

def download_data(loan_csv_list_url, customers_url, visits_url):
    visits = pd.read_csv(visits_url)
    customers = pd.read_json(customers_url, lines=True)
    #list of all data files, this would be easier to pick if directly from a GCS bucket
    loan_csv_list = loan_csv_list_url.split(', ')
    #for url in loan_csv_list:
    #    print(url)
    
    return loan_csv_list, customers, visits

def preprocess_data(loan_csv_list, customers, visits):
    #combine multiple dataframes and use datetime instead of epoch
    dfs = [pd.read_csv(url) for url in loan_csv_list]
    loan = pd.concat(dfs, ignore_index=True)
    loan['timestamp'] = pd.to_datetime(loan['timestamp'], unit='s')
    ##format webvisit_id
    loan['webvisit_id'] = loan['webvisit_id'].str.replace(r'[()]', '').str.replace(r',', '')
    #webvisit_id object to numeric
    loan['webvisit_id'] = pd.to_numeric(loan['webvisit_id'], errors='coerce').convert_dtypes() 
    loan['webvisit_id'] =loan['webvisit_id'].fillna(0)
    loan = loan.drop("Unnamed: 0",axis=1)
    loan = loan.replace(0, np.nan)
    
    #use datetime
    visits = visits.drop("Unnamed: 0",axis=1)
    visits['timestamp'] = pd.to_datetime(visits['timestamp'], unit='s')
    
    return loan, visits

loan_csv_list, customers, visits = download_data(loan_csv_list_url, customers_url, visits_url)
loan, visits = preprocess_data(loan_csv_list, customers, visits)

#dropna() is used as a workaround to exclude left join on nan values
loan_visits_ljoin = pd.merge(loan[pd.notnull(loan.webvisit_id)], visits, how='left', left_on='webvisit_id', right_on='id',  suffixes=('_loan', '_visits'))
loan_visits_ljoin.sort_values('user_id').head()

loan_customers_ljoin = pd.merge(loan, customers, how='left', left_on='user_id', right_on='id', suffixes=('_loan', '_customers'))
loan_customers_ljoin.sort_values('user_id').head()

loan_customers_visits = pd.merge(loan_customers_ljoin, loan_visits_ljoin, how='left', on=['id_loan','user_id', 'loan_amount', 'loan_purpose', 'outcome', 'interest', 'webvisit_id'])
loan_customers_visits.sort_values('user_id').head()

loan_customers_visits.to_csv('data_foobank_test.csv')