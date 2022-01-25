import time
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
import json
import requests
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
import datetime
from sales_dw import LoadInvoice



#datetime.datetime(2022,01,12,00,00,00),
args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2022,1,1),
    'end_date': datetime.datetime(2022,1,4),
    'depends_on_past': False,
    'provide_context': True,
    'backfill': True
}

dag = DAG(
    dag_id='datawarehouse_etl',
    default_args=args,
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=16
)
   
# load the data to the aws redshift
def load_invoice_staging(**op_kwargs):
    print(op_kwargs)
    ts = pd.to_datetime(op_kwargs['ts_var'])
    print("ts: ", ts)
    print(type(ts))
    
    
    obj = LoadInvoice()
    invoice_df = obj.read_invoices(ts.year, ts.month, ts.day)
    print(invoice_df)

    invoice_df['invoice_date'] = datetime.datetime(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")

    # push to redshift staging table
    # redshift is the connection id of aws redshift
    redshift_hook = PostgresHook('redshift')
    invoice_df.to_sql('invoice_staging_{}_{}_{}'.format(ts.year, ts.month, ts.day), redshift_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)


def load_invoice_fact(**op_kwargs):
    print(op_kwargs)
    
    ts = pd.to_datetime(op_kwargs['ts_var'])

    redshift_hook = PostgresHook('redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    sql_statement = LoadInvoice().get_sql('invoice_staging_{}_{}_{}'.format(ts.year, ts.month, ts.day))
    cursor.execute(sql_statement)
    cursor.close()
    conn.commit()

def del_invoice_staging(**op_kwargs):
    print(op_kwargs)
    
    ts = pd.to_datetime(op_kwargs['ts_var'])

    redshift_hook = PostgresHook('redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    sql_statement = LoadInvoice().del_sql('invoice_staging_{}_{}_{}'.format(ts.year, ts.month, ts.day))
    cursor.execute(sql_statement)
    cursor.close()
    conn.commit()

with dag:

    start_operator = DummyOperator(task_id='start_execution')

    load_invoice_staging = PythonOperator(
        task_id='load_invoices',
        python_callable=load_invoice_staging,
        provide_context=True,
        op_kwargs={
            'key':'value',
            'ts_var': '{{ ts }}',
            'tsnodash_var': '{{ ts_nodash }}'
        }
    )

    load_invoice_fact = PythonOperator(
        task_id='load_invoice_fact',
        python_callable=load_invoice_fact,
        provide_context=True,
        op_kwargs={
            'key':'value',
            'ts_var': '{{ ts }}',
            'tsnodash_var': '{{ ts_nodash }}'
        }
    )

    del_invoice_staging = PythonOperator(
        task_id = 'del_invoice_staging',
        python_callable = del_invoice_staging,
        provide_context= 'True',
        op_kwargs={
            'key':'value',
            'ts_var': '{{ ts }}',
            'tsnodash_var': '{{ ts_nodash }}'
        }

    )


    end_operator = DummyOperator(task_id='stop_execution')

    start_operator >> load_invoice_staging >> load_invoice_fact >> del_invoice_staging >> end_operator
