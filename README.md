# sales-datawarehouse-etl

   

## Table of Contents
1. Objective
2. Architecture
3. Modules

### Objective
Implement end to end data pipeline for sales data warehouse. The data is loaded in json format by day in directory. The data is first loaded into a staging table on redshift. From there, it is transformed and made ready to be inserted into fact table. Once loaded into the fact table, staging table is dropped. 

The project uses 1 month worth of data for month of Jan where each day has around 100 invoices, spread accros 51 customers, 31 products and 11 suppliers. 

For scheduler, airflow is used. Redshift is used for datawarehouse storage. 

### Architecture

Each json file is one user sales transaction. All transactions for a day are kept in a directory. The directory name is formatted to reflect the day. 

Directory name format: d<YEAR>-<MONTH>-<DAY>

Helper class `LoadInvoice` from ``sales_dw.py`is used in the scheduler processes. `load_invoice_staging` function is invoked in a daily frequency which reads all transaction from the directory of the specific cay, into a pandas dataframe and loads it into Redshift staging table. 

Redshift staging table name format: `invoice_staging_<YEAR>_<MONTH>_<DAY>`

After that, `load_invoice_fact` transforms staging table and loads facts with surrogate keys into the `invoice_fact` table. Once facts are loaded, staging table is dropped. 

- Image of the architecture. 
![Architecture](https://user-images.githubusercontent.com/16570874/151455825-b0dc39dd-10c3-4c60-aebb-1a0d54b79b5e.jpg)

- Image of ER Diagram
![sql-er](https://user-images.githubusercontent.com/16570874/151455877-199b8c99-c4bb-4152-992f-0d33dd87c71a.png)
  
  - product,customer,store,purchase_datetime are the dimensions
  - sales_fact is fact table where sales_price and quantity  are facts aggregated for one day

### Module	

Airflow is used to transform the data and load it into redshift. First invoice_stagging data is loaded and then aggregated data is kept into invoice_fact table and the staging table is deleted, which is shown in below figure.

- Image of DAG!
 ![Screenshot from 2022-01-24 13-45-18](https://user-images.githubusercontent.com/16570874/151455919-19b7abe3-fef1-4b01-9bd7-81a0208e1871.png)

- Airflow setup details. 

    Follow [here] (https://www.progress.com/tutorials/jdbc/connect-to-redshift-salesforce-and-others-from-apache-airflow) to setup airflow and connect it to redshift. Connection page should look like this.
    
![Screenshot from 2022-01-23 22-34-14](https://user-images.githubusercontent.com/16570874/151455968-c44590de-bd8b-4ff8-9f29-9310247d9f71.png)

- Image of RUNs

- On success, we see below tree in green.
![Screenshot from 2022-01-24 13-44-00](https://user-images.githubusercontent.com/16570874/151456005-335638dd-9681-46ee-8d93-0332da1e4f3a.png)

- Image of dashboard and explanations
