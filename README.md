# Intro

The task is approached as a 1-time analysis with  deviation from “minimalistic” pipeline to enable additional analyses where it doesn’t cost much.

Airflow (`load_data_to_snowflake.py`) converts source .xlsx files into .csv with , loads them to Snowflake Stage and then copies data to Snowflake datawarehouse into `SUMUP.RAW.<table_name>`.

DBT stages data from RAW schema in `STAGING`, and builds models in `WAREHOUSE` and `ANALYSES` schemas.

DBT models aim to follow DBT official style guide and recomendations: lower case, multiple CTEs, no table aliases, models structure, etc. Airflow code aims to follow PEP-8 python style guide.

### Folders 

| Folder | Description |
| --- | --- |
| Question answers | SQL scripts for answering analytical questions. The querries are compiled from DBT models (models/materialized_analyses |
| airflow_wsl_test | Airflow instance configuren with a virtunal environment and WSL (windows subsystem for linux). Most interesting file is dags/load_data_to_snowflake.py. <br> Virtual environment and Logs folders were removed due to git conflicts. |
| sumup_data | Raw data in .xlsx files |
| sumup_dbt | DBT models and DBT-syntaxed analytical querries |

# Assumptions

### Source data

  - Timestamps in all tables have no timezone
  
  - `Transaction.amount` is in eurocents
  
  - Letter ‘v’ in transaction.product_sku number is the only type of error in this data field
  
  - `Store.created_at` are incorrect for many stores and it is fair to substitute it with a timestamp of the first transaction made by a store
  
  - Data fields `product_name`, `product_category`, `customer_id`, `happened_at` are of no use, as they are completely random
  
  - Data fields `card_number`, `cvv` are of no use and should be removed as excessive personal data
  
  - `customer_id` field in a form of an excel formula was not supposed to get to the DW and replicate it’s random definition there.

 ### Task scope

  - This is a one-time analysis that won’t require accumulation of data over time
  
  - If real data size is larger, it is stored in a data lake, properly partitioned by year/month/day
  
  - The data is supposed to be imported from a local device to a cloud datawarehouse and then analyzed there.

### Performance assumptions

- Original data tables are small enough to be handled in 1 batch by local computer (.xslx > .csv) and by Snowflake Operators (.csv from local to snowflake stage)

- Calogica tests `dbt_expectations.expect_column_value_lengths_to_equal` and  `dbt_expectations.expect_column_values_to_be_in_set` do not hinder performance on large datasets (to be tested on larger data).

# Limitations / points for improvement

### Airflow / Snowflake

- Iteration through multiple files / batching not implemented

- Executor is set to SequentialExecutor instead of Local or Celery (due to wsl issues with databases)

- Previous data in snowflake tables is truncated before new data is inserted (to make multiple test runs of DAGs). For the production environment truncation should be removed

- Duplicate data control is not implemented for the case of iterating through multiple files of data

- There is no systems of loaded data checking, recent operations roll-back, early error checks (to see the error before loading large amount of data).

### DBT

- Incremental materialization for transaction (large) tables is not implemented since it’s a one time analysis. Yet larger chunks of data that would not be processable by dbt in one run may require setting incremental materialization.

- Analyses results data formats are kept ‘raw’ - percentages are displayed as fractions, transaction amounts are kept in eurocents. 

- Subsampling techniques and batch processing to save compute resources for analyses are not applied

- There was no datawarehouse optimization within the project (indexes, partitioning), so DBT uses filters and joins based solely on analytical needs.

# Design details

### Pipeline

1. Airflow converts received .xlsx files into CSV
    
    Snowflake cannot read data from .xlsx, so we need CSVs. DAG iterates over all Excel files one by one (loading the whole file into memory), writes the content into a CSV line by line. 
    
    There are multiple possible improvements with 2 main ones:
    
    a) add iterating through multiple files and mb reading over .xslx by chunks with pandas
    
    b) load .xlsx directly to snowflake stage and read excel directly with snowpark, thus moving the compute load to a snowflake server.
    
2. Airflow loads files to Snowflake Stage
    
    Loading the whole files with ‘Put’ via Snowflake Operator to Snowflake stage. According to [documentation](https://docs.snowflake.com/en/sql-reference/sql/put#optional-parameters) snowflake can split large files in chunks automatically with default concurrency = 4. Available up to 99 depending on real data size.
    
3. Airflow runs SQL on Snowflake to copy data from CSV to relevant tables in RAW schema
    
    It trancates each table and loads new data after. This is a convenient implementation for assignment purposes, yet in production it should be “adding” data, not re-writing, so trancation function would be removed, some queries to check last loaded data would be added to load only new batches to avoid data duplication.
    
4. DBT staging (sumup_dbt/models/staging)
    
    DBT references source data from SUMUP.RAW schema into a staging layer (SUMUP.STAGING), drops unnecessary columns, casts most lightweight datatypes (except timestamp, it’s kept for potential analyses) and adjust names:
    
    `transaction.amount` > `amount_eurocents` (good practise to know what’s the measure)
    
    `device.type` > `type_id` (sine it’s a number)
    
    Timestamp data type is used across all analyses, as it might be useful to keep them for some product analytics. Yet if it’s not needed, date format could be used to reduce storage space and have smaller/faster indexes. (DW optimization seems not to be in the scope of the assignment)
    
    Staging layer also has a lot of tests to make sure there is no duplicated + some custom tests for product_sku (field length) and status fields (acceptable values) to avoid unexpected errors.
    
    Product_sku correction:
    
    Product_sku field is corrected at staging layer with expectedly lightweight operation before aggregating transactions, because: (a) we still need transaction-level details for some analysis + aggregation would make table useless for store/device analyses (b) If aggregated by multiple fields, including store_id and device_id, the data would not shrink in size significantly (subject to test with real size data) > not much performance gain for cleaning product_sku, yet it would double the size of data stored, as we would still have to store transaction-level data.
    
5. DBT modelling (sumup_dbt/models/marts)
    
    DBT calculates intermediate, fact (transactions) and dimensional (stores, devices) models.
    
    Transactions are materialized as view and reference intermediate transactions model which is a table. This is to enable other dim models reference transactions as an intermediate model and not a fact model that is already served to a user (as a matter of good practice). `fct_transactions` being a view should not affect performance as snowflake should be able to pass filters over views (subject to tests).
    
    Transaction model is slightly de-normalized with `device_type_id` and `store_id`, as these are frequently used fields that don’t take a lot of memory. Without them, we would have to consistently perform joins of moderate stores/devices with a large transactions table.
    
    Dim_stores reference int_transactions model to correct `stores.created_at` field with the first transaction made at a store.
    
    Product dimensional model is not built since, apart from product sku, all product data is random and meaningless 
    
6. Analyses (sumup_dbt/models/materialzied_analyses)
    
    Analyses only reference marts models.
    
    Analyses are materialized as tables and kept in DBT so that they build analytical outcomes to be checked in Snowflake. It is a quick approach to facilitate query writing, keep all querries together with dbt project and check the outputs directly in snowflake (as an alternative to DBT Power User extension for VS Code). 
    

### Data model

Classic star-schema de-normalized due to frequent needs to join stores and transactions and low weight of their ids. More normalized “snowflake” approach (stores only connected to devices, but not to transactions) would not gain much clarity or storage space, but would decrease query performance for frequent analyses.

Simplified DBT flow

<img width="413" alt="Screenshot 2023-08-26 141413" src="https://github.com/z-ferrum/data_pipeline/assets/23115279/2cbff114-fd00-451e-99cf-37cc36fd4806">

Simplified data model

<img width="521" alt="Screenshot 2023-08-26 142729" src="https://github.com/z-ferrum/data_pipeline/assets/23115279/b57a435b-0106-448a-b4a4-fe2ed40e123e">
