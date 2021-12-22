# Data Size Scaling

Question: Imagine doing the ETL ingestion and table creation on the full TPC-DS dataset

- What are Issues that could occur in spark
- Where would it occur?
- Why would it occur?
- What are the most common bottlenecks when increasing the data scale?
- How should you change the code in other to process such a large amount of data?

## What are Issues that could occur in spark

- **OutOfMemory Error**: this can occur when loading very large file (e.g 1TB) into memory
- **Substantial Increase in wait time for I/O processing**
- **Substantial Increase Garbage Collection**: This happens as a result of the in-memory nature of spark

## Where would it occur?

- Ingestion Stage: while loading the data from a particular storage system (e.g s3)
- Loading/Storage Stage: this happens when saving the data into a table or running a query to derive new metrics

## Why would it occur?

- Data size has increased hence the reason, Don't stress me

## What are the most common bottlenecks when increasing the data scale?

- Re-writing ETL codes: if a pipeline as been designed in a rigid format where modififcations are hard to implement, it will be difficult for the same pipeline to adapt to increase in data size especially if it was not designed to handle one. This can be solved by desinging pipelines that are flexible and gives room for modification and additions.
- Changing Architecture:

## How should you change the code in other to process such a large amount of data?

- Include Partitions on the data using the Primary Key Column during data ingestion
- I will add a layer to the ingestion code to check for duplicates and remove them if exists
- I will use a more normalized data model e.g (a 3rd Normal Form)
- Include Materialized Views for queries that are ran frequently in the Data warehouse to cut down processing time
