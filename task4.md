# Data Size Scaling

Question: Imagine doing the ETL ingestion and table creation on the full TPC-DS dataset

- What are Issues that could occur in spark
- Where would it occur?
- Why would it occur?
- What are the most common bottlenecks when increasing the data scale?
- How should you change the code in other to process such a large amount of data?

## What are Issues that could occur in spark

- **OutOfMemory Error**
- **Substantial Increase in wait time for I/O processing**
- **Substantial Increase Garbage Collection**

## Where would it occur?

- Ingestion Stage: while loading the data from a particular storage system (e.g s3)
- Loading/Storage Stage: this will happen when saving the data into a table or running a query to derive metrics

## Why would it occur?

- *Substantial Increase Garbage Collection*: This will happen as a result of the in-memory (how spark process data) nature of spark
- *OutOfMemory Error*: this can occur when loading very large file (e.g 1TB) into memory

## What are the most common Spark bottlenecks when increasing the data scale?

- Resource Management: Spark doesn’t come batteries included when it comes to setting up Spark contexts and configuration. Even for simple tasks, you have to end up setting these things up
- Out of Memory Error

## How should you change the code in other to process such a large amount of data?

- Include Partitions on the data using the Primary Key Column during data ingestion
- I will add a layer to the ingestion code to check for duplicates and remove them if exists
- I will use a more normalized data model e.g (a 3rd Normal Form)
- Include Materialized Views for queries that are ran frequently in the Data warehouse to cut down processing time
