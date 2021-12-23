# Choice of Model

I chose to use the Star Schema Data Model Design for the business problem with the `store_sales` table used as the fact table while other tables (customer, store, item, date_dim) as the dimensions in the data model. I have added two additional table to normalize the data in the data warehouse. These additional tables are (address, company). The data model design can be further normalized by creating additional tables in other to reduce data redundancy, but trying to satisfy a requirement that says *the BI team wants a data model, where they can minimize the number of SQL joins*, hence the reason why i stopped the data normalization at this stage.

The fields/columns that have been included are based on the metrics that the BI team wants to get from the data warehouse as well as the additional information that might be required.

Star Schema would be best because of the following reasons;

- **Simpler Queries**

Join logic of star schema is quite cinch in comparison to other join logic which are needed to fetch data from a transactional schema that is highly normalized. Hence to get metrics that invlove 2 or more tables will be easy and less computationally expensive

- **Simplified Business Reporting Logic**

In comparison to a transactional schema that is highly normalized, the star schema makes simpler common business reporting logic, such as as-of reporting and period-over-period. Therefore, answering questions like customer habits, yearly comparison etc will be done with less use of computation power.

Also Star Schema is relatively simple to use for the given data.
