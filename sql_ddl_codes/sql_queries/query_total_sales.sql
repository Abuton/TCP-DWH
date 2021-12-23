create view total_sales as (
    select sum(ss_quantity) as total_quantity from store_sales
);