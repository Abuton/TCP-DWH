create view total_quantites as (
    select sum(ss_sales_price) as total_sales from store_sales
);