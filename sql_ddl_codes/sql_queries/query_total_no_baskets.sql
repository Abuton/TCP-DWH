create view total_no_baskets as (
    select count(ss_ticket_number) as total_basket from store_sales
);