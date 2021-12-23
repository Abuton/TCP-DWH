DROP TABLE IF EXISTS customer_text;
CREATE TABLE customer_text
(
    c_customer_sk             int not null,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string not null,
    c_last_name               string not null,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string not null,
    c_last_review_date        string
)
USING csv
OPTIONS(header "false", delimiter "|", path "TPCDS_GENDATA_DIR/customer")
;
DROP TABLE IF EXISTS customer;
CREATE TABLE customer USING PARQUET
AS (SELECT * FROM customer_text WHERE (mod(abs(HASH(c_customer_id)), 8) > 0))
LOCATION 'alluxio://master_hostname:port/customer';
;
DROP TABLE IF EXISTS customer_text;