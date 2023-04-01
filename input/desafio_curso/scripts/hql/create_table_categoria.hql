CREATE EXTERNAL TABLE IF NOT EXISTS TBL_VENDAS ( 
actual_delivery_date string,
customerKey string,
dateKey string,
discount_amount string,
invoice_date string,
invoice_number string,
item_class string,
item_number string,
item string,
line_number string,
list_prince string,
order_number string,
promised_delivery_date string,
sales_amount string,
sales_amount_based_on_list_price string,
sales_cost_amount string,
sales_margin_amount string,
sales_price string,
sales_quantity string,
sales_rep string,
u_m string
    )
COMMENT 'TBL_VENDAS'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS TBL_DIVISAO ( 
division string,
division_name string

    )
COMMENT 'TBL_DIVISAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS TBL_ENDERECO ( 
address_number string,
city string,
country string,
customer_address_1 string,
customer_address_2 string,
customer_address_3 string,
customer_address_4 string,
state string,
zip_code string

    )
COMMENT 'TBL_ENDERECO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/endereco/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS TBL_REGIAO ( 
region_code string,
region_name string


    )
COMMENT 'TBL_REGIAO'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS TBL_CLIENTES ( 
address_number string,
business_family string,
business_unity string,
customer string,
customerKey string,
customer_type string,
division string,
line_of_business string,
phone string,
region_code string,
regional_sales_mgr string,
search_type string


    )
COMMENT 'TBL_CLIENTES'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");