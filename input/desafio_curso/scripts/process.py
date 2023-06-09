from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive -dfs
df_vendas = spark.sql("select * from desafio_curso.TBL_VENDAS")
df_clientes = spark.sql("select * from desafio_curso.TBL_CLIENTES")
df_endereco = spark.sql("select * from desafio_curso.TBL_ENDERECO")
df_regiao = spark.sql("select * from desafio_curso.TBL_REGIAO")
df_divisao = spark.sql("select * from desafio_curso.TBL_DIVISAO")

df_vendas.createOrReplaceTempView("vendas")
df_clientes.createOrReplaceTempView("clientes")
df_endereco.createOrReplaceTempView("endereco")
df_regiao.createOrReplaceTempView("regiao")
df_divisao.createOrReplaceTempView("divisao") //

df1 = df_vendas.join(df_clientes,df_vendas.customerkey == df_clientes.customerkey, "inner")
df2 = df1.join(df_endereco,df1.address_number == df_endereco.address_number, "left")
df3 = df2.join(df_regiao, df2.region_code == df_regiao.region_code, "left")
df4 = df3.join(df_divisao, df3.division == df_divisao.division, "left")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

# criando o fato
ft_vendas = []

ft = ft_vendas = spark.sql("""
select customerkey, discount_amount, invoice_number, customerkey, item_class, item_number, item, line_number,list_prince,
order_number, sales_amount, sales_amount_based_on_list_price, sales_cost_amount, sales_margin_amount, sales_price,
sales_quantity, sales_rep
from ft_vendas

""")
//aa

['customerkey',
 'discount_amount',
 'invoice_number',
 'customerkey',
 'item_class',
 'item_number',
 'item',
 'line_number',
 'list_prince',
 'order_number',
 'sales_amount',
 'sales_amount_based_on_list_price',
 'sales_cost_amount',
 'sales_margin_amount',
 'sales_price',
 'sales_quantity',
 'sales_rep']

 a



#criando as dimensões
df_clientess = df_1 = spark.sql("""
select  business_family, business_unity, customer, customerkey, customer_type, 
   line_of_business, phone, search_type 
   from dim_cli 


""")

['business_family',
 'business_unity',
 'customer',
 'customerkey',
 'customer_type',
 'line_of_business',
 'phone',
 'search_type']

 dim_temp = df_vendas = spark.sql("""
select actual_delivery_date, datekey, invoice_date, promised_delivery_date
from dim_vendas


""")

dim_temp = df_vendas = spark.sql("""
select actual_delivery_date, datekey, invoice_date, promised_delivery_date
from dim_vendas


""")

salvar_df(df_vendas,file='dim_temp')
salvar_df(df_clientes, 'df_clientess')
salvar_df(df_vendas,'ft')

dim_loc = df_endereco = spark.sql("""
   select address_number, city, country , customer_address_1, customer_address_2, customer_address_3, customer_address_4,
   state, zip_code
   from dim_localidade
   


""")

salvar_df(df_endereco, 'dim_loc')

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(dim_clientes, 'dimclientes')

~