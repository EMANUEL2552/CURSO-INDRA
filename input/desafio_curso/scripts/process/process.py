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

# Criando dataframes diretamente do Hive
df_vendas = spark.sql("select * from desafio_curso.TBL_VENDAS")
df_clientes = spark.sql("select * from desafio_curso.TBL_CLIENTES")
df_endereco = spark.sql("select * from desafio_curso.TBL_ENDERECO")
df_regiao = spark.sql("select * from desafio_curso.TBL_REGIAO")
df_divisao = spark.sql("select * from desafio_curso.TBL_DIVISAO")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

# criando o fato
ft_vendas = []

#criando as dimensões
dim_clientes = []

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