#!/bin/bash


echo "Digite o nome da pasta a ser criada:"
read nome_pasta


hdfs dfs -mkdir -p datalake/raw/"$nome_pasta"

hdfs dfs -copyFromLocal "input/desafio_curso/raw/${nome_pasta}.csv" "datalake/raw/${nome_pasta}/"

