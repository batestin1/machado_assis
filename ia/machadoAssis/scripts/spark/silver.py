#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: silver.py                                                          #
                        #   created: 2022-03-10                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
import os
import re
import json


path = "ia/parameters/parameters.json"
parameter=open(path)
data = parameter.read()
content = json.loads(data)

#variables recover
res = content["requests"]
html_parser = content["features"]
comp = content["compile"]
link_1 = content["link"]
link_2 = content["link2"]
pdf_path = content["pdf_path"]
txt_path = content["txt_path"]
memories = content["memories"]
csv_path = content["csv_path"]
bronze_path = content["bronze_path"]
silver_path = content["silver_path"]
mod = content["mod"]

#variables
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

#extract session
mao = spark.read.parquet(f"{bronze_path}a_mao_e_a_luva/").createOrReplaceTempView("mao")

casa = spark.read.parquet(f"{bronze_path}casa_velha/").createOrReplaceTempView("casa")

dom = spark.read.parquet(f"{bronze_path}dom_casmurro/").createOrReplaceTempView("dom")

esau = spark.read.parquet(f"{bronze_path}esau_e_jaco/").createOrReplaceTempView("esau")

helena = spark.read.parquet(f"{bronze_path}helena/").createOrReplaceTempView("helena")

iaia = spark.read.parquet(f"{bronze_path}iaiá_garcia/").createOrReplaceTempView("iaia")

aires = spark.read.parquet(f"{bronze_path}memorial_de_aires/").createOrReplaceTempView("aires")

memorias = spark.read.parquet(f"{bronze_path}memorias_postumas_de_bras_cubas/").createOrReplaceTempView("memorias")

quincas = spark.read.parquet(f"{bronze_path}quincas_borba/").createOrReplaceTempView("quincas")

ressur = spark.read.parquet(f"{bronze_path}ressurreicao/").createOrReplaceTempView("ressur")


#wr.writerow(['_id','title','word','ents'])
#mergin
df = spark.sql(""" SELECT monotonically_increasing_id() as id, title,  word, ents,  DATE_FORMAT(current_date(),'yyyyMMdd') as date_transform FROM 
(
    SELECT mao.title, mao.word, mao.ents FROM mao mao
    UNION
    SELECT casa.title, casa.word, casa.ents FROM casa casa
    UNION
    SELECT dom.title, dom.word, dom.ents FROM dom dom
    UNION
    SELECT esau.title, esau.word, esau.ents FROM esau esau
    UNION
    SELECT helena.title, helena.word, helena.ents FROM helena helena
    UNION
    SELECT iaia.title, iaia.word, iaia.ents FROM iaia iaia
    UNION
    SELECT aires.title, aires.word, aires.ents FROM aires aires
    UNION
    SELECT memorias.title, memorias.word, memorias.ents FROM memorias memorias
    UNION
    SELECT quincas.title, quincas.word, quincas.ents FROM quincas quincas
    UNION
    SELECT ressur.title, ressur.word, ressur.ents FROM ressur ressur

)

""")






#load process
df.write.mode(mod).format("parquet").partitionBy("date_transform").save(f"{silver_path}collection_machado_assis")
