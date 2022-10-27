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

#variables
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

#extract session
mao = spark.read.parquet("ia/machadoAssis/dw/bronze/a_mao_e_a_luva/").createOrReplaceTempView("mao")

casa = spark.read.parquet("ia/machadoAssis/dw/bronze/casa_velha/").createOrReplaceTempView("casa")

dom = spark.read.parquet("ia/machadoAssis/dw/bronze/dom_casmurro/").createOrReplaceTempView("dom")

esau = spark.read.parquet("ia/machadoAssis/dw/bronze/esau_e_jaco/").createOrReplaceTempView("esau")

helena = spark.read.parquet("ia/machadoAssis/dw/bronze/helena/").createOrReplaceTempView("helena")

iaia = spark.read.parquet("ia/machadoAssis/dw/bronze/iaiá_garcia/").createOrReplaceTempView("iaia")

aires = spark.read.parquet("ia/machadoAssis/dw/bronze/memorial_de_aires/").createOrReplaceTempView("aires")

memorias = spark.read.parquet("ia/machadoAssis/dw/bronze/memorias_postumas_de_bras_cubas/").createOrReplaceTempView("memorias")

quincas = spark.read.parquet("ia/machadoAssis/dw/bronze/quincas_borba/").createOrReplaceTempView("quincas")

ressur = spark.read.parquet("ia/machadoAssis/dw/bronze/ressurreicao/").createOrReplaceTempView("ressur")


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
df.write.mode("overwrite").format("parquet").partitionBy("date_transform").save("ia/machadoAssis/dw/silver/collection_machado_assis")
