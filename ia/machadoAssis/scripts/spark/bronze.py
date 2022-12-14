#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: bronze.py                                                          #
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
mod = content["mod"]
# variables

#variables
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

#extract session
mao = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}a_mao_e_a_luva.csv")
mao.createOrReplaceTempView("mao")

casa = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}casa_velha.csv")
casa.createOrReplaceTempView("casa")

dom = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}dom_casmurro.csv")
dom.createOrReplaceTempView("dom")

esau = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}esau_e_jaco.csv")
esau.createOrReplaceTempView("esau")

helena = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}helena.csv")
helena.createOrReplaceTempView("helena")

iaia = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}iaiá_garcia.csv")
iaia.createOrReplaceTempView("iaia")

aires = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}memorial_de_aires.csv")
aires.createOrReplaceTempView("aires")

memorias = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}memorias_postumas_de_bras_cubas.csv")
memorias.createOrReplaceTempView("memorias")

quincas = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}quincas_borba.csv")
quincas.createOrReplaceTempView("quincas")

ressur = spark.read.option("delimiter", ';').option("header", "true").csv(f"{csv_path}ressurreicao.csv")
ressur.createOrReplaceTempView("ressur")


#deplucation
mao = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM mao WHERE title = 'a_mao_e_a_luva' and ents != 'null')""")

casa = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM casa WHERE title = 'casa_velha' and ents != 'null')""")

dom= spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM dom WHERE title = 'dom_casmurro' and ents != 'null')""")

esau = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM esau WHERE title = 'esau_e_jaco' and ents != 'null')""")

helena = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM helena WHERE title = 'helena' and ents != 'null')""")

iaia = spark.sql("""SELECT * FROM
(SELECT *,  DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM iaia WHERE title = 'iaiá_garcia' and ents != 'null')""")

aires = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM aires WHERE title = 'memorial_de_aires' and ents != 'null')""")

memorias = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM memorias WHERE title = 'memorias_postumas_de_bras_cubas' and ents != 'null')""")

quincas = spark.sql("""SELECT * FROM
(SELECT *, DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM quincas WHERE title = 'quincas_borba' and ents != 'null')""")

ressur = spark.sql("""SELECT * FROM
(SELECT *,DATE_FORMAT(current_date(),'yyyyMMdd') AS date_processed, ROW_NUMBER() OVER (PARTITION BY title,  word, ents ORDER BY _id) AS row_id FROM ressur WHERE title = 'ressurreicao' and ents != 'null')""")


#load process
mao.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}a_mao_e_a_luva")
casa.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}casa_velha")
dom.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}dom_casmurro")
esau.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}esau_e_jaco")
helena.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}helena")
iaia.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}iaiá_garcia")
aires.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}memorial_de_aires")
memorias.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}memorias_postumas_de_bras_cubas")
quincas.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}quincas_borba")
ressur.write.mode(mod).format("parquet").partitionBy("date_processed").save(f"{bronze_path}ressurreicao")
