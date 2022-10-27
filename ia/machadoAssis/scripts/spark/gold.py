#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: gold.py                                                            #
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
import pandas as pd
import openpyxl
from openpyxl import Workbook
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
path_gold = content["path_gold"]
mod = content["mod"]
#variables
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

#extract session
df = spark.read.parquet(f"{silver_path}collection_machado_assis").createOrReplaceTempView("df")
df = spark.sql("SELECT * FROM df")

#metrics
df_m = spark.sql("""
select * from (
    select title as books, word, ents from df
)
PIVOT(COUNT(word) for ents in ('ORG' as organizations, 'PER' as characters,'LOC' as places,'MISC' as others))
""")

#load


df.write.option("header", "true").csv(f"{path_gold}collection_machado_assis.csv")
df_m.write.option("header", "true").csv(f"{path_gold}abalyzed.csv")