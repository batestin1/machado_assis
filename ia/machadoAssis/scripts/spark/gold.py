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
#variables
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

#extract session
df = spark.read.parquet("ia/machadoAssis/dw/silver/collection_machado_assis").createOrReplaceTempView("df")
df = spark.sql("SELECT * FROM df")

#metrics
df_m = spark.sql("""
select * from (
    select title as books, word, ents from df
)
PIVOT(COUNT(word) for ents in ('ORG' as organizations, 'PER' as characters,'LOC' as places,'MISC' as others))
""")

#load


df.write.option("header", "true").csv("ia/machadoAssis/dw/collection_machado_assis.csv")
df_m.write.option("header", "true").csv("ia/machadoAssis/dw/abalyzed.csv")