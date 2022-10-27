#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: renamed.py                                                         #
                        #   created: 2022-03-10                                                          #
                        #   system: Darwin                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import requests
import re
import shutil
import os
from tqdm import tqdm
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

num = 0
for root, dirs, files in os.walk(pdf_path):
    for file in tqdm(files):
      num = num + 1
      file_out = f"book_{num}.pdf"
      old = os.path.join(pdf_path,file)
      new = os.path.join(pdf_path,file_out)
      shutil.move(old,new)
