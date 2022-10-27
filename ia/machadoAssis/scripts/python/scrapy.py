#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: scrapy.py                                                          #
                        #   created: 2022-03-10                                                          #
                        #   system: Darwin                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import requests
from bs4 import BeautifulSoup
import re
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


url = requests.get(res)
soup = BeautifulSoup(url.content, features=html_parser)
num = 0
for i in tqdm(soup.find_all(href=re.compile(comp)),colour='red'):
    a = str(i)
    threat=a.replace('<a href=', '').replace('title="Download"><img class="efeito" src="/templates/machado/img/iconPdf.png"/></a>', '').replace('"','')
    link = threat.replace(link_1,link_2).replace(' ','\n')
    fileout = pdf_path
    response = requests.get(link)
    if response.status_code == 200:
        arquivo_path = os.path.join(fileout, os.path.basename(link))
        with open(arquivo_path, 'wb') as f:
            f.write(response.content)
    else:
        print("Download failed")
