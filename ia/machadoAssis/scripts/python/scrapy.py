#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: scrapy.py                                                          #
                        #   created: 2022-03-10                                                          #
                        #   system: Windows                                                              #
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


url = requests.get("http://machado.mec.gov.br/obra-completa-lista/itemlist/category/23-romance")
soup = BeautifulSoup(url.content, features="html.parser")
num = 0
for i in tqdm(soup.find_all(href=re.compile("^/obra-completa-lista/item/download/")),colour='red'):
    a = str(i)
    threat=a.replace('<a href=', '').replace('title="Download"><img class="efeito" src="/templates/machado/img/iconPdf.png"/></a>', '').replace('"','')
    link = threat.replace('/obra-completa-lista/','http://machado.mec.gov.br/obra-completa-lista/').replace(' ','\n')
    fileout = 'ia/machadoAssis/dl/pdf/'
    response = requests.get(link)
    if response.status_code == 200:
        arquivo_path = os.path.join(fileout, os.path.basename(link))
        with open(arquivo_path, 'wb') as f:
            f.write(response.content)
    else:
        print("Download failed")
