#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: read_pdf.py                                                           #
                        #   created: 2022-03-10                                                          #
                        #   system: Darwin                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import PyPDF2
import os
import re
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
txt_path = content["txt_path"]


for root, dirs, files in os.walk(pdf_path):
    for file in tqdm(files):
        pdf_file = open(f'{pdf_path}/{file}','rb')
        fl = PyPDF2.PdfFileReader(pdf_file,strict=False)
        title = str(fl.documentInfo['/Title']).replace(r'file://E:\obras\romances\ROMANCE, ','').replace('.htm','').split(',')
        if title[0].replace(' ','_').lower() == 'microsoft_word_-_romance':
            title = title[1].replace(' ','_').lower()
            txt_out = open(f"{txt_path}{title[1:]}.txt",'a')
            content = fl.getNumPages()
            for i in range(content):
                page = fl.getPage(i)
                script = page.extractText()
                txt_out.write(script)
        else:
            title = title[0].replace(' ','_').lower()
            txt_out = open(f"{txt_path}{title}.txt",'a')
            content = fl.getNumPages()
            for i in range(content):
                page = fl.getPage(i)
                script = page.extractText()
                txt_out.write(script)









