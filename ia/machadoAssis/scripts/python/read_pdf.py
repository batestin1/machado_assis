#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: read_pdf.py                                                           #
                        #   created: 2022-03-10                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import PyPDF2
import os
import re
from tqdm import tqdm


main_folder = r'ia/machadoAssis/dl/pdf'
for root, dirs, files in os.walk(main_folder):
    for file in tqdm(files):
        pdf_file = open(f'{main_folder}/{file}','rb')
        fl = PyPDF2.PdfFileReader(pdf_file,strict=False)
        title = str(fl.documentInfo['/Title']).replace(r'file://E:\obras\romances\ROMANCE, ','').replace('.htm','').split(',')
        if title[0].replace(' ','_').lower() == 'microsoft_word_-_romance':
            title = title[1].replace(' ','_').lower()
            txt_out = open(f"ia/machadoAssis/dl/txt/{title[1:]}.txt",'a')
            content = fl.getNumPages()
            for i in range(content):
                page = fl.getPage(i)
                script = page.extractText()
                txt_out.write(script)
        else:
            title = title[0].replace(' ','_').lower()
            txt_out = open(f"ia/machadoAssis/dl/txt/{title}.txt",'a')
            content = fl.getNumPages()
            for i in range(content):
                page = fl.getPage(i)
                script = page.extractText()
                txt_out.write(script)









