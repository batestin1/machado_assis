#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: nlp_to_csv.py                                                      #
                        #   created: 2022-03-10                                                          #
                        #   system: Darwin                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#

import spacy
from spacy import displacy
from spacy.lang.pt.stop_words import STOP_WORDS #words in nonstop
import csv
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
memories = content["memories"]
csv_path = content["csv_path"]
# variables

npl = spacy.load("pt_core_news_sm") # to work with language portuguese
npl.max_length = memories # a number to process in memory


#logic for PLN
for root, dirs, files in os.walk(txt_path):
    for file in tqdm(files, colour='red'):
        f=open(f"{txt_path}{file}")
        content = f.read()
        document = npl(content)
        renamed = file.replace('.txt','')
        csvfile = open(f'{csv_path}{renamed}.csv','w')
        wr = csv.writer(csvfile,delimiter=';')
        wr.writerow(['_id','title','word','ents'])
        id=0
        for token in tqdm(document.ents,colour='red'):
            id = id + 1
            word = token.text if npl.vocab[token.text].is_stop else token.text
            ents = token.label_
            wr.writerow([id,renamed,word,ents])

