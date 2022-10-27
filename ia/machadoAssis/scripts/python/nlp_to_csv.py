#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: nlp_to_csv.py                                                      #
                        #   created: 2022-03-10                                                          #
                        #   system: Windows                                                              #
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

# variables
main_folder = r'ia/machadoAssis/dl/txt/'
npl = spacy.load("pt_core_news_sm") # to work with language portuguese
npl.max_length = 2108632 # a number to process in memory


#logic for PLN
for root, dirs, files in os.walk(main_folder):
    for file in tqdm(files, colour='red'):
        f=open(f"ia/machadoAssis/dl/txt/{file}")
        content = f.read()
        document = npl(content)
        renamed = file.replace('.txt','')
        csvfile = open(f'ia/machadoAssis/dl/csv/{renamed}.csv','w')
        wr = csv.writer(csvfile,delimiter=';')
        wr.writerow(['_id','title','word','ents'])
        id=0
        for token in tqdm(document.ents,colour='red'):
            id = id + 1
            word = token.text if npl.vocab[token.text].is_stop else token.text
            ents = token.label_
            wr.writerow([id,renamed,word,ents])

