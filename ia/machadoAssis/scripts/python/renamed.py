#!/usr/local/bin/python3
                        
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: renamed.py                                                         #
                        #   created: 2022-03-10                                                          #
                        #   system: Windows                                                              #
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

main_folder = r'ia/machadoAssis/dl/pdf'

num = 0
for root, dirs, files in os.walk(main_folder):
    for file in tqdm(files):
      num = num + 1
      file_out = f"book_{num}.pdf"
      old = os.path.join(main_folder,file)
      new = os.path.join(main_folder,file_out)
      shutil.move(old,new)
