#!/bin/sh

#####################################################################
#
# script name: start.sh
# created in: 21/19/22
# modified in: 09:14:41
#
# summary: inicia o projeto de ia do machado de assis
#                                               developed by: bates
#####################################################################

#variables
DATE=$(date)

if [ $?  -eq 0 ]
then
    clear
    echo "Starting the IA program of the literary works of Machado Assis"
    sleep 1
    echo "Running on date: $DATE"
    sleep 1
    echo "Checking dependencies"
    sleep 1
    if source ia/machadoAssis/env/env.sh
    then
        pip install --upgrade pip > /dev/null
        pip3 install -r ia/machadoAssis/env/requeriments.txt --upgrade > /dev/null
        sleep 1
        python -m spacy download pt  > /dev/null
        echo "Running the scripts"
        sleep 1
        echo "Downloading books"
        python3 ia/machadoAssis/scripts/python/scrapy.py  2> /dev/null
        sleep 1
        echo "Download completed!"
        sleep 1
        echo "Sorting and renaming books"
        python3 ia/machadoAssis/scripts/python/renamed.py 2> /dev/null
        sleep 1
        echo "Sorting and renaming completed!"
        sleep 1
        echo "Converting to TXT format"
        sleep 1
        python3 ia/machadoAssis/scripts/python/read_pdf.py 2> /dev/null
        sleep 1
        echo "Converting sucessed!"
        sleep 1
        echo "Processing natural language (PLN) to obtain book entities"
        python3 ia/machadoAssis/scripts/python/nlp_to_csv.py 2> /dev/null
        sleep 1
        echo "PLN completed!"
        sleep 1
        echo "Processing the data, stage 1"
        sleep 1
        python3 ia/machadoAssis/scripts/spark/bronze.py 2> /dev/null
        sleep 1
        echo "Processing the data, stage 2"
        python3 ia/machadoAssis/scripts/spark/silver.py 2> /dev/null
        sleep 1
        echo "Processing the data, stage 3"
        python3 ia/machadoAssis/scripts/spark/gold.py 2> /dev/null
        echo "Data successfully analyzed and saved on the way ´ia/machadoAssis/dw´"



    else
        echo "Something went wrong"
        fi
else
    echo "You ran the program wrong!"
fi



