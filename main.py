import xmltodict
import json
import requests
from bs4 import BeautifulSoup
import requests, zipfile, io
import subprocess
from subprocess import call
import os
import xmljson
from xmljson import parker, Parker
from xml.etree.ElementTree import fromstring
from json import dumps
import patents_code_download_extract_split as download
import patent_xmljsonconvert as converter
import patents_dataframe_preparation as dataframes
import xml.etree.ElementTree as ET
from os import listdir
from os.path import isfile, join
from xmljson import parker as pk
from xml.etree.ElementTree import Element, tostring
from xml.etree.ElementTree import fromstring
from json import dumps
from sqlalchemy import create_engine
import pymysql
import logging
import time

start= time.time()

### Object calling download py function to download and extract all files
print("Downloading..\n")
download.download_xml()
print("Done !\n")

## Object calling split py to split xml files
print("xmls splitting started..\n")
working_directory = "/newvolume/xmldownloads"
for subdir, dirs, files in os.walk(working_directory):
    for file in files:
        if file.endswith('.xml'):
            folder = os.path.join(working_directory, file)
            download.split_large_xml(folder)

print("\nsplitting complete\n")

# Code to delete the compressed xml file which got downloaded.
working_directory = "/newvolume/xmldownloads"
files_in_directory = os.listdir(working_directory)
filtered_files = [file for file in files_in_directory if file.endswith(".xml")]
for file in filtered_files:
	path_to_file = os.path.join(working_directory, file)
	os.remove(path_to_file)

print("Deleted the compressed xml file\n")

## Object calling converter py to convert all xml to jsons
print("json conversion started..\n")
converter.json_converter()

## Code to delete xmls files
print("\ndeleting splitted xmls..\n")
working_directory = "/newvolume/xmls"
files_in_directory = os.listdir(working_directory)
filtered_files = [file for file in files_in_directory if file.endswith(".xml")]
for file in filtered_files:
	path_to_file = os.path.join(working_directory, file)
	os.remove(path_to_file)


print("Data Extraction started..\n")


## Object call for dataframes
#dataframes.data_for_dates()
#print("Dates data collection completed..\n")

#dataframes.data_for_cpc_classes()
#print("cpc class data collection complected..\n")

dataframes.data_for_ipc_classes()
print("ipc class data collection completed..\n ")

#dataframes.data_for_inventors()
#print("inventors data collection completed..\n")

dataframes.data_for_applicants()
print("applicants data collection completed..\n")

dataframes.data_for_abstract()
print("abstract data collection completed..\n")

## Code to delete json files
working_directory = "/newvolume/xmls"
files_in_directory = os.listdir(working_directory)
filtered_files = [file for file in files_in_directory if file.endswith(".json")]
for file in filtered_files:
	path_to_file = os.path.join(working_directory, file)
	os.remove(path_to_file)

print("Jsons deleted & Completed! \n ")
end= time.time()
print(f"Runtime of the program is {(end - start)/60} minutes")
