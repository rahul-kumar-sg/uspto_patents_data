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

### Object calling download py function to download and extract all files
# download.download_xml()

## Object calling split py to split xml files
# working_directory = r"F:\FINAL\xml_downloads"
# for subdir, dirs, files in os.walk(working_directory):
#     for file in files:
#         if file.endswith('.xml'):
#             folder = os.path.join(working_directory, file)
#             download.split_large_xml(folder)


## Object calling converter py to convert all xml to jsons
# converter.json_converter()

## Code to delete xmls files
working_directory = r"F:\FINAL\xmls"
files_in_directory = os.listdir(working_directory)
filtered_files = [file for file in files_in_directory if file.endswith(".xml")]
for file in filtered_files:
	path_to_file = os.path.join(working_directory, file)
	os.remove(path_to_file)

## Below code reads all json files and store it in a list so that we can iterate over file by file to get respective the data.
working_directory = r"F:\FINAL\xmls"
json_list = []
for subdir, dirs, files in os.walk(working_directory):
    for file in files:
        if file.endswith('.json'):
            json_list.append(file)

## Object call for dataframes
# dataframes.data_for_dates()
# dataframes.data_for_cpc_classes()
# dataframes.data_for_inventors()
# dataframes.data_for_description()

## Code to delete json files
working_directory = r"F:\FINAL\xmls"
files_in_directory = os.listdir(working_directory)
filtered_files = [file for file in files_in_directory if file.endswith(".json")]
for file in filtered_files:
	path_to_file = os.path.join(working_directory, file)
	os.remove(path_to_file)