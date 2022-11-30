import xmljson
import os
import json
import logging
import xml.etree.ElementTree as ET
from os import listdir
from os.path import isfile, join
from xmljson import parker as pk
from xml.etree.ElementTree import Element, tostring
from xml.etree.ElementTree import fromstring
from json import dumps

def json_converter():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_jsons.log')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    c = 0
    mypath = "/newvolume/xmls"
    only_files = [file for file in listdir(mypath) if file.endswith(".xml")]
    for file in only_files:
        path_to_file = os.path.join(mypath, file)
        root = ET.parse(path_to_file).getroot()
        xml_string = tostring(root)
        xml_dict = pk.data(fromstring(xml_string))
        a = dumps(xml_dict)
        final_string = json.loads(a)
        small_filename = '{}.json'.format(file)
        file = os.path.join(mypath, small_filename)
        smallfile = open(file, "w")
        json.dump(final_string, smallfile)
        c += 1
        print('\rfilenum-', c, end='', flush=True)
        smallfile.close()

    count = 0
    files_in_directory = os.listdir(mypath)
    filtered_files = [file for file in files_in_directory if file.endswith(".json")]
    for file in filtered_files:
        count += 1
        logger.info(f"file- {file}, {count}")







