import xmljson
import os
import json
import xml.etree.ElementTree as ET
from os import listdir
from os.path import isfile, join
from xmljson import parker as pk
from xml.etree.ElementTree import Element, tostring
from xml.etree.ElementTree import fromstring
from json import dumps

def json_converter():
    mypath = r"F:\FINAL\xmls"
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
        smallfile.close()








