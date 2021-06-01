## Major imports

import xmltodict
import json
import pandas as pd
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
from nested_lookup import nested_lookup



## The below code will fetch publication number,respective dates and title.
def data_for_dates():
    working_directory = r"F:\FINAL\xmls"
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data= pd.read_json(file)
            a = data.loc["publication-reference":"claim"]
            data_transpose = a.T
            data_transpose= data_transpose.loc["us-bibliographic-data-application":"us-bibliographic-data-application"]
            data_transpose = data_transpose.filter(regex="reference|invention-title")
            data_transpose.to_csv('Patents_publication_dates_data_2016.csv', mode='a', header=False)

        except KeyError:
            print("Showing as key error:",file)
        except ValueError:
            print("Showing as Value error:",file)


## Below Code will fetch CPC class, sublcass and patent country.
def data_for_cpc_classes():
    working_directory = r"F:\FINAL\xmls"
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data= pd.read_json(file)
            p= data["us-bibliographic-data-application"]['publication-reference']
            publication_number = nested_lookup('doc-number', p)

            cpc= data["us-bibliographic-data-application"]['classifications-cpc']
            patent_section = nested_lookup('section', cpc)
            patent_class = nested_lookup('class', cpc)
            patent_subclass = nested_lookup('subclass', cpc)
            patent_main_group = nested_lookup('main-group', cpc)
            patent_subgroup = nested_lookup('subgroup', cpc)
            patent_country = nested_lookup('country', cpc)
            combined_data_cpc = publication_number + [patent_section] + [patent_class] + [patent_subclass] + [patent_main_group] + [patent_subgroup] + [patent_country]
            df= pd.DataFrame([combined_data_cpc], columns = ["publication_number","patent_section","patent_class","patent_subclass","patent_main_group","patent_subgroup","patent_country"])

            df.to_csv('Patents_cpc_data_2016.csv', mode='a', header=False)

        except KeyError:
            print("Showing as key error:",file)
        except ValueError:
            print("Showing as Value error:",file)


## Below Code will fetch patent's inventor names and their respective city and country.
def data_for_inventors():
    working_directory = r"F:\FINAL\xmls"
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data= pd.read_json(file)
            p= data["us-bibliographic-data-application"]['publication-reference']
            publication_number = nested_lookup('doc-number', p)

            inventors= data["us-bibliographic-data-application"]['us-parties']
            #applicant = nested_lookup('orgname', inventors)
            first_name = nested_lookup('first-name', inventors)
            last_name = nested_lookup('last-name', inventors)
            inventors_name = [i +" "+ j for i, j in zip(first_name, last_name)]
            address = nested_lookup('address', inventors)
            address = address[1:]
            city = nested_lookup("city", address)
            country = nested_lookup("country", address)

            combined_data_inventors = publication_number + [inventors_name]+ [city] +[country]
            df= pd.DataFrame([combined_data_inventors], columns = ["publication_number","inventors_name","city","country"])

            df.to_csv('Patents_inventors_data_2016.csv', mode='a', header=False)

        except KeyError:
            print("Showing as key error:",file)
        except ValueError:
            print("Showing as Value error:",file)


### Code to extract description of the patent for first 10 lines
def data_for_description():
    working_directory = r"F:\FINAL\xmls"
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data= pd.read_json(file)
            p= data["us-bibliographic-data-application"]['publication-reference']
            publication_number = nested_lookup('doc-number', p)

            description = data["description"]["p"]
            for i in range(len(description) - 1, -1, -1):
                if isinstance(description[i], dict):
                    del description[i]

            description = description[0:10]
            combined_data_des = publication_number + [description]
            df= pd.DataFrame([combined_data_des], columns = ["publication_number","description"])

            df.to_csv('Patents_description_2016.csv', mode='a', header=False)

        except KeyError:
            print("Showing as key error:",file)
        except ValueError:
            print("Showing as Value error:",file)

### Exceptions are present for some parsed json files , further xml exploration required for getiing more data