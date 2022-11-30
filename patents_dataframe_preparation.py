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
from sqlalchemy import create_engine
import pymysql
import logging


## The below code will fetch publication number,respective dates and title.
def data_for_dates():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
            .format(user="admin", pw="d5Sj5U7lZqwNYsqRjhJI", db="datacollection"))

    conn = engine.connect()

    conn.execute("CREATE TABLE IF NOT EXISTS table_for_dates (publication_country varchar(10),\
                          publication_number varchar(100), publication_kind varchar(10), publication_date varchar(50),\
                          application_country varchar(10), application_number varchar(50), application_date varchar(50),\
                          invention_title varchar(500));")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = pd.read_json(file)
            data_transpose = data.T
            data_transpose = data_transpose.loc["us-bibliographic-data-application":"us-bibliographic-data-application"]
            data_transpose = data_transpose.filter(regex="reference|invention-title")
            data1 = data_transpose["publication-reference"]["us-bibliographic-data-application"]
            publication_country = nested_lookup('country', data1)
            publication_number = nested_lookup('doc-number', data1)
            publication_kind = nested_lookup('kind', data1)
            publication_date = nested_lookup('date', data1)
            data2 = data_transpose["application-reference"]["us-bibliographic-data-application"]
            application_date = nested_lookup('date', data2)
            application_number = nested_lookup('doc-number', data2)
            application_country = nested_lookup('country', data2)
            invention_title = [data_transpose["invention-title"]["us-bibliographic-data-application"]]

            zippedList = list(zip(publication_country, publication_number, publication_kind,
                                  publication_date, application_country, application_number, application_date,
                                  invention_title))

            df = pd.DataFrame(zippedList, columns=['publication_country', 'publication_number',
                                                   'publication_kind', 'publication_date', 'application_country',
                                                   'application_number',
                                                   'application_date', 'invention_title'])

            df.to_sql('table_for_dates', con=engine, if_exists='append', chunksize=1000, index=False)

            # df.to_csv('Patents_publication_dates_data.csv', mode='a', header=False)


        except ValueError:
            try:
                    data = json.load(open(file))
                    df = data["us-bibliographic-data-application"]
                    data1 = nested_lookup('publication-reference', df)
                    publication_country = nested_lookup('country', data1)
                    publication_number = nested_lookup('doc-number', data1)
                    publication_kind = nested_lookup('kind', data1)
                    publication_date = nested_lookup('date', data1)
                    data2 = nested_lookup('application-reference', df)
                    application_date = nested_lookup('date', data2)
                    application_number = nested_lookup('doc-number', data2)
                    application_country = nested_lookup('country', data2)
                    invention_title = nested_lookup('invention-title', df)

                    zippedList = list(zip(publication_country, publication_number, publication_kind,
                                  publication_date, application_country, application_number, application_date,
                                  invention_title))

                    df = pd.DataFrame(zippedList, columns=['publication_country', 'publication_number',
                                                   'publication_kind', 'publication_date', 'application_country',
                                                   'application_number',
                                                   'application_date', 'invention_title'])

                    df.to_sql('table_for_dates', con=engine, if_exists='append', chunksize=1000, index=False)

            except BaseException as error:
                    logger.error(f"info missing from dates {error} " + file)

        except BaseException as error:
            logger.error(f"check dates {error} " + file)

    conn.close()


## Below Code will fetch CPC class, sublcass and patent country.
def data_for_cpc_classes():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
            .format(user="admin", pw="d5Sj5U7lZqwNYsqRjhJI", db="datacollection"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS table_for_cpc (publication_number varchar(100),\
                             patent_section varchar(5), patent_class int(5), patent_subclass varchar(5),\
                             patent_main_group int(5), patent_subgroup int(5), patent_country varchar(5));")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = json.load(open(file))
            df = data["us-bibliographic-data-application"]
            if "classifications-cpc" in df:
                data1 = nested_lookup('publication-reference', df)
                publication_number = nested_lookup('doc-number', data1)

                cpc = nested_lookup('classifications-cpc', df)
                patent_section = nested_lookup('section', cpc)
                patent_class = nested_lookup('class', cpc)
                patent_subclass = nested_lookup('subclass', cpc)
                patent_main_group = nested_lookup('main-group', cpc)
                patent_subgroup = nested_lookup('subgroup', cpc)
                patent_country = nested_lookup('country', cpc)

            else:
                logger.error(f"info missing from cpc " + file)
                continue

            for i in range(len(patent_section) - 1):
                publication_number.append(publication_number[0])
            zippedList = list(
                zip(patent_section, patent_class, patent_subclass, patent_main_group, patent_subgroup,
                    patent_country))
            df = pd.DataFrame(zippedList,
                              columns=['patent_section', 'patent_class', 'patent_subclass', 'patent_main_group',
                                       'patent_subgroup', 'patent_country'])
            df.insert(0, "publication_number", publication_number, allow_duplicates=True)

            df.to_sql('table_for_cpc', con=engine, if_exists='append', chunksize=1000, index=False)
            # df.to_csv('Patents_cpc_sample.csv', mode='a', header=False)

        except BaseException as error:
            logger.error(f" corrupted {error} " + file)

    conn.close()

## Below Code will fetch IPC class, sublcass and patent country.
def data_for_ipc_classes():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
                                   .format(user="admin",pw="d5Sj5U7lZqwNYsqRjhJI",db="datacollection"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS table_for_ipc (publication_number varchar(100),\
                             patent_section varchar(5), patent_class int(5), patent_subclass varchar(5),\
                             patent_main_group int(5), patent_subgroup int(5), patent_country varchar(5));")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = json.load(open(file))
            df = data["us-bibliographic-data-application"]
            if "publication-reference" in df:
                pub = df.get("publication-reference")
                documentid = pub.get("document-id")
                publication_number = [documentid.get("doc-number")]  # publication number
            else:
                publication_number = [None]

            if "classifications-ipcr" in df:
                ipcr = df.get("classifications-ipcr")
                ipc = ipcr.get("classification-ipcr")
                if isinstance(ipc, list):
                    patent_section = [sub['section'] for sub in ipc]
                    patent_class = [sub['class'] for sub in ipc]
                    patent_subclass = [sub['subclass'] for sub in ipc]
                    patent_main_group = [sub['main-group'] for sub in ipc]
                    patent_subgroup = [sub['subgroup'] for sub in ipc]
                    gen = [sub['generating-office'] for sub in ipc]
                    patent_country = [sub['country'] for sub in gen]


                else:
                    patent_section = [ipc.get("section")]
                    patent_class = [ipc.get("class")]
                    patent_subclass = [ipc.get("subclass")]
                    patent_main_group = [ipc.get("main-group")]
                    patent_subgroup = [ipc.get("subgroup")]
                    gen = ipc.get("generating-office")
                    patent_country = [gen.get("country")]

            else:
                logger.error(f"info missing from ipc " + file)
                continue

            for i in range(len(patent_section) - 1):
                publication_number.append(publication_number[0])
            zippedList = list(zip(patent_section, patent_class, patent_subclass, patent_main_group, patent_subgroup, patent_country))
            df = pd.DataFrame(zippedList,columns=['patent_section', 'patent_class', 'patent_subclass', 'patent_main_group',
                                       'patent_subgroup', 'patent_country'])
            df.insert(0, "publication_number", publication_number, allow_duplicates=True)

            df.to_sql('table_for_ipc', con=engine, if_exists='append', chunksize=1000, index=False)
            # df.to_csv('Patents_cpc_sample.csv', mode='a', header=False)

        except BaseException as error:
            logger.error(f"corrupted {error} " + file)

    conn.close()



## Below Code will fetch patent's inventor names and their respective city and country.
def data_for_inventors():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
            .format(user="admin", pw="d5Sj5U7lZqwNYsqRjhJI", db="datacollection"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS table_for_inventors (publication_number varchar(100),\
                             inventors_name varchar(100), city varchar(100), country varchar(100));")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = json.load(open(file))
            df = data["us-bibliographic-data-application"]
            if "publication-reference" in df:
                pub = df.get("publication-reference")
                documentid = pub.get("document-id")
                publication_number = [documentid.get("doc-number")]  # docnumber
            else:
                publication_number = []

            if "parties" in df:
                parties = df.get("parties")
                if "inventors" in parties:
                    inventors = parties.get("inventors")
                    inventor = inventors.get("inventor")
                    if isinstance(inventor, list):
                        res = [sub['addressbook'] for sub in inventor]
                        if "last-name" in res[0]:
                            first_name = [sub['first-name'] for sub in res]  # firstname
                            last_name = [sub['last-name'] for sub in res]
                            inventors_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = [sub['address'] for sub in res]
                            city = [sub['city'] for sub in address]
                            country = [sub['country'] for sub in address]  # country
                        else:
                            inventors_name = []
                            city = []
                            country = []

                    elif isinstance(inventor, dict):
                        res = inventor.get("addressbook")
                        if "last-name" in res:
                            first_name = [res.get("first-name")]  # firstname
                            last_name = [res.get("last-name")]
                            inventors_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = res.get("address")
                            city = [address.get("city")]
                            country = [address.get("country")]  # country
                        else:
                            inventors_name = []
                            city = []
                            country = []

                    else:
                        inventors_name = []
                        city = []
                        country = []

                else:
                    inventors_name = []
                    city = []
                    country = []

            elif "us-parties" in df:
                parties = df.get("us-parties")
                if "inventors" in parties:
                    inventors = parties.get("inventors")
                    inventor = inventors.get("inventor")
                    if isinstance(inventor, list):
                        res = [sub['addressbook'] for sub in inventor]
                        if "last-name" in res[0]:
                            first_name = [sub['first-name'] for sub in res]  # firstname
                            last_name = [sub['last-name'] for sub in res]
                            inventors_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = [sub['address'] for sub in res]
                            city = [sub['city'] for sub in address]
                            country = [sub['country'] for sub in address]  # country
                        else:
                            inventors_name = []
                            city = []
                            country = []

                    elif isinstance(inventor, dict):
                        res = inventor.get("addressbook")
                        if "last-name" in res:
                            first_name = [res.get("first-name")]  # firstname
                            last_name = [res.get("last-name")]
                            inventors_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = res.get("address")
                            city = [address.get("city")]
                            country = [address.get("country")]  # country
                        else:
                            inventors_name = []
                            city = []
                            country = []

                    else:
                        inventors_name = []
                        city = []
                        country = []
                else:
                    inventors_name = []
                    city = []
                    country = []

            else:
                continue

            for i in range(len(inventors_name) - 1):
                publication_number.append(publication_number[0])
            zippedList = list(zip(inventors_name, city, country))
            df = pd.DataFrame(zippedList, columns=["inventors_name", "city", "country"])
            df.insert(0, "publication_number", publication_number, allow_duplicates=True)

            if len(inventors_name) & len(city) !=0:
                df.to_sql('table_for_inventors', con=engine, if_exists='append', chunksize=1000, index=False)
            else:
                pass

        except BaseException as error:
            logger.error(f"check inventors {error}" + file)

    conn.close()
  
#### Code to extract applicants of the patents
def data_for_applicants():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
                                   .format(user="admin",pw="d5Sj5U7lZqwNYsqRjhJI",db="datacollection"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS table_for_applicants (publication_number varchar(100),\
                             applicants_name varchar(100), city varchar(100), country varchar(100));")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = json.load(open(file))
            df = data["us-bibliographic-data-application"]
            if "publication-reference" in df:
                pub = df.get("publication-reference")
                documentid = pub.get("document-id")
                publication_number = [documentid.get("doc-number")]  # docnumber
            else:
                publication_number = []

            if "parties" in df:
                parties = df.get("parties")
                if "applicants" in parties:
                    applicants = parties.get("applicants")
                    applicant = applicants.get("applicant")
                    if isinstance(applicant, list):
                        res = [sub['addressbook'] for sub in applicant]
                        if "last-name" in res[0]:
                            first_name = [sub['first-name'] for sub in res]  # firstname
                            last_name = [sub['last-name'] for sub in res]
                            applicants_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = [sub['address'] for sub in res]
                            city = [sub['city'] for sub in address]
                            country = [sub['country'] for sub in address]  # country
                        else:
                            applicants_name = []
                            city = []
                            country = []

                    elif isinstance(applicant, dict):
                        res = applicant.get("addressbook")
                        if "last-name" in res:
                            first_name = [res.get("first-name")]  # firstname
                            last_name = [res.get("last-name")]
                            applicants_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = res.get("address")
                            city = [address.get("city")]
                            country = [address.get("country")]  # country
                        else:
                            applicants_name = []
                            city = []
                            country = []

                    else:
                        applicants_name = []
                        city = []
                        country = []

                else:
                    applicants_name = []
                    city = []
                    country = []

            elif "us-parties" in df:
                parties = df.get("us-parties")
                if "us-applicants" in parties:
                    applicants = parties.get("us-applicants")
                    applicant = applicants.get("us-applicant")
                    if isinstance(applicant, list):
                        res = [sub['addressbook'] for sub in applicant]
                        if "last-name" in res[0]:
                            first_name = [sub['first-name'] for sub in res]  # firstname
                            last_name = [sub['last-name'] for sub in res]
                            applicants_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = [sub['address'] for sub in res]
                            city = [sub['city'] for sub in address]
                            country = [sub['country'] for sub in address]  # country
                        else:
                            applicants_name = []
                            city = []
                            country = []

                    elif isinstance(applicant, dict):
                        res = applicant.get("addressbook")
                        if "last-name" in res:
                            first_name = [res.get("first-name")]  # firstname
                            last_name = [res.get("last-name")]
                            applicants_name = [i + " " + j for i, j in zip(first_name, last_name)]
                            address = res.get("address")
                            city = [address.get("city")]
                            country = [address.get("country")]  # country
                        else:
                            applicants_name = []
                            city = []
                            country = []

                    else:
                        applicants_name = []
                        city = []
                        country = []
                else:
                    applicants_name = []
                    city = []
                    country = []

            else:
                continue

            for i in range(len(applicants_name) - 1):
                publication_number.append(publication_number[0])
            zippedList = list(zip(applicants_name, city, country))
            df = pd.DataFrame(zippedList, columns=["applicants_name", "city", "country"])
            df.insert(0, "publication_number", publication_number, allow_duplicates=True)

            if len(applicants_name) & len(city) !=0:
                df.to_sql('table_for_applicants', con=engine, if_exists='append', chunksize=1000, index=False)
            else:
                pass

        except BaseException as error:
            logger.error(f"check applicants {error}" + file)

    conn.close()



### Code to extract abstract of the patents
def data_for_abstract():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s:%(lineno)d:%(name)s:%(message)s')

    file_handler = logging.FileHandler('logger_data.log')
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    working_directory = "/newvolume/xmls"
    engine = create_engine("mysql+pymysql://{user}:{pw}@database-1.cluster-ro-ct2brvwy8za8.us-east-1.rds.amazonaws.com/{db}"
                                   .format(user="admin",pw="d5Sj5U7lZqwNYsqRjhJI",db="datacollection"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS table_for_abstract (publication_number varchar(100),\
                                     abstract MEDIUMTEXT);")
    json_list = []
    for subdir, dirs, files in os.walk(working_directory):
        for file in files:
            if file.endswith('.json'):
                json_list.append(file)
    for file in json_list:
        try:
            data = json.load(open(file))
            df = data["us-bibliographic-data-application"]
            if "publication-reference" in df:
                pub = df.get("publication-reference")
                documentid = pub.get("document-id")
                publication_number = [documentid.get("doc-number")]  # docnumber
            else:
                publication_number = []

            if "abstract" in data:
                df = data["abstract"]
                abst = df.get("p")
                if isinstance(abst, dict):
                    abstract = []

                elif isinstance(abst, list):
                    if isinstance(abst[0], str):
                        abst = str(abst)
                        abstract = [abst]
                    else:
                        abstract = []

                else:
                    abstract = [abst]

            else:
                abstract = []

            zippedList = list(zip(publication_number, abstract))
            df = pd.DataFrame(zippedList, columns=["publication_number", "abstract"])

            if len(abstract) != 0:
                df.to_sql('table_for_abstract', con=engine, if_exists='append', chunksize=1000, index=False)
            else:
                pass

        #     df.to_csv('Patents_description_sample_2021.csv', mode='a', header=False)
        except BaseException as error:
            logger.error(f"check abstract {error} " + file)

    conn.close()

### Exceptions are present for some parsed json files ,check logger_data.log file for more details
