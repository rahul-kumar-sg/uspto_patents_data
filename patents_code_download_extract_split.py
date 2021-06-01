## Major imports
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

### Downloading of files, PLEASE ENTER the link for the respective year to download patents data in url variable.
def download_xml():
    url = 'https://bulkdata.uspto.gov/data/patent/application/redbook/fulltext/2016/'
    page = requests.get(url)
    #print(page)
    soup = BeautifulSoup(page.text, 'html.parser')
    #print(soup.prettify())
    download_urls = []
    file_names = []
    for link in soup.find_all('a'):
        if '.zip' in link['href']:
            file_names.append(link['href'].split('.')[0].replace('ipa',''))
            download_urls.append(url + link['href'])

    for download_url in download_urls[0:1]:
        r = requests.get(download_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(path = r"F:\FINAL\xml_downloads")


### code for splitting all xml files

def split_large_xml(infile):
    smallfile = None
    with open(infile) as bigfile:
        for lineno, line in enumerate(bigfile):
            if '<?xml version="1.0" encoding="UTF-8"?>' in line:
                print('starting xml: ' + str(lineno))
                
    #         if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                folder = os.path.join(r"F:\FINAL", "xmls")
                small_filename = 'small_file_{}.xml'.format(lineno)
                file = os.path.join(folder, small_filename)
                smallfile = open(file, "w")

            smallfile.write(line)
        if smallfile:
            smallfile.close()
            

