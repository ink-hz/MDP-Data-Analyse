#!/usr/bin/env python3

"""
*
* Copyright (C) 2022 DeShengHong All Rights Reserved.
* 
* File Name: get_data.py
* Author   : Ink Huang
* INFO     : Nhanes download script
*
"""

import os
import sys
import argparse
import urllib.request
import re
import json
import functools

from multiprocessing import Pool
from common import conditionalMkdir
from loguru import logger

try:
        from BeautifulSoup import BeautifulSoup
except ImportError:
        from bs4 import BeautifulSoup

#NhanesList = {'Demographics', 'Dietary', 'Examination', 'Laboratory', 'Questionnaire', 'Non'}
NhanesList = {'Dietary'}

logger.add(sys.stderr, format="{time} {level} {message}", filter="my_module", level="INFO")
logger.add("NhanesDownload.log")


''' Finds all links to XPT files in source HTML '''
def parsePageXPT(html_source):
    # Parse HTML source code with BeautifulSoup library
    soup = BeautifulSoup(html_source, 'html.parser')

    # Get all <a>...</a> with .XPT extensions
    xpt_urls = soup.findAll('a', href=re.compile('\.XPT$'))
    xpt_urls = [url['href'] for url in xpt_urls]
    return xpt_urls


'''Get year associated with file '''
def getFileYear(file_url):
    # Search URL for a year
    year = re.search('\/(\d+-\d+)\/', file_url)

    # Get value from regular expression match
    if year:
        year = year.group(1)
    else:
        # If no match, assign year as 'Other'
        year = 'Other'

    return year


''' Creates directory for file and downloads file from provided URL '''
def getFile(file_dir, file_url, file_type):
    # Get data year
    year = getFileYear(file_url)

    # Compile file location
    file_dir = os.path.join(file_dir, year, file_type)

    # Make directory for file if necessary
    conditionalMkdir(file_dir)

    # Get name for file
    file_name = file_url.split('/')[-1]
    file_loc = os.path.join(file_dir, file_name)

    # Check that file does not already exist
    if not os.path.isfile(file_loc):
        # Download the file and write to local
        urllib.request.urlretrieve(file_url, file_loc)


# Reads HTML source from provided URLs, parses HTML for XPT files, and saves files 

def parseWebSite(url, output_dir):
    # Get base URL for appending to relative file URLs
    base_url = 'http://' + url.lstrip('http://').split('/')[0]

    # Get file type for this URL
    file_type = re.search('Component=([a-zA-Z]+)', url)
    if file_type:
        file_type = file_type.group(1)
    else:
        file_type = 'Other'

    # Open the website and download source HTML
    with urllib.request.urlopen(url) as page:
        html_source = page.read()

    # Parse the website for .XPT file links
    file_urls = parsePageXPT(html_source)
    file_urls = [base_url + file_url for file_url in file_urls]
    result = '\n'.join(file_urls)

    # Save the url to files
    with open('./org_urls/'+file_type, 'w') as f:
        f.write(result)
        f.close()
        logger.info("Save "+file_type+" download urls success!")



def downloadFile(file_type, output_dir):
    # with open('./urls/'+file_type, 'r') as f:
    with open('./urls_html/'+file_type, 'r') as f:
        urls = f.readlines()

    # download file from file_urls
    logger.info('Start download ' + file_type + ' data')
    try:
        i = 0
        for url in urls:
            new_url = url.strip()

            try:
                i = i+1
                logger.info(file_type+new_url+'  i = ' + str(i))
                getFile(output_dir, new_url, file_type)

                # todo Lable parase
                # getLabel(output_dir, file_url, file_type)

                # remove success download url from list
                urls.remove(url)

                logger.info("Download "+file_type+" "+new_url+" success!")

                i = i+1
                if i == 10:
                    logger.info('Finished download ', + file_type + '10 files, update the download url')
                    # update urls file
                    result = ''.join(urls)
                    with open('./urls/'+file_type, 'w') as f:
                        f.write(result)
                        f.close()
                    i = 0
            except Exception as e:
                logger.error('Download ' + file_type + ' ' + new_url + ' failed!')
    except Exception as e:
        logger.error('Download '+file_type+' failed')
    finally:
        # update urls file
        result = ''.join(urls)
        with open('./urls/'+file_type, 'w') as f:
            f.write(result)
            f.close()

    logger.info('Download ' + file_type + ' data finished!')

def main():
    # Get text file with list of URLs for NHANES data
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output_dir', type=str,\
            default='./data/raw_data/', help='Location for writing files')
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multiprocessing python to parallelize downloads')
    parser.add_argument('url_list', type=str, default='./NHANES_URLS.txt',\
            nargs='?', help='text document containing URLs to NHANES\
            website listing data files')
    parser.add_argument('-u', '--update', action='store_true', help='update the nhanes download urls')
    parser.add_argument('-d', '--download', action='store_true', help='download file from the nhanes download urls')
    args = parser.parse_args()

    # Make output directory if necessary
    conditionalMkdir(args.output_dir)

    if args.update:
        logger.info("Update the nhanes urls")
        print(NhanesList)
        # Get list of URLs
        with open(args.url_list, 'r') as f:
            urls = f.readlines()

        # Parse each webpage
        if args.multithread:
            parallelParseWebSite = functools.partial(parseWebSite,\
                    output_dir=args.output_dir)
            pool = Pool(processes=os.cpu_count())
            pool.map(parallelParseWebSite, urls)
        else:
            for url in urls:
                parseWebSite(url, args.output_dir)

    if args.download:
        logger.info("Download file from the nhanes urls")

        if args.multithread:
            parallelDownloadFile = functools.partial(downloadFile,\
                    output_dir=args.output_dir)
            pool = Pool(processes=os.cpu_count())
            pool.map(parallelDownloadFile, NhanesList)
        else:
            for file_type in NhanesList:
                downloadFile(file_type, args.output_dir)

if __name__ == '__main__':
    main()
