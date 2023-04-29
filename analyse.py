#!/usr/bin/env python3
"""
* Copyright (C) 2022 DeShengHong All Rights Reserved.
* 
* File Name: analyse.py
* Author   : Ink Huang
* Creation Date: 2023-04-24
* INFO     : Nhanes analyse script
"""

import os
import time
import json
import copy
import pandas as pd
import pprint
import shutil
import collections


from common import *
from collections import defaultdict
from bs4 import BeautifulSoup

def saveFileDict(file_dict):
    saveDictToJsonfile(od_dict, './data/csv_dict.json')
    saveDictToYamlfile(od_dict, './data/csv_dict.yaml')

    # pprint.pprint(od_dict)
    # print(json.dumps(od_dict, indent = 4))

def classifyByFileName(od_dict):
    file_list = od_dict.keys()
    filetype_set = set()

    for file in file_list:
        file = file.split('.')[0]
        file = file.split('_')[0]
        filetype_set.add(file)

    new_dict = defaultdict(list)
    for filetype in filetype_set:
        for file in file_list:
            # if str(filetype + '.') in file or str(filetype + '_') in file:
            if file.startswith(filetype + '.') or file.startswith(filetype + '_'):
            # if str.startswith(file)
                new_dict[filetype].append(od_dict[file])
    return new_dict



# def saveHtmlDict(html_dict):
    # todo

def initFileDict():
    # Get listing of data input directory
    file_dict = getFilePathDict('./data/csv_data', '.csv')

    # sort the file dict by key
    od_dict = collections.OrderedDict(sorted(file_dict.items()))

    # saveFileDict(file_dict)

    # classify 
    new_dict = classifyByFileName(od_dict)
    saveDictToJsonfile(new_dict, './data/merge_csv_dict.json')

def checkCsvColumns():
    file_dict = readJsonFile('./data/merge_csv_dict.json')
    same_columns_dict = dict()
    diff_columns_dict = dict()
    columns_set = set()

    for key in file_dict.keys():
        file_list = file_dict[key]

        for file in file_list:
            data = pd.read_csv(file)
            columns = '-'.join(data.columns)
            columns_set.add(columns)
        print(key + ' file size: ' + str(len(file_list)) + ' --- columns size: ' + str(len(columns_set)))

        if len(columns_set) == 1:
            same_columns_dict[key] = file_dict[key]
        else:
            diff_columns_dict[key] = file_dict[key]

        columns_set.clear()

    # saveDictToJsonfile(same_columns_dict, './data/same_col_dict.json')
    # saveDictToJsonfile(diff_columns_dict, './data/diff_col_dict.json')

def mergeSameColCsv():
    same_col_dict = readJsonFile('./data/same_col_dict.json')
    left_col_dict = copy.deepcopy(same_col_dict)

    try:
        for key in same_col_dict.keys():
            print('Start merge ' + key + ' csv files: ' + ','.join(same_col_dict[key]))
            df = pd.concat(map(pd.read_csv, same_col_dict[key]), ignore_index=True)
            df.to_csv('./data/merge_csv/same_column/'+key+'.csv', index = False)

            left_col_dict.pop(key)
            print('Finished merge ' + key + ' csv')
    except Exception as e:
        print('Merge csv failed:', e)

    finally:
        saveDictToJsonfile(same_col_dict, './data/same_col_dict.json')

def classifyCsv():
    conditionalMkdir('./data/classified_csv/')

    classify_dict = readJsonFile('./data/classify_csv_dict.json')

    for key in classify_dict:
        try:
            dst_dir = './data/classified_csv/'+key+'/'
            conditionalMkdir(dst_dir)

            for file in classify_dict[key]:
                file_name = file.split('/')[-1]
                type = file.split('/')[-2]
                file_year = file.split('/')[-3]
                dst_file = dst_dir+type+'_'+file_year+'_'+file_name

                src_html = file.replace('.csv', '.htm')
                dst_html = dst_file.replace('.csv', '.htm')

                shutil.copyfile(src_html, dst_html)
                print('Copy ' + src_html, ' --> ' +  dst_html + ' success')

                shutil.copyfile(file, dst_file)
                print('Copy ' + file, ' --> ' +  dst_file + ' success')

        except Exception as e:
            print('Classify csv failed:', e)

def addHtmlToCsv():
    file_dict = readJsonFile('./data/csv_dict.json')

    i = 0

    for file in file_dict:
        src_html_path = file_dict[file].replace('csv_data', 'html_data').replace('.csv', '.htm')
        src_html_path = './data/' + src_html_path

        dst_html_path = file_dict[file].replace('.csv', '.htm')
        dst_html_path = './data/' + dst_html_path

        shutil.copyfile(src_html_path, dst_html_path)
        print('Copy ' + src_html_path, ' --> ' +  dst_html_path + ' success')

        i = i+1
    print('Copy ' + str(i) + ' htmls success')

def copyMergeCsv():
    file_dict = readJsonFile('./data/same_col_dict.json')

    for key in file_dict:
        if len(file_dict[key]) > 1:
            src_file = './data/merge_csv/same_column/'+key+'.csv'
            dst_file = '/home/ftp/Nhanes/classified_csv/'+key+'/'+key+'.csv'

            shutil.copyfile(src_file, dst_file)
            print('Copy ' + src_file, ' --> ' +  dst_file + ' success')

def delOnly1ColMergeCsv():
    file_dict = readJsonFile('./data/same_col_dict.json')

    for key in file_dict:
        if len(file_dict[key]) == 1:
            # dst_file = '/home/ftp/Nhanes/classified_csv/'+key+'/'+key+'.csv'
            dst_file = './data/classified_csv/'+key+'/'+key+'.csv'
            os.remove(dst_file)

            print('Del '+ dst_file + ' success')



def getClassifiedDict():
    file_dict = readJsonFile('./data/classify_csv_dict.json')

    new_dict = dict()

    for key in file_dict:
        dir = './data/classified_csv/'+key+'/'
        file_paths = list(os.walk(dir))

        new_file_names = list()

        for file_path in file_paths:
            file_dir, _, f_names = file_path
            for f_name in f_names:
                f_name  = './data/classified_csv/' + key + '/' + f_name
                new_file_names.append(f_name)

            # print(key + ' -- ' + file_paths)
            new_dict[key] = new_file_names

    saveDictToJsonfile(new_dict, './data/html_csv_dict.json')


def getHtmlInfo():
    file_dict = readJsonFile('./data/html_csv_dict.json')
    name_dict = dict()
    all_name_set = set()

    for key in file_dict:
        name_set = set()
        for file in file_dict[key]:
            if '.htm' in file:
                html = BeautifulSoup(open(file), 'html.parser')
                result = html.find("div", {"id": "PageHeader"})
                name = html.find("page", {"name": "NameHeader"})
                key = html.find("page", {"key": "KeyHeader"})
                print(html)
                time.sleep(100)

                for res in result:
                    if str('h3') in str(res):
                        all_name = str(res).split('>')[1].split('(')[0].strip()
                name_set.add(all_name)
                all_name_set.add(all_name)

        name_dict[key] = list(name_set)

    saveDictToJsonfile(name_dict, './data/name_dict.json')


def main():
    # initFileDict()
    
    checkCsvColumns()
    
    # mergeSameColCsv()

    # addHtmlToCsv()

    # classifyCsv()

    # copyMergeCsv()
    
    # delOnly1ColMergeCsv()

    # getClassifiedDict()

    # getHtmlInfo()



if __name__ == '__main__':
    main()
