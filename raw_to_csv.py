#!/usr/bin/env python3
"""
* Copyright (C) 2022 DeShengHong All Rights Reserved.
* 
* File Name: raw_to_csv.py
* Author   : Ink Huang
* Creation Date: 2022-11-30
* INFO     : Nhanes XPT to csv script
"""

import os
import pandas as pd
import json
import argparse
import functools
from multiprocessing import Pool

from common import conditionalMkdir


def loadConcatColumns(os_walk_paths):
    # Empty dictionary to hold mapped column names
    global_map = {}

    # Cycle through walk paths
    for os_walk_path in os_walk_paths:
        read_dir, _, f_names = os_walk_path
        # Filter out non JSON files
        f_names = [f_name for f_name in f_names if f_name.endswith('.JSON')]

        # Get information for each column header
        for f_name in f_names:
            # Load JSON
            f_path = os.path.join(read_dir, f_name)
            try:
                with open(f_path) as json_file:
                    current_map = json.load(json_file)
            # Catch decode errors due to empty JSON files
            except ValueError:
                print('JSON decode error: %s' % f_path)
                continue

            # Add key:val pairs to global_map
            for key, val in current_map.items():
                if key.lower() not in global_map:
                    global_map[key.lower()] = val

    return global_map



''' Replaces column headers using mapping in JSON file '''
def replaceColumns(data, columns_map):
    # Process current column keys to improve matching
    columns = [val.lower() for val in data.columns]

    # Map current labels to new labels
    data.columns = [columns_map[val] if val in columns_map else val\
            for val in columns]

    return data



''' Converts XPT files to CSV files '''
def XPT2CSV(os_walk_path, input_prefix, output_prefix, columns_map):
    read_dir, _, f_names = os_walk_path
    # Filter out non XPT files
    f_names = [f_name for f_name in f_names if f_name.endswith('.XPT')]
    for f_name in f_names:
        # Define read and write paths
        read_path = os.path.join(read_dir, f_name)
        write_dir = os.path.join(output_prefix, read_dir.lstrip(input_prefix))
        write_path = os.path.join(write_dir, f_name.replace('.XPT', '.csv'))

        # Make sure directory exists
        conditionalMkdir(write_dir)

        # Print progress
        print('Converting file: %s' % read_path)

        try:
            # Read XPT (SAS) and write CSV
            data = pd.read_sas(read_path)
            if columns_map:
                data = replaceColumns(data, columns_map)
            data.to_csv(write_path, index=False)
            os.remove(read_path)
            print('Converting file: %s' % read_path + ' success, remove the org file')
        except Exception as e:
            print('Xpt to csv failed, file path: '+read_path)


def main():
    # Get arguments from user
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--multithread', action='store_true',\
            help='invoke multiprocessing python to parallelize conversion')
    parser.add_argument('-i', '--input_dir', default='./data/raw_data/',\
            type=str, help='Input directory containing raw .XPT files from\
            NHANES dataset')
    parser.add_argument('-o', '--output_dir', default='./data/csv_data/',\
            type=str, help='Output directory for .XPT data files converted to\
            .CSV files')
    parser.add_argument('-c', '--columns', action='store_true', help='Boolean\
            setting for converting column headers to more verbose option.\
            Requires .JSON file with mapping in same directory as data file')
    args = parser.parse_args()

    # Get listing of data input directory
    os_walk_paths = list(os.walk(args.input_dir))
    # Load column header dictionaries if necessary
    if args.columns:
        columns = loadConcatColumns(os_walk_paths)
    else:
        columns = False

    # Convert XPT files to CSV
    if args.multithread:
        parallelXPT2CSV = functools.partial(XPT2CSV,\
                input_prefix=args.input_dir, output_prefix=args.output_dir,\
                columns_map=columns)
        pool = Pool(processes=os.cpu_count())
        pool.map(parallelXPT2CSV, os_walk_paths)
    else:
        for os_walk_path in os_walk_paths:
            XPT2CSV(os_walk_path, args.input_dir, args.output_dir, columns)


if __name__ == '__main__':
    main()
