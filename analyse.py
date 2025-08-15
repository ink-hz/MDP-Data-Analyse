#!/usr/bin/env python3
"""
NHANES Data Analysis Module

This module provides analysis and processing capabilities for NHANES CSV data,
including merging files, classification, and HTML documentation processing.

Copyright (C) 2022 DeShengHong All Rights Reserved.
Author: Ink Huang
Creation Date: 2023-04-24
"""

import os
import sys
import json
import shutil
import argparse
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict, OrderedDict
from enum import Enum

import pandas as pd
from loguru import logger
from bs4 import BeautifulSoup

from common import (
    getFilePathDict,
    saveDictToJsonfile,
    saveDictToYamlfile,
    readJsonFile,
    conditionalMkdir
)


class AnalysisStep(Enum):
    """Analysis workflow steps"""
    INIT_FILE_DICT = 'init_file_dict'
    CHECK_COLUMNS = 'check_columns'
    MERGE_CSV = 'merge_csv'
    CLASSIFY_CSV = 'classify_csv'
    ADD_HTML = 'add_html'
    EXTRACT_HTML_INFO = 'extract_html_info'


@dataclass
class AnalysisConfig:
    """Configuration for analysis operations"""
    csv_data_dir: Path = Path('./data/csv_data')
    html_data_dir: Path = Path('./data/html_data')
    output_dir: Path = Path('./data')
    merge_csv_dir: Path = Path('./data/merge_csv')
    classified_csv_dir: Path = Path('./data/classified_csv')
    
    # Output files
    csv_dict_file: Path = Path('./data/csv_dict.json')
    csv_dict_yaml: Path = Path('./data/csv_dict.yaml')
    merge_csv_dict_file: Path = Path('./data/merge_csv_dict.json')
    same_col_dict_file: Path = Path('./data/same_col_dict.json')
    diff_col_dict_file: Path = Path('./data/diff_col_dict.json')
    classify_csv_dict_file: Path = Path('./data/classify_csv_dict.json')
    html_csv_dict_file: Path = Path('./data/html_csv_dict.json')
    name_dict_file: Path = Path('./data/name_dict.json')
    
    chunk_size: int = 10000
    
    def __post_init__(self):
        """Ensure all directories exist"""
        for dir_path in [self.output_dir, self.merge_csv_dir, self.classified_csv_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for merge
        (self.merge_csv_dir / 'same_column').mkdir(parents=True, exist_ok=True)
        (self.merge_csv_dir / 'diff_column').mkdir(parents=True, exist_ok=True)


class FileClassifier:
    """Handles file classification based on naming patterns"""
    
    @staticmethod
    def extract_file_prefix(filename: str) -> str:
        """
        Extract the prefix from a filename for classification
        
        Args:
            filename: Name of the file
            
        Returns:
            File prefix for classification
        """
        # Remove extension
        name_without_ext = filename.split('.')[0]
        
        # Extract prefix (before underscore or dot)
        if '_' in name_without_ext:
            return name_without_ext.split('_')[0]
        return name_without_ext
    
    @staticmethod
    def classify_files(file_dict: Dict[str, str]) -> Dict[str, List[str]]:
        """
        Classify files based on their naming patterns
        
        Args:
            file_dict: Dictionary of filename to filepath
            
        Returns:
            Dictionary of file type to list of file paths
        """
        classified = defaultdict(list)
        
        for filename, filepath in file_dict.items():
            prefix = FileClassifier.extract_file_prefix(filename)
            classified[prefix].append(filepath)
        
        # Sort each list of files
        for key in classified:
            classified[key].sort()
        
        return dict(classified)


class CSVAnalyzer:
    """Main analyzer class for NHANES CSV data"""
    
    def __init__(self, config: AnalysisConfig = None):
        """Initialize analyzer with configuration"""
        self.config = config or AnalysisConfig()
        self._setup_logging()
        
    def _setup_logging(self) -> None:
        """Configure logging with proper formatting"""
        logger.remove()
        logger.add(
            sys.stderr,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="INFO"
        )
        logger.add(
            "analysis.log",
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {message}"
        )
    
    def initialize_file_dict(self) -> Dict[str, str]:
        """
        Initialize and save file dictionary
        
        Returns:
            Dictionary of filename to filepath
        """
        logger.info("Initializing file dictionary")
        
        # Get all CSV files
        file_dict = getFilePathDict(str(self.config.csv_data_dir), '.csv')
        
        if not file_dict:
            logger.warning(f"No CSV files found in {self.config.csv_data_dir}")
            return {}
        
        # Sort dictionary
        ordered_dict = OrderedDict(sorted(file_dict.items()))
        
        # Save to files
        saveDictToJsonfile(ordered_dict, str(self.config.csv_dict_file))
        saveDictToYamlfile(ordered_dict, str(self.config.csv_dict_yaml))
        
        # Classify files
        classified = FileClassifier.classify_files(ordered_dict)
        saveDictToJsonfile(classified, str(self.config.merge_csv_dict_file))
        
        logger.info(f"Found {len(file_dict)} CSV files, classified into {len(classified)} groups")
        
        return ordered_dict
    
    def check_csv_columns(self) -> Tuple[Dict, Dict]:
        """
        Check column consistency across files of the same type
        
        Returns:
            Tuple of (same_columns_dict, different_columns_dict)
        """
        logger.info("Checking CSV column consistency")
        
        if not self.config.merge_csv_dict_file.exists():
            logger.error(f"Merge CSV dictionary not found: {self.config.merge_csv_dict_file}")
            return {}, {}
        
        file_dict = readJsonFile(str(self.config.merge_csv_dict_file))
        same_columns_dict = {}
        diff_columns_dict = {}
        
        for file_type, file_list in file_dict.items():
            if not file_list:
                continue
            
            # Collect unique column signatures
            column_signatures = set()
            
            for file_path in file_list:
                try:
                    # Read just the header to check columns
                    df = pd.read_csv(file_path, nrows=0)
                    columns_sig = '-'.join(sorted(df.columns))
                    column_signatures.add(columns_sig)
                except Exception as e:
                    logger.error(f"Failed to read {file_path}: {e}")
                    continue
            
            logger.info(f"{file_type}: {len(file_list)} files, {len(column_signatures)} unique column sets")
            
            # Classify based on column consistency
            if len(column_signatures) == 1:
                same_columns_dict[file_type] = file_list
            else:
                diff_columns_dict[file_type] = file_list
        
        # Save results
        saveDictToJsonfile(same_columns_dict, str(self.config.same_col_dict_file))
        saveDictToJsonfile(diff_columns_dict, str(self.config.diff_col_dict_file))
        
        logger.info(f"Files with same columns: {len(same_columns_dict)} groups")
        logger.info(f"Files with different columns: {len(diff_columns_dict)} groups")
        
        return same_columns_dict, diff_columns_dict
    
    def merge_csv_files(self, file_list: List[str], output_path: Path) -> bool:
        """
        Merge multiple CSV files with same columns
        
        Args:
            file_list: List of CSV file paths to merge
            output_path: Output path for merged CSV
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if len(file_list) == 1:
                logger.info(f"Only one file to merge, copying: {file_list[0]}")
                shutil.copy2(file_list[0], output_path)
                return True
            
            logger.info(f"Merging {len(file_list)} files to {output_path}")
            
            # Read and concatenate in chunks for memory efficiency
            chunks = []
            for file_path in file_list:
                try:
                    df = pd.read_csv(file_path, chunksize=self.config.chunk_size)
                    for chunk in df:
                        chunks.append(chunk)
                except Exception as e:
                    logger.error(f"Failed to read {file_path}: {e}")
                    continue
            
            if not chunks:
                logger.error("No data to merge")
                return False
            
            # Concatenate all chunks
            merged_df = pd.concat(chunks, ignore_index=True)
            
            # Save merged file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            
            logger.success(f"Successfully merged to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to merge files: {e}")
            return False
    
    def merge_same_column_csvs(self) -> None:
        """Merge all CSV files with identical columns"""
        logger.info("Starting merge of same-column CSV files")
        
        if not self.config.same_col_dict_file.exists():
            logger.error(f"Same column dictionary not found: {self.config.same_col_dict_file}")
            return
        
        same_col_dict = readJsonFile(str(self.config.same_col_dict_file))
        
        successful = 0
        failed = 0
        
        for file_type, file_list in same_col_dict.items():
            if len(file_list) <= 1:
                logger.debug(f"Skipping {file_type}: only {len(file_list)} file(s)")
                continue
            
            output_path = self.config.merge_csv_dir / 'same_column' / f'{file_type}.csv'
            
            if self.merge_csv_files(file_list, output_path):
                successful += 1
            else:
                failed += 1
        
        logger.info(f"Merge complete: {successful} successful, {failed} failed")
    
    def classify_csv_files(self) -> None:
        """Classify and organize CSV files by type"""
        logger.info("Classifying CSV files")
        
        if not self.config.classify_csv_dict_file.exists():
            logger.error(f"Classification dictionary not found: {self.config.classify_csv_dict_file}")
            return
        
        classify_dict = readJsonFile(str(self.config.classify_csv_dict_file))
        
        for file_type, file_list in classify_dict.items():
            dst_dir = self.config.classified_csv_dir / file_type
            dst_dir.mkdir(parents=True, exist_ok=True)
            
            for src_file in file_list:
                src_path = Path(src_file)
                
                if not src_path.exists():
                    logger.warning(f"Source file not found: {src_path}")
                    continue
                
                # Build destination filename with metadata
                parts = src_path.parts
                if len(parts) >= 3:
                    year = parts[-3]
                    component = parts[-2]
                    filename = parts[-1]
                    dst_filename = f"{component}_{year}_{filename}"
                else:
                    dst_filename = src_path.name
                
                dst_file = dst_dir / dst_filename
                
                # Copy CSV file
                try:
                    shutil.copy2(src_path, dst_file)
                    logger.debug(f"Copied {src_path.name} to {dst_file}")
                    
                    # Copy associated HTML if exists
                    src_html = src_path.with_suffix('.htm')
                    if src_html.exists():
                        dst_html = dst_file.with_suffix('.htm')
                        shutil.copy2(src_html, dst_html)
                        logger.debug(f"Copied associated HTML: {src_html.name}")
                        
                except Exception as e:
                    logger.error(f"Failed to copy {src_path}: {e}")
        
        logger.info("Classification complete")
    
    def add_html_to_csv(self) -> None:
        """Copy HTML documentation files alongside CSV files"""
        logger.info("Adding HTML documentation to CSV files")
        
        if not self.config.csv_dict_file.exists():
            logger.error(f"CSV dictionary not found: {self.config.csv_dict_file}")
            return
        
        file_dict = readJsonFile(str(self.config.csv_dict_file))
        
        successful = 0
        missing = 0
        
        for filename, csv_path in file_dict.items():
            # Build HTML paths
            src_html = Path(csv_path).with_suffix('.htm')
            src_html = src_html.parent.parent / 'html_data' / src_html.parent.name / src_html.name
            
            dst_html = Path(csv_path).with_suffix('.htm')
            
            if src_html.exists():
                try:
                    shutil.copy2(src_html, dst_html)
                    successful += 1
                    logger.debug(f"Copied HTML for {filename}")
                except Exception as e:
                    logger.error(f"Failed to copy HTML for {filename}: {e}")
            else:
                missing += 1
                logger.debug(f"HTML not found for {filename}")
        
        logger.info(f"HTML copy complete: {successful} copied, {missing} not found")
    
    def extract_html_info(self) -> Dict[str, List[str]]:
        """
        Extract metadata information from HTML documentation files
        
        Returns:
            Dictionary of file type to list of extracted names
        """
        logger.info("Extracting information from HTML files")
        
        if not self.config.html_csv_dict_file.exists():
            logger.error(f"HTML CSV dictionary not found: {self.config.html_csv_dict_file}")
            return {}
        
        file_dict = readJsonFile(str(self.config.html_csv_dict_file))
        name_dict = {}
        
        for file_type, file_list in file_dict.items():
            names = set()
            
            for file_path in file_list:
                if not file_path.endswith('.htm'):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        soup = BeautifulSoup(f, 'html.parser')
                    
                    # Extract page header information
                    page_header = soup.find('div', {'id': 'PageHeader'})
                    if page_header:
                        h3_tags = page_header.find_all('h3')
                        for h3 in h3_tags:
                            text = h3.get_text(strip=True)
                            if '(' in text:
                                name = text.split('(')[0].strip()
                                names.add(name)
                    
                except Exception as e:
                    logger.error(f"Failed to parse HTML {file_path}: {e}")
            
            name_dict[file_type] = sorted(list(names))
            logger.info(f"Extracted {len(names)} unique names for {file_type}")
        
        # Save results
        saveDictToJsonfile(name_dict, str(self.config.name_dict_file))
        
        return name_dict
    
    def run_analysis(self, steps: List[AnalysisStep] = None) -> None:
        """
        Run the complete analysis pipeline
        
        Args:
            steps: List of analysis steps to run (default: all steps)
        """
        if steps is None:
            steps = list(AnalysisStep)
        
        logger.info(f"Running analysis with steps: {[s.value for s in steps]}")
        
        if AnalysisStep.INIT_FILE_DICT in steps:
            self.initialize_file_dict()
        
        if AnalysisStep.CHECK_COLUMNS in steps:
            self.check_csv_columns()
        
        if AnalysisStep.MERGE_CSV in steps:
            self.merge_same_column_csvs()
        
        if AnalysisStep.CLASSIFY_CSV in steps:
            self.classify_csv_files()
        
        if AnalysisStep.ADD_HTML in steps:
            self.add_html_to_csv()
        
        if AnalysisStep.EXTRACT_HTML_INFO in steps:
            self.extract_html_info()
        
        logger.info("Analysis pipeline complete")


def main():
    """Main entry point for the analysis script"""
    parser = argparse.ArgumentParser(
        description='Analyze and process NHANES CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-i', '--input_dir',
        type=str,
        default='./data/csv_data',
        help='Input directory containing CSV files'
    )
    
    parser.add_argument(
        '-o', '--output_dir',
        type=str,
        default='./data',
        help='Output directory for analysis results'
    )
    
    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize file dictionary'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check column consistency'
    )
    
    parser.add_argument(
        '--merge',
        action='store_true',
        help='Merge files with same columns'
    )
    
    parser.add_argument(
        '--classify',
        action='store_true',
        help='Classify CSV files'
    )
    
    parser.add_argument(
        '--html',
        action='store_true',
        help='Add HTML documentation'
    )
    
    parser.add_argument(
        '--extract',
        action='store_true',
        help='Extract HTML information'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Run all analysis steps'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config = AnalysisConfig(
        csv_data_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir)
    )
    
    # Initialize analyzer
    analyzer = CSVAnalyzer(config)
    
    # Determine which steps to run
    if args.all:
        steps = list(AnalysisStep)
    else:
        steps = []
        if args.init:
            steps.append(AnalysisStep.INIT_FILE_DICT)
        if args.check:
            steps.append(AnalysisStep.CHECK_COLUMNS)
        if args.merge:
            steps.append(AnalysisStep.MERGE_CSV)
        if args.classify:
            steps.append(AnalysisStep.CLASSIFY_CSV)
        if args.html:
            steps.append(AnalysisStep.ADD_HTML)
        if args.extract:
            steps.append(AnalysisStep.EXTRACT_HTML_INFO)
    
    # Default to check columns if no steps specified
    if not steps and not args.all:
        steps = [AnalysisStep.CHECK_COLUMNS]
    
    # Run analysis
    try:
        analyzer.run_analysis(steps)
    except KeyboardInterrupt:
        logger.warning("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during analysis: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()