#!/usr/bin/env python3
"""
XPT to CSV Conversion Module

This module converts NHANES XPT (SAS) files to CSV format with optional
column header mapping from JSON files.

Copyright (C) 2022 DeShengHong All Rights Reserved.
Author: Ink Huang
Creation Date: 2022-11-30
"""

import os
import sys
import json
import argparse
import functools
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from multiprocessing import Pool
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from common import conditionalMkdir


@dataclass
class ConversionConfig:
    """Configuration for file conversion operations"""
    input_dir: Path = Path('./data/raw_data/')
    output_dir: Path = Path('./data/csv_data/')
    use_column_mapping: bool = False
    chunk_size: int = 10000
    
    def __post_init__(self):
        """Ensure output directory exists"""
        self.output_dir.mkdir(parents=True, exist_ok=True)


class XPTConverter:
    """Converter for XPT files to CSV format"""
    
    def __init__(self, config: ConversionConfig = None):
        """Initialize converter with configuration"""
        self.config = config or ConversionConfig()
        self._setup_logging()
        self.column_map: Dict[str, str] = {}
        
    def _setup_logging(self) -> None:
        """Configure logging with proper formatting"""
        logger.remove()
        logger.add(
            sys.stderr,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="INFO"
        )
        logger.add(
            "conversion.log",
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {message}"
        )
    
    def load_column_mappings(self, walk_paths: List[Tuple]) -> Dict[str, str]:
        """
        Load column header mappings from JSON files
        
        Args:
            walk_paths: List of os.walk() results
            
        Returns:
            Dictionary mapping column names to verbose names
        """
        global_map = {}
        
        for read_dir, _, file_names in walk_paths:
            # Filter JSON files
            json_files = [f for f in file_names if f.endswith('.JSON')]
            
            for json_file in json_files:
                file_path = Path(read_dir) / json_file
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        current_map = json.load(f)
                    
                    # Add mappings to global map (case-insensitive)
                    for key, value in current_map.items():
                        global_map[key.lower()] = value
                        
                    logger.debug(f"Loaded {len(current_map)} mappings from {json_file}")
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error in {file_path}: {e}")
                except IOError as e:
                    logger.error(f"Failed to read {file_path}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error loading {file_path}: {e}")
        
        logger.info(f"Loaded total of {len(global_map)} column mappings")
        return global_map
    
    def apply_column_mapping(self, df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
        """
        Apply column name mappings to DataFrame
        
        Args:
            df: DataFrame with original column names
            column_map: Dictionary mapping old names to new names
            
        Returns:
            DataFrame with renamed columns
        """
        if not column_map:
            return df
        
        # Create case-insensitive mapping
        columns_lower = [col.lower() for col in df.columns]
        
        # Map columns
        new_columns = []
        mapped_count = 0
        
        for col, col_lower in zip(df.columns, columns_lower):
            if col_lower in column_map:
                new_columns.append(column_map[col_lower])
                mapped_count += 1
                logger.debug(f"Mapped column: {col} -> {column_map[col_lower]}")
            else:
                new_columns.append(col)
        
        df.columns = new_columns
        
        if mapped_count > 0:
            logger.info(f"Mapped {mapped_count}/{len(df.columns)} columns")
        
        return df
    
    def convert_xpt_to_csv(
        self,
        xpt_path: Path,
        csv_path: Path,
        column_map: Optional[Dict[str, str]] = None,
        remove_original: bool = True
    ) -> bool:
        """
        Convert a single XPT file to CSV
        
        Args:
            xpt_path: Path to input XPT file
            csv_path: Path to output CSV file
            column_map: Optional column name mappings
            remove_original: Whether to delete XPT file after conversion
            
        Returns:
            True if conversion successful, False otherwise
        """
        try:
            logger.info(f"Converting: {xpt_path.name}")
            
            # Read XPT file
            df = pd.read_sas(xpt_path, chunksize=self.config.chunk_size)
            
            # Process in chunks for memory efficiency
            first_chunk = True
            
            for chunk in df:
                # Apply column mapping if provided
                if column_map:
                    chunk = self.apply_column_mapping(chunk, column_map)
                
                # Write to CSV
                if first_chunk:
                    chunk.to_csv(csv_path, index=False, mode='w')
                    first_chunk = False
                else:
                    chunk.to_csv(csv_path, index=False, mode='a', header=False)
            
            # Verify output file was created
            if not csv_path.exists():
                raise IOError(f"Failed to create output file: {csv_path}")
            
            # Remove original file if requested
            if remove_original:
                xpt_path.unlink()
                logger.info(f"Removed original file: {xpt_path.name}")
            
            logger.success(f"Successfully converted: {xpt_path.name} -> {csv_path.name}")
            return True
            
        except pd.errors.EmptyDataError:
            logger.warning(f"Empty XPT file: {xpt_path}")
            return False
        except Exception as e:
            logger.error(f"Failed to convert {xpt_path}: {e}")
            return False
    
    def process_directory(
        self,
        walk_path: Tuple,
        input_prefix: str,
        output_prefix: str,
        column_map: Optional[Dict[str, str]] = None
    ) -> Tuple[int, int]:
        """
        Process all XPT files in a directory
        
        Args:
            walk_path: os.walk() result tuple
            input_prefix: Input directory prefix to strip
            output_prefix: Output directory prefix
            column_map: Optional column name mappings
            
        Returns:
            Tuple of (successful conversions, failed conversions)
        """
        read_dir, _, file_names = walk_path
        
        # Filter XPT files
        xpt_files = [f for f in file_names if f.upper().endswith('.XPT')]
        
        if not xpt_files:
            return 0, 0
        
        logger.info(f"Processing {len(xpt_files)} XPT files in {read_dir}")
        
        successful = 0
        failed = 0
        
        for xpt_file in xpt_files:
            # Build paths
            xpt_path = Path(read_dir) / xpt_file
            
            # Maintain directory structure
            relative_dir = Path(read_dir).relative_to(input_prefix)
            output_dir = Path(output_prefix) / relative_dir
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create CSV filename
            csv_file = xpt_file.replace('.XPT', '.csv').replace('.xpt', '.csv')
            csv_path = output_dir / csv_file
            
            # Convert file
            if self.convert_xpt_to_csv(xpt_path, csv_path, column_map):
                successful += 1
            else:
                failed += 1
        
        return successful, failed
    
    def convert_all(self, multithread: bool = False) -> None:
        """
        Convert all XPT files in input directory to CSV
        
        Args:
            multithread: Whether to use multiprocessing
        """
        # Get directory listing
        walk_paths = list(os.walk(self.config.input_dir))
        
        if not walk_paths:
            logger.warning(f"No files found in {self.config.input_dir}")
            return
        
        # Load column mappings if requested
        column_map = None
        if self.config.use_column_mapping:
            column_map = self.load_column_mappings(walk_paths)
        
        # Process files
        logger.info(f"Starting conversion of XPT files to CSV")
        
        total_successful = 0
        total_failed = 0
        
        if multithread:
            # Parallel processing
            process_func = functools.partial(
                self.process_directory,
                input_prefix=str(self.config.input_dir),
                output_prefix=str(self.config.output_dir),
                column_map=column_map
            )
            
            with Pool(processes=os.cpu_count()) as pool:
                results = pool.map(process_func, walk_paths)
            
            for successful, failed in results:
                total_successful += successful
                total_failed += failed
        else:
            # Sequential processing
            for walk_path in walk_paths:
                successful, failed = self.process_directory(
                    walk_path,
                    str(self.config.input_dir),
                    str(self.config.output_dir),
                    column_map
                )
                total_successful += successful
                total_failed += failed
        
        # Log summary
        logger.info(f"Conversion complete: {total_successful} successful, {total_failed} failed")


def validate_arguments(args: argparse.Namespace) -> None:
    """
    Validate command line arguments
    
    Args:
        args: Parsed command line arguments
        
    Raises:
        SystemExit: If validation fails
    """
    input_dir = Path(args.input_dir)
    
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    if not input_dir.is_dir():
        logger.error(f"Input path is not a directory: {input_dir}")
        sys.exit(1)
    
    # Check for XPT files
    xpt_files = list(input_dir.rglob('*.XPT')) + list(input_dir.rglob('*.xpt'))
    if not xpt_files:
        logger.warning(f"No XPT files found in {input_dir}")
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(0)


def main():
    """Main entry point for the conversion script"""
    parser = argparse.ArgumentParser(
        description='Convert NHANES XPT files to CSV format',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-i', '--input_dir',
        type=str,
        default='./data/raw_data/',
        help='Input directory containing XPT files'
    )
    
    parser.add_argument(
        '-o', '--output_dir',
        type=str,
        default='./data/csv_data/',
        help='Output directory for CSV files'
    )
    
    parser.add_argument(
        '-c', '--columns',
        action='store_true',
        help='Apply column header mapping from JSON files'
    )
    
    parser.add_argument(
        '-m', '--multithread',
        action='store_true',
        help='Use multiprocessing for parallel conversion'
    )
    
    parser.add_argument(
        '-k', '--keep',
        action='store_true',
        help='Keep original XPT files after conversion'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    validate_arguments(args)
    
    # Create configuration
    config = ConversionConfig(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        use_column_mapping=args.columns
    )
    
    # Initialize converter
    converter = XPTConverter(config)
    
    # Run conversion
    try:
        converter.convert_all(multithread=args.multithread)
    except KeyboardInterrupt:
        logger.warning("Conversion interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during conversion: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()