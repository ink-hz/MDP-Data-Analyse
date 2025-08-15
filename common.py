#!/usr/bin/env python3
"""
Common Utilities Module

This module provides common utilities for file operations, JSON/YAML handling,
and directory management used across the NHANES data processing pipeline.

Copyright (C) 2022 DeShengHong All Rights Reserved.
Author: Ink Huang
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from collections import OrderedDict

from loguru import logger


def conditionalMkdir(directory: Union[str, Path]) -> None:
    """
    Create directory if it doesn't exist
    
    Args:
        directory: Path to directory to create
    """
    path = Path(directory)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created directory: {path}")


def ensure_directory(directory: Union[str, Path]) -> Path:
    """
    Ensure directory exists and return Path object
    
    Args:
        directory: Directory path
        
    Returns:
        Path object for the directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path


def getFilePathDict(directory: Union[str, Path], file_type: str) -> Dict[str, str]:
    """
    Get dictionary mapping filenames to their full paths
    
    Args:
        directory: Directory to search
        file_type: File extension to filter (e.g., '.csv', '.json')
        
    Returns:
        Dictionary mapping filename to full path
    """
    directory = Path(directory)
    file_path_dict = {}
    
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return file_path_dict
    
    # Walk through directory tree
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(file_type):
                file_path = Path(root) / filename
                file_path_dict[filename] = str(file_path)
    
    logger.debug(f"Found {len(file_path_dict)} {file_type} files in {directory}")
    return file_path_dict


def get_files_by_pattern(
    directory: Union[str, Path],
    pattern: str = "*",
    recursive: bool = True
) -> List[Path]:
    """
    Get list of files matching a pattern
    
    Args:
        directory: Directory to search
        pattern: Glob pattern for matching files
        recursive: Whether to search recursively
        
    Returns:
        List of Path objects for matching files
    """
    directory = Path(directory)
    
    if not directory.exists():
        logger.warning(f"Directory does not exist: {directory}")
        return []
    
    if recursive:
        files = list(directory.rglob(pattern))
    else:
        files = list(directory.glob(pattern))
    
    # Filter to only files (not directories)
    files = [f for f in files if f.is_file()]
    
    logger.debug(f"Found {len(files)} files matching pattern '{pattern}' in {directory}")
    return files


def saveDictToJsonfile(
    input_dict: Dict[str, Any],
    json_file: Union[str, Path],
    indent: int = 4,
    sort_keys: bool = True
) -> bool:
    """
    Save dictionary to JSON file with proper formatting
    
    Args:
        input_dict: Dictionary to save
        json_file: Path to output JSON file
        indent: JSON indentation level
        sort_keys: Whether to sort dictionary keys
        
    Returns:
        True if successful, False otherwise
    """
    try:
        json_file = Path(json_file)
        
        # Ensure parent directory exists
        json_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Sort dictionary if requested
        if sort_keys:
            output_dict = OrderedDict(sorted(input_dict.items()))
        else:
            output_dict = input_dict
        
        # Write JSON file
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(output_dict, f, indent=indent, ensure_ascii=False)
        
        logger.debug(f"Saved dictionary to JSON: {json_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save JSON file {json_file}: {e}")
        return False


def saveDictToYamlfile(
    input_dict: Dict[str, Any],
    yaml_file: Union[str, Path],
    sort_keys: bool = True
) -> bool:
    """
    Save dictionary to YAML file
    
    Args:
        input_dict: Dictionary to save
        yaml_file: Path to output YAML file
        sort_keys: Whether to sort dictionary keys
        
    Returns:
        True if successful, False otherwise
    """
    try:
        yaml_file = Path(yaml_file)
        
        # Ensure parent directory exists
        yaml_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Sort dictionary if requested
        if sort_keys:
            output_dict = OrderedDict(sorted(input_dict.items()))
        else:
            output_dict = input_dict
        
        # Write YAML file
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(
                output_dict,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=sort_keys
            )
        
        logger.debug(f"Saved dictionary to YAML: {yaml_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save YAML file {yaml_file}: {e}")
        return False


def readJsonFile(json_file: Union[str, Path]) -> Dict[str, Any]:
    """
    Read JSON file and return dictionary
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        Dictionary from JSON file, empty dict if error
    """
    try:
        json_file = Path(json_file)
        
        if not json_file.exists():
            logger.warning(f"JSON file does not exist: {json_file}")
            return {}
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.debug(f"Read JSON file: {json_file}")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in {json_file}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Failed to read JSON file {json_file}: {e}")
        return {}


def readYamlFile(yaml_file: Union[str, Path]) -> Dict[str, Any]:
    """
    Read YAML file and return dictionary
    
    Args:
        yaml_file: Path to YAML file
        
    Returns:
        Dictionary from YAML file, empty dict if error
    """
    try:
        yaml_file = Path(yaml_file)
        
        if not yaml_file.exists():
            logger.warning(f"YAML file does not exist: {yaml_file}")
            return {}
        
        with open(yaml_file, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        logger.debug(f"Read YAML file: {yaml_file}")
        return data if data else {}
        
    except yaml.YAMLError as e:
        logger.error(f"YAML parse error in {yaml_file}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Failed to read YAML file {yaml_file}: {e}")
        return {}


def merge_dicts(*dicts: Dict[str, Any], deep: bool = False) -> Dict[str, Any]:
    """
    Merge multiple dictionaries
    
    Args:
        *dicts: Variable number of dictionaries to merge
        deep: Whether to perform deep merge for nested dictionaries
        
    Returns:
        Merged dictionary
    """
    result = {}
    
    for d in dicts:
        if not isinstance(d, dict):
            continue
        
        if deep:
            # Deep merge for nested dictionaries
            for key, value in d.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value, deep=True)
                else:
                    result[key] = value
        else:
            # Shallow merge
            result.update(d)
    
    return result


def safe_file_operation(
    operation: callable,
    *args,
    default_return: Any = None,
    error_message: str = None,
    **kwargs
) -> Any:
    """
    Safely execute file operations with error handling
    
    Args:
        operation: Function to execute
        *args: Arguments for the function
        default_return: Value to return on error
        error_message: Custom error message
        **kwargs: Keyword arguments for the function
        
    Returns:
        Result of operation or default_return on error
    """
    try:
        return operation(*args, **kwargs)
    except Exception as e:
        if error_message:
            logger.error(f"{error_message}: {e}")
        else:
            logger.error(f"File operation failed: {e}")
        return default_return


def get_file_stats(file_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    Get statistics about a file
    
    Args:
        file_path: Path to file
        
    Returns:
        Dictionary with file statistics or None if error
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            return None
        
        stat = file_path.stat()
        
        return {
            'path': str(file_path),
            'name': file_path.name,
            'size': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'modified': stat.st_mtime,
            'created': stat.st_ctime,
            'is_file': file_path.is_file(),
            'is_dir': file_path.is_dir(),
            'suffix': file_path.suffix,
            'parent': str(file_path.parent)
        }
        
    except Exception as e:
        logger.error(f"Failed to get stats for {file_path}: {e}")
        return None