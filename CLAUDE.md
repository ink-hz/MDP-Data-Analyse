# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a NHANES (National Health and Nutrition Examination Survey) data download and analysis toolkit. The project downloads health survey data from the CDC website, converts XPT files to CSV format, and provides analysis capabilities for the collected data.

## Core Architecture

### Main Components

1. **Data Download Pipeline** (`get_data.py`):
   - Downloads NHANES data files from CDC website
   - Supports multi-threaded downloads
   - Organizes files by year and data type (Demographics, Dietary, Examination, Laboratory, Questionnaire)
   - Uses BeautifulSoup for HTML parsing and URL extraction

2. **Data Conversion** (`raw_to_csv.py`):
   - Converts XPT (SAS) files to CSV format
   - Supports column header mapping from JSON files
   - Multi-threaded processing capability
   - Automatically removes source XPT files after conversion

3. **Data Analysis** (`analyse.py`):
   - Merges CSV files with identical columns
   - Classifies data by file name patterns
   - Processes HTML documentation files alongside CSV data
   - Manages various data dictionaries for organization

4. **Common Utilities** (`common.py`):
   - File system operations (directory creation, file path management)
   - JSON/YAML serialization utilities
   - Dictionary operations

### Data Flow

1. URLs from `NHANES_URLS.txt` → Download XPT files → Store in `data/raw_data/`
2. XPT files → Convert to CSV → Store in `data/csv_data/`
3. CSV files → Analysis/Merging → Store in `data/merge_csv/` and `data/classified_csv/`

## Common Commands

### Download NHANES Data
```bash
# Update download URLs from NHANES website
python get_data.py -u

# Download data files
python get_data.py -d

# Multi-threaded download
python get_data.py -d -m
```

### Convert XPT to CSV
```bash
# Basic conversion
python raw_to_csv.py

# Multi-threaded conversion with column mapping
python raw_to_csv.py -m -c

# Custom input/output directories
python raw_to_csv.py -i ./data/raw_data/ -o ./data/csv_data/
```

### Run Analysis
```bash
# Run the analysis pipeline
python analyse.py
```

## Key Data Structures

- **File Organization**: `data/{year}/{component_type}/` (e.g., `data/2017-2018/Demographics/`)
- **URL Storage**: Component-specific files in `./org_urls/` and `./urls_html/`
- **Data Dictionaries**: JSON files for tracking file metadata (`csv_dict.json`, `merge_csv_dict.json`, etc.)

## Dependencies

- pandas: Data manipulation and XPT/CSV conversion
- BeautifulSoup4: HTML parsing
- loguru: Logging functionality
- PyYAML: YAML file operations

## Important Notes

- The project uses loguru for logging with output to `NhanesDownload.log`
- Multi-processing support available for both download and conversion operations
- Data is organized hierarchically by year and component type
- HTML documentation files are preserved alongside CSV data for reference
