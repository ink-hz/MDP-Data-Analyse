# NHANES Data Analysis Pipeline

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A comprehensive Python toolkit for downloading, processing, and analyzing data from the National Health and Nutrition Examination Survey (NHANES). This pipeline provides automated tools for managing large-scale health survey data with efficient processing and analysis capabilities.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Scenarios](#usage-scenarios)
- [Code Structure](#code-structure)
- [Framework Design](#framework-design)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Overview

The NHANES Data Analysis Pipeline is designed to streamline the process of working with NHANES survey data. It handles the complete workflow from data acquisition to analysis, providing researchers and data scientists with a robust toolset for health data analysis.

### Key Capabilities

- **Automated Data Collection**: Download NHANES data files directly from the CDC website
- **Format Conversion**: Convert proprietary XPT (SAS) files to universally accessible CSV format
- **Data Organization**: Intelligent classification and organization of data by type and year
- **Analysis Tools**: Merge, classify, and analyze datasets with column consistency checking
- **Documentation Integration**: Process and link HTML documentation with data files

## Features

### Core Features

- ✅ **Multi-threaded Downloads** - Parallel processing for faster data acquisition
- ✅ **Resumable Operations** - Continue interrupted downloads and conversions
- ✅ **Memory Efficient** - Chunked processing for handling large datasets
- ✅ **Type Safety** - Comprehensive type hints throughout the codebase
- ✅ **Robust Error Handling** - Retry logic and graceful failure recovery
- ✅ **Structured Logging** - Detailed logging with rotation and retention policies
- ✅ **Flexible Configuration** - Dataclass-based configuration management
- ✅ **Progress Tracking** - Real-time feedback for long-running operations

### Data Processing Features

- 📊 **Column Mapping** - Apply human-readable column names from JSON mappings
- 🔄 **Batch Processing** - Process multiple files efficiently
- 📁 **Intelligent Organization** - Automatic file classification by type and year
- 🔗 **Data Merging** - Combine files with identical columns
- 📝 **Metadata Extraction** - Extract information from HTML documentation

## Architecture

### System Architecture

```
┌─────────────────┐
│   CDC Website   │
└────────┬────────┘
         │ HTTP/HTTPS
         ▼
┌─────────────────┐
│  get_data.py    │◄──── URL List (NHANES_URLS.txt)
│  (Downloader)   │
└────────┬────────┘
         │ XPT Files
         ▼
┌─────────────────┐
│ raw_to_csv.py   │◄──── Column Mappings (JSON)
│  (Converter)    │
└────────┬────────┘
         │ CSV Files
         ▼
┌─────────────────┐
│  analyse.py     │◄──── Configuration
│  (Analyzer)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Output Files   │
│  - Merged CSV   │
│  - Classified   │
│  - Metadata     │
└─────────────────┘
```

### Data Flow Pipeline

```
1. Download Stage (get_data.py)
   ├── Parse NHANES website URLs
   ├── Extract XPT file links
   ├── Download with retry logic
   └── Organize by year/component

2. Conversion Stage (raw_to_csv.py)
   ├── Load XPT files
   ├── Apply column mappings
   ├── Convert to CSV format
   └── Maintain directory structure

3. Analysis Stage (analyse.py)
   ├── Initialize file dictionary
   ├── Check column consistency
   ├── Merge similar files
   ├── Classify by type
   └── Extract HTML metadata
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Internet connection for downloading NHANES data

### Install from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/MDP-Data-Analyse.git
cd MDP-Data-Analyse

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Dependencies

Core dependencies include:
- `pandas` - Data manipulation and XPT/CSV conversion
- `beautifulsoup4` - HTML parsing for web scraping
- `loguru` - Advanced logging functionality
- `PyYAML` - YAML file operations

See [requirements.txt](requirements.txt) for the complete list.

## Quick Start

### 1. Update Download URLs

```bash
# Fetch latest NHANES data URLs from CDC website
python get_data.py -u
```

### 2. Download Data Files

```bash
# Download specific components with multi-threading
python get_data.py -d -m -c Demographics Dietary Laboratory
```

### 3. Convert XPT to CSV

```bash
# Convert all XPT files to CSV with column mapping
python raw_to_csv.py -m -c
```

### 4. Analyze Data

```bash
# Run complete analysis pipeline
python analyse.py --all
```

## Usage Scenarios

### Scenario 1: Research Project Setup

**Goal**: Set up a new research project with specific NHANES components

```bash
# Step 1: Download demographics and laboratory data for analysis
python get_data.py -d -c Demographics Laboratory -o ./research_data/raw/

# Step 2: Convert to CSV with meaningful column names
python raw_to_csv.py -i ./research_data/raw/ -o ./research_data/csv/ -c

# Step 3: Check column consistency for merging
python analyse.py -i ./research_data/csv/ --check

# Step 4: Merge files with same columns
python analyse.py -i ./research_data/csv/ --merge
```

### Scenario 2: Updating Existing Dataset

**Goal**: Add new survey years to existing dataset

```bash
# Update URLs to get latest survey data
python get_data.py -u

# Download only new files (existing files are skipped)
python get_data.py -d -m

# Convert and integrate new data
python raw_to_csv.py -m
python analyse.py --merge --classify
```

### Scenario 3: Large-Scale Data Processing

**Goal**: Process entire NHANES dataset efficiently

```bash
# Use multi-threading for all operations
python get_data.py -d -m -c Demographics Dietary Examination Laboratory Questionnaire

# Convert with parallel processing
python raw_to_csv.py -m -c

# Run complete analysis pipeline
python analyse.py --all
```

### Scenario 4: Custom Analysis Pipeline

**Goal**: Create custom analysis workflow

```python
from pathlib import Path
from analyse import CSVAnalyzer, AnalysisConfig, AnalysisStep

# Custom configuration
config = AnalysisConfig(
    csv_data_dir=Path('./custom_data/csv'),
    output_dir=Path('./custom_output'),
    chunk_size=50000  # Larger chunks for better performance
)

# Initialize analyzer
analyzer = CSVAnalyzer(config)

# Run specific steps
analyzer.run_analysis([
    AnalysisStep.INIT_FILE_DICT,
    AnalysisStep.CHECK_COLUMNS,
    AnalysisStep.MERGE_CSV
])
```

## Code Structure

### Project Structure

```
MDP-Data-Analyse/
├── get_data.py           # Data download module
├── raw_to_csv.py         # XPT to CSV conversion module
├── analyse.py            # Data analysis module
├── common.py             # Shared utilities
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore patterns
├── README.md            # This file
├── NHANES_URLS.txt      # Source URLs for NHANES data
├── CLAUDE.md            # AI assistant instructions
│
├── data/                # Data directory (git-ignored)
│   ├── raw_data/        # Downloaded XPT files
│   ├── csv_data/        # Converted CSV files
│   ├── merge_csv/       # Merged datasets
│   └── classified_csv/  # Classified data files
│
├── org_urls/            # Original URLs by component
├── urls/                # URL tracking files
└── urls_html/           # HTML content URLs
```

### Module Descriptions

#### get_data.py
Main classes and functions:
- `ComponentType`: Enum for NHANES data components
- `DownloadConfig`: Configuration dataclass
- `NHANESDownloader`: Main download orchestrator
  - `parse_xpt_urls()`: Extract XPT URLs from HTML
  - `download_file()`: Download with retry logic
  - `process_data_file()`: Process individual files
  - `update_urls()`: Update URL database

#### raw_to_csv.py
Main classes and functions:
- `ConversionConfig`: Configuration dataclass
- `XPTConverter`: File conversion handler
  - `load_column_mappings()`: Load JSON column maps
  - `convert_xpt_to_csv()`: Convert single file
  - `process_directory()`: Batch conversion
  - `convert_all()`: Orchestrate conversion

#### analyse.py
Main classes and functions:
- `AnalysisStep`: Enum for pipeline steps
- `AnalysisConfig`: Configuration dataclass
- `FileClassifier`: File organization utilities
- `CSVAnalyzer`: Main analysis engine
  - `initialize_file_dict()`: Create file inventory
  - `check_csv_columns()`: Verify column consistency
  - `merge_csv_files()`: Combine similar files
  - `classify_csv_files()`: Organize by type

#### common.py
Utility functions:
- `conditionalMkdir()`: Safe directory creation
- `getFilePathDict()`: File discovery
- `saveDictToJsonfile()`: JSON persistence
- `readJsonFile()`: JSON loading
- `get_file_stats()`: File metadata

## Framework Design

### Design Principles

1. **Separation of Concerns**: Each module handles a specific aspect of the pipeline
2. **Configuration-Driven**: Dataclasses provide centralized configuration
3. **Error Recovery**: Comprehensive error handling with retry mechanisms
4. **Type Safety**: Full type annotations for better IDE support and error detection
5. **Scalability**: Support for both single-threaded and parallel processing
6. **Extensibility**: Easy to add new components and analysis steps

### Core Components

#### Configuration Management

```python
@dataclass
class DownloadConfig:
    """Centralized configuration using dataclasses"""
    base_url: str = 'https://wwwn.cdc.gov'
    output_dir: Path = Path('./data/raw_data/')
    max_retries: int = 3
    chunk_size: int = 8192
    
    def __post_init__(self):
        """Auto-create directories on initialization"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
```

#### Error Handling Strategy

```python
def download_file(self, file_url: str, file_path: Path, retry_count: int = 0) -> bool:
    """Download with exponential backoff retry"""
    try:
        # Download logic
        return True
    except urllib.error.URLError as e:
        if retry_count < self.config.max_retries:
            # Retry with backoff
            return self.download_file(file_url, file_path, retry_count + 1)
        else:
            logger.error(f"Failed after {self.config.max_retries} retries")
            return False
```

#### Pipeline Architecture

```python
class CSVAnalyzer:
    """Pipeline-based analysis with composable steps"""
    
    def run_analysis(self, steps: List[AnalysisStep] = None) -> None:
        """Execute analysis pipeline with selected steps"""
        for step in steps:
            if step == AnalysisStep.INIT_FILE_DICT:
                self.initialize_file_dict()
            elif step == AnalysisStep.CHECK_COLUMNS:
                self.check_csv_columns()
            # ... additional steps
```

### Extension Points

The framework is designed to be easily extended:

1. **New Data Sources**: Add new component types to `ComponentType` enum
2. **Custom Processing**: Extend analyzer classes with new methods
3. **Pipeline Steps**: Add new `AnalysisStep` enum values
4. **Configuration**: Extend dataclasses with additional parameters

## API Reference

### get_data Module

```python
class NHANESDownloader:
    def __init__(self, config: DownloadConfig = None)
    def parse_xpt_urls(self, html_content: str) -> List[str]
    def download_file(self, file_url: str, file_path: Path) -> bool
    def update_urls(self, urls: List[str], multithread: bool = False)
    def download_files(self, components: Set[str], multithread: bool = False)
```

### raw_to_csv Module

```python
class XPTConverter:
    def __init__(self, config: ConversionConfig = None)
    def load_column_mappings(self, walk_paths: List[Tuple]) -> Dict[str, str]
    def convert_xpt_to_csv(self, xpt_path: Path, csv_path: Path) -> bool
    def convert_all(self, multithread: bool = False)
```

### analyse Module

```python
class CSVAnalyzer:
    def __init__(self, config: AnalysisConfig = None)
    def initialize_file_dict(self) -> Dict[str, str]
    def check_csv_columns(self) -> Tuple[Dict, Dict]
    def merge_csv_files(self, file_list: List[str], output_path: Path) -> bool
    def run_analysis(self, steps: List[AnalysisStep] = None)
```

## Configuration

### Environment Variables

```bash
# Set custom data directory
export NHANES_DATA_DIR=/path/to/data

# Set log level
export LOG_LEVEL=DEBUG
```

### Configuration Files

Create a `config.json` for custom settings:

```json
{
    "download": {
        "max_retries": 5,
        "chunk_size": 16384,
        "timeout": 60
    },
    "conversion": {
        "chunk_size": 20000,
        "use_column_mapping": true
    },
    "analysis": {
        "merge_chunk_size": 50000
    }
}
```

## Examples

### Example 1: Download Specific Years

```python
from get_data import NHANESDownloader, DownloadConfig
from pathlib import Path

config = DownloadConfig(
    output_dir=Path('./data/2017-2018/'),
)

downloader = NHANESDownloader(config)
downloader.download_files({'Demographics', 'Laboratory'})
```

### Example 2: Custom Column Mapping

```python
from raw_to_csv import XPTConverter, ConversionConfig

config = ConversionConfig(
    input_dir=Path('./raw_data/'),
    output_dir=Path('./processed_data/'),
    use_column_mapping=True,
    chunk_size=50000
)

converter = XPTConverter(config)
converter.convert_all(multithread=True)
```

### Example 3: Selective Analysis

```python
from analyse import CSVAnalyzer, AnalysisStep

analyzer = CSVAnalyzer()
analyzer.run_analysis([
    AnalysisStep.CHECK_COLUMNS,
    AnalysisStep.MERGE_CSV
])
```

## Troubleshooting

### Common Issues

1. **Download Failures**
   - Check internet connection
   - Verify NHANES_URLS.txt exists
   - Review logs in NhanesDownload.log

2. **Conversion Errors**
   - Ensure XPT files are not corrupted
   - Check available disk space
   - Verify pandas version compatibility

3. **Memory Issues**
   - Reduce chunk_size in configuration
   - Process fewer files at once
   - Use single-threaded mode

### Debug Mode

Enable detailed logging:

```python
from loguru import logger
logger.add("debug.log", level="DEBUG")
```

## Performance Optimization

### Tips for Large Datasets

1. **Use Multi-threading**
   ```bash
   python get_data.py -d -m
   python raw_to_csv.py -m
   ```

2. **Adjust Chunk Sizes**
   - Larger chunks = faster but more memory
   - Smaller chunks = slower but less memory

3. **Process in Batches**
   - Download components separately
   - Convert year by year

4. **Monitor Resources**
   ```bash
   # Monitor memory usage
   watch -n 1 free -h
   
   # Monitor disk I/O
   iotop
   ```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Format code
black .

# Type checking
mypy .
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- CDC/NHANES for providing public health data
- Contributors and maintainers
- Open source community

## Contact

For questions or support, please open an issue on GitHub or contact the maintainers.

---

**Note**: This tool is not affiliated with or endorsed by the CDC or NHANES. It is an independent tool for researchers to access publicly available data.
