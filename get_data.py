#!/usr/bin/env python3
"""
NHANES Data Download Module

This module handles downloading NHANES data files from the CDC website.
It supports both single-threaded and multi-threaded downloads, with
proper error handling and resumable downloads.

Copyright (C) 2022 DeShengHong All Rights Reserved.
Author: Ink Huang
"""

import os
import sys
import argparse
import urllib.request
import urllib.error
import re
import functools
from pathlib import Path
from typing import List, Set, Optional, Tuple
from multiprocessing import Pool
from dataclasses import dataclass
from enum import Enum

from loguru import logger
from bs4 import BeautifulSoup

from common import conditionalMkdir


class ComponentType(Enum):
    """NHANES data component types"""
    DEMOGRAPHICS = 'Demographics'
    DIETARY = 'Dietary'
    EXAMINATION = 'Examination'
    LABORATORY = 'Laboratory'
    QUESTIONNAIRE = 'Questionnaire'
    OTHER = 'Other'


@dataclass
class DownloadConfig:
    """Configuration for download operations"""
    base_url: str = 'https://wwwn.cdc.gov'
    output_dir: Path = Path('./data/raw_data/')
    url_list_file: Path = Path('./NHANES_URLS.txt')
    org_urls_dir: Path = Path('./org_urls/')
    urls_html_dir: Path = Path('./urls_html/')
    urls_dir: Path = Path('./urls/')
    max_retries: int = 3
    chunk_size: int = 8192
    batch_size: int = 10
    
    def __post_init__(self):
        """Ensure all directory paths exist"""
        for dir_path in [self.output_dir, self.org_urls_dir, self.urls_html_dir, self.urls_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)


class NHANESDownloader:
    """Main class for downloading NHANES data files"""
    
    def __init__(self, config: DownloadConfig = None):
        """Initialize downloader with configuration"""
        self.config = config or DownloadConfig()
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
            "NhanesDownload.log",
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {module}:{function}:{line} | {message}"
        )
    
    def parse_xpt_urls(self, html_content: str) -> List[str]:
        """
        Extract all XPT file URLs from HTML content
        
        Args:
            html_content: HTML source code
            
        Returns:
            List of XPT file URLs
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            xpt_links = soup.find_all('a', href=re.compile(r'\.XPT$', re.IGNORECASE))
            return [link['href'] for link in xpt_links]
        except Exception as e:
            logger.error(f"Failed to parse HTML content: {e}")
            return []
    
    def extract_file_year(self, file_url: str) -> str:
        """
        Extract year from file URL
        
        Args:
            file_url: URL of the file
            
        Returns:
            Year string (e.g., '2017-2018') or 'Other'
        """
        year_match = re.search(r'/(\d{4}-\d{4})/', file_url)
        return year_match.group(1) if year_match else 'Other'
    
    def extract_component_type(self, url: str) -> str:
        """
        Extract component type from URL
        
        Args:
            url: URL containing component information
            
        Returns:
            Component type string
        """
        type_match = re.search(r'Component=([a-zA-Z]+)', url)
        if type_match:
            component = type_match.group(1)
            try:
                return ComponentType[component.upper()].value
            except KeyError:
                return component
        return ComponentType.OTHER.value
    
    def download_file(self, file_url: str, file_path: Path, retry_count: int = 0) -> bool:
        """
        Download a file with retry logic and progress tracking
        
        Args:
            file_url: URL of the file to download
            file_path: Local path to save the file
            retry_count: Current retry attempt
            
        Returns:
            True if download successful, False otherwise
        """
        if file_path.exists():
            logger.debug(f"File already exists: {file_path}")
            return True
        
        try:
            with urllib.request.urlopen(file_url, timeout=30) as response:
                file_size = int(response.headers.get('Content-Length', 0))
                
                with open(file_path, 'wb') as f:
                    downloaded = 0
                    while True:
                        chunk = response.read(self.config.chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if file_size > 0:
                            progress = (downloaded / file_size) * 100
                            if downloaded % (self.config.chunk_size * 100) == 0:
                                logger.debug(f"Progress: {progress:.1f}% for {file_path.name}")
                
                logger.info(f"Successfully downloaded: {file_path.name}")
                return True
                
        except urllib.error.URLError as e:
            if retry_count < self.config.max_retries:
                logger.warning(f"Download failed, retrying ({retry_count + 1}/{self.config.max_retries}): {e}")
                return self.download_file(file_url, file_path, retry_count + 1)
            else:
                logger.error(f"Failed to download after {self.config.max_retries} retries: {file_url}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {file_url}: {e}")
            return False
    
    def process_data_file(self, file_url: str, component_type: str) -> bool:
        """
        Process and download a single data file
        
        Args:
            file_url: URL of the data file
            component_type: Type of component (Demographics, Dietary, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        year = self.extract_file_year(file_url)
        file_dir = self.config.output_dir / year / component_type
        file_dir.mkdir(parents=True, exist_ok=True)
        
        file_name = file_url.split('/')[-1]
        file_path = file_dir / file_name
        
        return self.download_file(file_url, file_path)
    
    def parse_website(self, url: str) -> List[str]:
        """
        Parse website and extract data file URLs
        
        Args:
            url: Website URL to parse
            
        Returns:
            List of extracted file URLs
        """
        try:
            # Validate URL
            if not url.startswith(('http://', 'https://')):
                logger.error(f"Invalid URL format: {url}")
                return []
            
            component_type = self.extract_component_type(url)
            
            # Download HTML content
            with urllib.request.urlopen(url, timeout=30) as response:
                html_content = response.read().decode('utf-8')
            
            # Parse XPT URLs
            relative_urls = self.parse_xpt_urls(html_content)
            
            # Convert to absolute URLs
            base_url = '/'.join(url.split('/')[:3])
            absolute_urls = [f"{base_url}{rel_url}" for rel_url in relative_urls]
            
            # Save URLs to file
            urls_file = self.config.org_urls_dir / component_type
            urls_file.write_text('\n'.join(absolute_urls))
            logger.info(f"Saved {len(absolute_urls)} URLs for {component_type}")
            
            return absolute_urls
            
        except Exception as e:
            logger.error(f"Failed to parse website {url}: {e}")
            return []
    
    def download_component_files(self, component_type: str) -> None:
        """
        Download all files for a specific component type
        
        Args:
            component_type: Type of component to download
        """
        urls_file = self.config.urls_html_dir / component_type
        
        if not urls_file.exists():
            logger.error(f"URLs file not found: {urls_file}")
            return
        
        urls = [url.strip() for url in urls_file.read_text().splitlines() if url.strip()]
        total_files = len(urls)
        
        if total_files == 0:
            logger.warning(f"No URLs found for {component_type}")
            return
        
        logger.info(f"Starting download of {total_files} {component_type} files")
        
        successful = 0
        failed_urls = []
        
        for i, url in enumerate(urls, 1):
            logger.info(f"Processing {component_type} file {i}/{total_files}")
            
            if self.process_data_file(url, component_type):
                successful += 1
            else:
                failed_urls.append(url)
            
            # Save progress periodically
            if i % self.config.batch_size == 0 or i == total_files:
                remaining_urls = urls[i:] + failed_urls
                urls_file.write_text('\n'.join(remaining_urls))
                logger.info(f"Progress saved: {successful}/{i} files downloaded successfully")
        
        # Final save of failed URLs
        if failed_urls:
            failed_file = self.config.urls_dir / f"{component_type}_failed"
            failed_file.write_text('\n'.join(failed_urls))
            logger.warning(f"Failed to download {len(failed_urls)} files. Saved to {failed_file}")
        
        logger.info(f"Completed {component_type}: {successful}/{total_files} files downloaded")
    
    def update_urls(self, urls: List[str], multithread: bool = False) -> None:
        """
        Update download URLs from NHANES website
        
        Args:
            urls: List of website URLs to parse
            multithread: Whether to use multiprocessing
        """
        logger.info(f"Updating URLs from {len(urls)} sources")
        
        if multithread:
            with Pool(processes=os.cpu_count()) as pool:
                pool.map(self.parse_website, urls)
        else:
            for url in urls:
                self.parse_website(url)
    
    def download_files(self, components: Set[str], multithread: bool = False) -> None:
        """
        Download files for specified components
        
        Args:
            components: Set of component types to download
            multithread: Whether to use multiprocessing
        """
        logger.info(f"Downloading files for components: {components}")
        
        if multithread:
            with Pool(processes=os.cpu_count()) as pool:
                pool.map(self.download_component_files, components)
        else:
            for component in components:
                self.download_component_files(component)


def main():
    """Main entry point for the download script"""
    parser = argparse.ArgumentParser(
        description='Download NHANES data files from CDC website',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '-o', '--output_dir',
        type=str,
        default='./data/raw_data/',
        help='Directory for saving downloaded files'
    )
    
    parser.add_argument(
        '-m', '--multithread',
        action='store_true',
        help='Use multiprocessing for parallel operations'
    )
    
    parser.add_argument(
        'url_list',
        type=str,
        default='./NHANES_URLS.txt',
        nargs='?',
        help='Text file containing URLs to NHANES website data listings'
    )
    
    parser.add_argument(
        '-u', '--update',
        action='store_true',
        help='Update NHANES download URLs from website'
    )
    
    parser.add_argument(
        '-d', '--download',
        action='store_true',
        help='Download files from NHANES URLs'
    )
    
    parser.add_argument(
        '-c', '--components',
        type=str,
        nargs='+',
        choices=['Demographics', 'Dietary', 'Examination', 'Laboratory', 'Questionnaire'],
        default=['Dietary'],
        help='Component types to download'
    )
    
    args = parser.parse_args()
    
    # Create configuration
    config = DownloadConfig(
        output_dir=Path(args.output_dir),
        url_list_file=Path(args.url_list)
    )
    
    # Initialize downloader
    downloader = NHANESDownloader(config)
    
    # Process commands
    if args.update:
        if not config.url_list_file.exists():
            logger.error(f"URL list file not found: {config.url_list_file}")
            sys.exit(1)
        
        urls = [url.strip() for url in config.url_list_file.read_text().splitlines() if url.strip()]
        downloader.update_urls(urls, args.multithread)
    
    if args.download:
        components = set(args.components)
        downloader.download_files(components, args.multithread)
    
    if not args.update and not args.download:
        parser.print_help()


if __name__ == '__main__':
    main()