from pathlib import PurePosixPath
import sqlite3
import sys
import click
from ftplib import FTP
from loguru import logger
from urllib.parse import ParseResult as UrlParseResult
from urllib.parse import urlparse

from ftp_scan.scan import *

@click.command()
@click.argument('ftp_addr')
@click.argument('output', type=click.Path())
@click.option('--encoding', help='The character encoding to be used in the FTP connection', default='utf8')
def main_cli(ftp_addr: str, output: str, encoding: str):
    # logger.add("ftp-scan.log", backtrace=True, diagnose=True)
    scanner = FTPScanner(ftp_addr, '', output, encoding=encoding)
    scanner.get_basics()
    scanner.recursive_scan_dir(PurePosixPath('/'))
    scanner.close()