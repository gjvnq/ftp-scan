from pathlib import PurePosixPath
import sqlite3
import sys
import click
from ftplib import FTP
from loguru import logger
from urllib.parse import ParseResult as UrlParseResult
from urllib.parse import urlparse

from ftp_scan.scan import *

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.group(context_settings=CONTEXT_SETTINGS)
def main_cli():
    pass

@main_cli.command()
@click.argument('ftp_url', metavar='FTP_URL')
@click.argument('sqlite_output_path', metavar='SQLITE_OUTPUT', type=click.Path())
@click.option('--dir', 'ftp_dir', help='The FTP directory to scan', default='/', envvar='FTP_SCAN_DIR')
@click.option('--user', 'username', help='The FTP username', envvar='FTP_SCAN_USER')
@click.option('--pass', 'password', help='The FTP password', envvar='FTP_SCAN_PASS')
@click.option('--encoding', help='The character encoding to be used in the FTP connection', default='utf8', envvar='FTP_SCAN_ENCODING')
@click.option('--timeout', help='FTP connection timeout in seconds', default=None, envvar='FTP_SCAN_TIMEOUT')
@click.option('--log-path', help='A log file', default=None, type=click.Path(), envvar='FTP_SCAN_LOG_PATH')
@click.option('--source_address', help='Specify the IP address and port for the server', default=None, envvar='FTP_SCAN_LOG_SRC_ADDR')
@click.option('--flavour', help='The directory listing flavour', type=click.Choice(FTPFlavour._member_names_))
def basic_scan(ftp_url: str, sqlite_output_path: str, ftp_dir: str, username: str, password: str, encoding: str, timeout: Optional[int], log_path: Optional[str], source_address: Optional[str], flavour: Optional[FTPFlavour]):
    """Scans an FTP directory and saves the result to an SQLITE database.

    FTP_URL may contain an username, a password and a path but the explicit arguments --dir, --user, and --pass take priority. Begin your FTP_URL with sftp:// if you want to use an SSL connection.

    In case of encoding errors, try using --encoding latin1.
    """
    # Parse and process arguments
    if log_path is not None:
        logger.add(log_path, backtrace=True, diagnose=True)
    src_addr = None
    if source_address is not None:
        source_address_parts = source_address.split(":")
        src_addr = (source_address_parts[0], int(source_address_parts[1]))
    parsed_ftp_url = urlparse(ftp_url)
    if parsed_ftp_url.path != '' and ftp_dir == '/':
        ftp_dir = parsed_ftp_url.path

    # Connect and do the work
    scanner = FTPScanner(ftp_url, sqlite_output_path, username, password, encoding=encoding, timeout=timeout, source_address=src_addr, flavour=flavour)
    scanner.get_basics()
    scanner.recursive_scan_dir(PurePosixPath(ftp_dir))
    scanner.close()
