from pathlib import PurePosixPath
import sqlite3
import click
from ftplib import FTP
from urllib.parse import ParseResult as UrlParseResult
from urllib.parse import urlparse

from ftp_scan.scan import *

@click.command()
@click.argument('ftp_addr')
@click.argument('output', type=click.Path())
@click.option('--encoding', help='The character encoding to be used in the FTP connection')
def main_cli(ftp_addr: str, output: str, encoding: str="utf8"):
    addr = urlparse(ftp_addr)
    ftp = FTP(addr.netloc, addr.username, addr.password, encoding=encoding)
    ftp.login()
    con = sqlite3.connect(output)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS nodes (path PRIMARY KEY, name, mime, size, permission_error, details);")
    recursive_scan(ftp, PurePosixPath('/cnes/TxtCapt'), con)
    cur.close()
    con.commit()
    con.close()