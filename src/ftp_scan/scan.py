# trick: run NLST and LIST so that we can compare the
# outputs and get the extra info from each entry
#
from __future__ import annotations
from abc import abstractmethod, abstractproperty
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from ftplib import FTP
import ftplib
import mimetypes
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, urlunparse
import regex as re
from loguru import logger
import sqlite3
from typing import Dict, List, Optional, Tuple

UnixDirRegex = re.compile(r"^(?<inode_type>\S)(?<permissions>(?:[r-][w-][x-]){3})\s+[0-9]+\s(?<user>\S+)\s+(?<group>\S+)\s+(?<size>\S+)\s+(?<month>\w{3})\s+(?<day>\d{1,2})\s+((?<year>\d{4})|(?<hour>\d{1,2}):(?<minute>\d{1,2}))\s+(?<filename>.*\S)\s*$")

MsDosDirRegex = re.compile(r"^(?<month>[0-9]{2})-(?<day>[0-9]{2})-(?<year>[0-9]{2})\s+(?<hour>[0-9]{2}):(?<minute>[0-9]{2})(?<ampm>AM|PM)\s+(?<inode_type><DIR>)?\s+(?<size>\d+)?\s+(?<filename>.*\S)\s*$")

class FTPFlavour(StrEnum):
    Unix = 'Unix'
    MsDos = 'MsDos'

month2num = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12
}

@dataclass
class FileNode:
    path: PurePosixPath
    modification_date: Optional[datetime] = None
    error_msg: Optional[str] = None

    @abstractproperty
    def is_dir(self) -> bool:
        pass

    @abstractmethod
    def to_sqlite_tuple(self) -> Tuple:
        pass

    @abstractmethod
    def save(self, cur: sqlite3.Cursor):
        pass

    @staticmethod
    def new_from_dir_line(flavour: FTPFlavour, path: PurePosixPath, line: str) -> FileNode:
        with logger.contextualize(line=line):
            if flavour == FTPFlavour.Unix:
                m = UnixDirRegex.match(line)
                assert m is not None
                filename = m.groupdict()['filename']
                mod_date = None
                try:
                    if m.groupdict()['year'] is not None:
                        year = int(m.groupdict()['year'])
                        day = int(m.groupdict()['day'])
                        mod_date = datetime(year, month2num[m.groupdict()['month']], day)
                    else:
                        year = datetime.now().year
                        day = int(m.groupdict()['day'])
                        hour = int(m.groupdict()['hour'])
                        minute = int(m.groupdict()['minute'])
                        mod_date = datetime(year, month2num[m.groupdict()['month']], day, hour, minute)
                except Exception as e:
                    logger.opt(exception=True).warning(f'Failed to parse date in: {str(e)}')
                inode_type = m.groupdict()['inode_type']
                if inode_type == 'd':
                    return Directory(path / filename, mod_date, None, None)
                elif inode_type == '-':
                    ans = RegularFile(path / filename, mod_date, None, None, m.groupdict()['size'])
                    ans.guess_mime()
                    return ans
            elif flavour == FTPFlavour.MsDos:
                m = MsDosDirRegex.match(line)
                assert m is not None
                filename = m.groupdict()['filename']
                mod_date = None
                try:
                    year = int(m.groupdict()['year'])
                    month = int(m.groupdict()['month'])
                    day = int(m.groupdict()['day'])
                    hour = int(m.groupdict()['hour'])
                    minute = int(m.groupdict()['minute'])
                    ampm = m.groupdict()['ampm']

                    if 70 <= year <= 99:
                        year += 1900
                    else:
                        year += 2000

                    if ampm == 'PM':
                        hour += 12
                    if hour >= 24:
                        hour -= 24

                    mod_date = datetime(year, month, day, hour, minute)
                except Exception as e:
                    logger.opt(exception=True).warning(f'Failed to parse date in: {str(e)}')
                if m.groupdict()['inode_type'] == '<DIR>':
                    return Directory(path / filename, mod_date, None, None)
                else:
                    ans = RegularFile(path / filename, mod_date, None, None, m.groupdict()['size'])
                    ans.guess_mime()
                    return ans

@dataclass
class RegularFile(FileNode):
    mime: Optional[str] = None
    size: Optional[int] = None

    @property
    def is_dir(self) -> bool:
        return False

    def guess_mime(self):
        self.mime = mimetypes.guess_type(self.path)[0]

    def to_sqlite_tuple(self) -> Tuple:
        return (str(self.path), self.mime, self.size, self.modification_date, self.error_msg)

    def save(self, cur: sqlite3.Cursor):
        cur.execute("REPLACE INTO files (path, mime, size, modification_date, error_msg) VALUES (?, ?, ?, ?, ?);", self.to_sqlite_tuple())

    @staticmethod
    def create_table(cur: sqlite3.Cursor):
        cur.execute("CREATE TABLE IF NOT EXISTS files (path PRIMARY KEY, mime, size, modification_date, error_msg);")

@dataclass
class Directory(FileNode):
    num_children: Optional[int] = None

    @property
    def is_dir(self) -> bool:
        return True

    def to_sqlite_tuple(self) -> Tuple:
        return (str(self.path), self.num_children, self.modification_date, self.error_msg)

    def save(self, cur: sqlite3.Cursor):
        cur.execute("REPLACE INTO directories (path, num_children, modification_date, error_msg) VALUES (?, ?, ?, ?);", self.to_sqlite_tuple())

    @staticmethod
    def load(directory: PurePosixPath, cur: sqlite3.Cursor) -> Optional[Directory]:
        res = cur.execute("SELECT path, num_children, modification_date, error_msg FROM directories WHERE path = ?", (str(directory),))
        line = res.fetchone()
        if line is None:
            return None
        else:
            return Directory(path=line[0], num_children=line[1], modification_date=line[2], error_msg=line[3])

    @staticmethod
    def create_table(cur: sqlite3.Cursor):
        cur.execute("CREATE TABLE IF NOT EXISTS directories (path PRIMARY KEY, num_children, modification_date, error_msg);")

class FTPScanner:
    ftp: FTP
    con: sqlite3.Connection
    flavour: Optional[FTPFlavour]

    def __init__(self, ftp_addr: str, password: str, sqlite_path: str|Path, encoding: str="utf8"):
        addr = urlparse(ftp_addr, scheme='ftp')
        if addr.netloc == '':
            addr = addr._replace(netloc=addr.path, path='')
        logger.info(f"Connecting to {urlunparse(addr)}")
        self.ftp = FTP(addr.netloc, addr.username, password, encoding=encoding)
        logger.info(f"Logging as {addr.username}")
        self.ftp.login()
        logger.info(f"opening SQLite database at {sqlite_path}")
        self.con = sqlite3.connect(sqlite_path)

        cur = self.con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS general (key PRIMARY KEY, value);")
        RegularFile.create_table(cur)
        Directory.create_table(cur)
        cur.close()
        self.con.commit()

    def get_basics(self):
        cur = self.con.cursor()
        logger.debug(f"Saving to general table key='HOST' value={self.ftp.host!r}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("host", self.ftp.host))

        welcome_msg = self.ftp.getwelcome()
        logger.info(f"FTP welcome message: {welcome_msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("welcome_msg", welcome_msg))

        msg = self.ftp.sendcmd("STAT")
        logger.debug(f"FTP STAT output: {msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("STAT", msg))

        msg = self.ftp.sendcmd("HELP")
        logger.debug(f"FTP HELP output: {msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("HELP", msg))

        msg = self.ftp.sendcmd("FEAT")
        logger.debug(f"FTP FEAT output: {msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("FEAT", msg))

        msg = self.ftp.sendcmd("SYST")
        logger.info(f"FTP SYST output: {msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("SYST", msg))
        if "Windows_NT" in msg:
            self.flavour = FTPFlavour.MsDos
        elif "UNIX" in msg:
            self.flavour = FTPFlavour.Unix
        else:
            raise ValueError(f"Unknown flavour {msg}")

        cur.close()
        self.con.commit()

    def scan_dir(self, directory: PurePosixPath) -> List[FileNode]:
        with logger.contextualize(wd=directory):
            cur = self.con.cursor()
            this_dir = Directory.load(directory, cur)
            if this_dir is None:
                this_dir = Directory(directory)

            try:
                logger.info(f'Scanning {str(directory)}')
                self.ftp.cwd(str(directory))
                dir_listing = []
                self.ftp.retrlines('LIST', dir_listing.append)

                this_dir.num_children = len(dir_listing)
                this_dir.save(cur)

                output: List[FileNode] = []
                for line in dir_listing:
                    try:
                        node: FileNode = FileNode.new_from_dir_line(self.flavour, directory, line)
                        # logger.debug(node)
                        node.save(cur)
                        output.append(node)
                    except Exception as e:
                        logger.opt(exception=True).error(f'Failed to parse entry line {line!r}: {str(e)}')
                cur.close()
                self.con.commit()
                return output
            except ftplib.error_perm as e:
                logger.warning(f'No permission to scan {str(directory)}')
                this_dir.error_msg = str(e)
                this_dir.save(cur)
                return []


    def recursive_scan_dir(self, directory: PurePosixPath):
        with logger.contextualize(wd=directory):
            for node in self.scan_dir(directory):
                if isinstance(node, Directory):
                    self.recursive_scan_dir(node.path)

    def close(self):
        self.con.close()
        self.ftp.close()