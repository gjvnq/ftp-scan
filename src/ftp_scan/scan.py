# trick: run NLST and LIST so that we can compare the
# outputs and get the extra info from each entry
#
from __future__ import annotations
from abc import abstractmethod, abstractproperty
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from ftplib import FTP, FTP_TLS
import ftplib
from functools import partial
import json
import mimetypes
import os
from pathlib import Path, PurePosixPath
from queue import Queue
import select
import threading
from urllib.parse import urlparse, urlunparse
import regex as re
from loguru import logger
import sqlite3
from typing import Dict, Generator, Iterable, List, Optional, Tuple

UnixDirRegex = re.compile(r"^(?<inode_type>\S)(?<permissions>(?:[r-][w-][xs-]){3})\s+[0-9]+\s(?<user>\S+)\s+(?<group>\S+)\s+(?<size>\S+)\s+(?<month>\w{3})\s+(?<day>\d{1,2})\s+((?<year>\d{4})|(?<hour>\d{1,2}):(?<minute>\d{1,2}))\s+(?<filename>.*\S)\s*$")
UnixDirRegex_SymLink = re.compile(r"^(?<source>.*\S) -> (?<target>.*\S)$")

MsDosDirRegex = re.compile(r"^(?<month>[0-9]{2})-(?<day>[0-9]{2})-(?<year>[0-9]{2})\s+(?<hour>[0-9]{2}):(?<minute>[0-9]{2})(?<ampm>AM|PM)\s+(?<inode_type><DIR>)?\s+(?<size>\d+)?\s+(?<filename>.*\S)\s*$")

class FTPFlavour(StrEnum):
    Unix = 'unix'
    MsDos = 'ms-dos'
    Mlsd = 'mlsd'

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

def parse_ftp_date(date_str: Optional[str]) -> Optional[datetime]:
    if date_str is None:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d%H%M%S")
    except Exception as e:
        logger.warning(f'Failed to parse date: {date_str}')
        return None

@dataclass
class FileNode:
    path: PurePosixPath
    modification_date: Optional[datetime] = None
    error_msg: Optional[str] = None
    extra: Optional[str] = None

    @abstractproperty
    def is_dir(self) -> Optional[bool]:
        pass

    @abstractmethod
    def to_sqlite_tuple(self) -> Tuple:
        pass

    @abstractmethod
    def save(self, cur: sqlite3.Cursor):
        pass

    @staticmethod
    def new_from_mlsd_line(flavour: FTPFlavour, working_path: PurePosixPath, entry: Tuple[str, Dict[str]]) -> FileNode:
        with logger.contextualize(wd=working_path, entry=entry):
            filename, entry = entry
            mod_date = parse_ftp_date(entry.get('modify', None))
            entry_as_json = json.dumps(entry)
            filesize = entry.get('size', None)
            filesize = int(filesize) if filesize is not None else None
            if entry['type'] == 'file':
                ans = RegularFile(path=working_path / filename, modification_date=mod_date, extra=entry_as_json, size=filesize)
                ans.guess_mime()
                return ans
            elif entry['type'] in ['cdir', 'pdir', 'dir']:
                return Directory(path=working_path / filename, modification_date=mod_date, extra=entry_as_json)
            else:
                logger.error("Unsupported node type: "+entry['type'])
                return None


    @staticmethod
    def new_from_dir_line(flavour: FTPFlavour, working_path: PurePosixPath, line: str) -> FileNode:
        with logger.contextualize(wd=working_path, line=line):
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
                    logger.opt(exception=True).warning(f'Failed to parse date: {str(e)}')
                inode_type = m.groupdict()['inode_type']
                if inode_type == 'd':
                    return Directory(working_path / filename, mod_date, None, None)
                elif inode_type == '-':
                    filesize = int(m.groupdict()['size'])
                    ans = RegularFile(path=working_path / filename, modification_date=mod_date, size=filesize)
                    ans.guess_mime()
                    return ans
                elif inode_type == 'l':
                    m = UnixDirRegex_SymLink.match(filename)
                    assert m is not None
                    filename = m.groupdict()['source']
                    target = PurePosixPath(m.groupdict()['target'])
                    # logger.debug(target)
                    if not target.is_absolute():
                        target = working_path / target
                        # logger.debug(target)
                        try:
                            target = target.relative_to('/', walk_up=True)
                        except:
                            target = target.relative_to('/')
                        # logger.debug(target)
                        target = PurePosixPath('/') / target
                    # logger.debug(target)
                    return SymbolicLink(working_path / filename, mod_date, target=target)
                else:
                    logger.error("Unsupported node type: "+inode_type)
                    return None
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
                    logger.opt(exception=True).warning(f'Failed to parse date: {str(e)}')
                if m.groupdict()['inode_type'] == '<DIR>':
                    return Directory(working_path / filename, mod_date, None, None)
                else:
                    filesize = int(m.groupdict()['size'])
                    ans = RegularFile(path=working_path / filename, modification_date=mod_date, size=filesize)
                    ans.guess_mime()
                    return ans
            else:
                raise ValueError("unsupported flavour: "+flavour)

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
        return (str(self.path), self.mime, self.size, self.modification_date, self.extra, self.error_msg)

    def save(self, cur: sqlite3.Cursor):
        cur.execute("REPLACE INTO files (path, mime, size, modification_date, extra, error_msg) VALUES (?, ?, ?, ?, ?, ?);", self.to_sqlite_tuple())

    @staticmethod
    def create_table(cur: sqlite3.Cursor):
        cur.execute("CREATE TABLE IF NOT EXISTS files (path PRIMARY KEY, mime, size, modification_date, extra, error_msg);")

@dataclass
class SymbolicLink(FileNode):
    target: Optional[PurePosixPath] = None

    @property
    def is_dir(self) -> Optional[bool]:
        return None

    def to_sqlite_tuple(self) -> Tuple:
        return (str(self.path), str(self.target), self.modification_date, self.extra, self.error_msg)

    def save(self, cur: sqlite3.Cursor):
        cur.execute("REPLACE INTO links (path, target, modification_date, extra, error_msg) VALUES (?, ?, ?, ?, ?);", self.to_sqlite_tuple())

    @staticmethod
    def create_table(cur: sqlite3.Cursor):
        cur.execute("CREATE TABLE IF NOT EXISTS links (path PRIMARY KEY, target, modification_date, extra, error_msg);")

@dataclass
class Directory(FileNode):
    num_children: Optional[int] = None

    @property
    def is_dir(self) -> bool:
        return True

    def to_sqlite_tuple(self) -> Tuple:
        return (str(self.path), self.num_children, self.modification_date, self.extra, self.error_msg)

    def save(self, cur: sqlite3.Cursor):
        cur.execute("REPLACE INTO directories (path, num_children, modification_date, extra, error_msg) VALUES (?, ?, ?, ?, ?);", self.to_sqlite_tuple())

    @staticmethod
    def load(directory: PurePosixPath, cur: sqlite3.Cursor) -> Optional[Directory]:
        res = cur.execute("SELECT path, num_children, modification_date, extra, error_msg FROM directories WHERE path = ?", (str(directory),))
        line = res.fetchone()
        if line is None:
            return None
        else:
            return Directory(path=line[0], num_children=line[1], modification_date=line[2], extra=line[3], error_msg=line[4])

    @staticmethod
    def create_table(cur: sqlite3.Cursor):
        cur.execute("CREATE TABLE IF NOT EXISTS directories (path PRIMARY KEY, num_children, modification_date, extra, error_msg);")

class FTPScanner:
    ftp: FTP
    con: sqlite3.Connection
    flavour: Optional[FTPFlavour]

    def __init__(self, ftp_addr: str, sqlite_path: str|Path, username: str=None, password: str=None, /,  source_address: Optional[Tuple[str, int]]=None, timeout: Optional[int]=None, encoding: str="utf8", flavour: Optional[FTPFlavour]=None):
        self.flavour = flavour
        addr = urlparse(ftp_addr, scheme='ftp')
        if addr.netloc == '':
            addr = addr._replace(netloc=addr.path, path='')
        if username is not None:
            addr = addr._replace(username=username)
        elif addr.username is not None:
            username = addr.username
        if password is None and addr.password is not None:
            password = addr.password

        logger.info(f"Connecting to {urlunparse(addr)}")
        logger.debug(addr)
        netloc = addr.hostname
        netloc += f':{addr.port}' if addr.port is not None else ''
        if addr.scheme == 'ftp':
            self.ftp = FTP(netloc, username, password, encoding=encoding, source_address=source_address, timeout=timeout)
        elif addr.scheme == 'ftps':
            self.ftp = FTP_TLS(netloc, username, password, encoding=encoding, source_address=source_address, timeout=timeout)
        else:
            raise ValueError(f'not an FTP scheme: {addr.scheme}')

        logger.info(f"Logging in as {username}")
        if addr.scheme == 'ftp':
            self.ftp.login()
        if addr.scheme == 'ftps':
            self.ftp.prot_p()
        logger.info(f"opening SQLite database at {sqlite_path}")
        self.con = sqlite3.connect(sqlite_path)

        cur = self.con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS general (key PRIMARY KEY, value);")
        RegularFile.create_table(cur)
        SymbolicLink.create_table(cur)
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

        try:
            msg = self.ftp.sendcmd("STAT")
            logger.debug(f"FTP STAT output: {msg}")
            cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("STAT", msg))
        except Exception as e:
            logger.warning(f'Failed to run STAT command: {str(e)}')

        try:
            msg = self.ftp.sendcmd("HELP")
            logger.debug(f"FTP HELP output: {msg}")
            cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("HELP", msg))
            if re.findall(r"\bMLSD\b", msg) and self.flavour is None:
                logger.debug("Changing FTP flavour to MLSD")
                self.flavour = FTPFlavour.Mlsd
        except Exception as e:
            logger.warning(f'Failed to run HELP command: {str(e)}')

        try:
            msg = self.ftp.sendcmd("FEAT")
            logger.debug(f"FTP FEAT output: {msg}")
            cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("FEAT", msg))
        except Exception as e:
            logger.warning(f'Failed to run STAT command: {str(e)}')

        msg = self.ftp.sendcmd("SYST")
        logger.info(f"FTP SYST output: {msg}")
        cur.execute("REPLACE INTO general (key, value) VALUES (?, ?)", ("SYST", msg))
        if self.flavour is None:
            if "Windows_NT" in msg or "Win32NT" in msg:
                self.flavour = FTPFlavour.MsDos
            elif "UNIX" in msg:
                self.flavour = FTPFlavour.Unix
            else:
                raise ValueError(f"Unknown flavour {msg}")

        cur.close()
        self.con.commit()

    def _read_lines(self, command: str) -> Generator[str]:
        q = Queue(maxsize=1)
        JOB_DONE = object()
        TIMEOUT = 30

        def append_line(chunk):
            q.put(chunk)

        def task():
            self.ftp.retrlines(command, callback=append_line)
            q.put(JOB_DONE)

        t = threading.Thread(target=task)
        t.start()

        while True:
            chunk = q.get(timeout=TIMEOUT)
            if chunk is JOB_DONE:
                break
            yield chunk

        t.join()

    def scan_dir(self, directory: PurePosixPath, report_every_n_entries=1000) -> List[FileNode]:
        with logger.contextualize(wd=directory):
            cur = self.con.cursor()
            this_dir = Directory.load(directory, cur)
            if this_dir is None:
                this_dir = Directory(directory)

            try:
                logger.info(f'Scanning {str(directory)}')
                self.ftp.cwd(str(directory))

                if self.flavour == FTPFlavour.Mlsd:
                    dir_listing = self.ftp.mlsd()
                    new_node_func = FileNode.new_from_mlsd_line
                else:
                    dir_listing = self._read_lines('LIST')
                    new_node_func = FileNode.new_from_dir_line

                output: List[FileNode] = []
                n_entries = 0
                for line in dir_listing:
                    n_entries += 1
                    if n_entries >= report_every_n_entries and n_entries % report_every_n_entries == 0:
                        logger.info(f'Processed {n_entries} in {directory} so far')
                    try:
                        node: FileNode = new_node_func(self.flavour, directory, line)
                        if node is not None:
                            node.save(cur)
                            output.append(node)
                    except Exception as e:
                        logger.opt(exception=True).error(f'Failed to parse entry line {line!r}: {str(e)}')

                this_dir.num_children = n_entries
                this_dir.save(cur)

                cur.close()
                self.con.commit()
                return output
            except ftplib.error_perm as e:
                logger.warning(f'No permission to scan {str(directory)}: {str(e)}')
                this_dir.error_msg = str(e)
                this_dir.save(cur)
                return []
            except TimeoutError as e:
                logger.warning(f'Timeout while scanning {str(directory)}: {str(e)}')
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