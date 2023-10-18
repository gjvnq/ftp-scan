# trick: run NLST and LIST so that we can compare the
# outputs and get the extra info from each entry
#
from __future__ import annotations
from dataclasses import dataclass
from ftplib import FTP
import ftplib
import mimetypes
from pathlib import PurePosixPath
import sqlite3
from typing import Dict, Optional

DIRECTORY_MIME = 'inode/directory'

@dataclass
class FileNode:
    path: PurePosixPath
    mime: Optional[str] = None
    size: Optional[int] = None
    permission_error: bool = False
    details: Optional[str] = None

    @property
    def is_dir(self) -> bool:
        return self.mime == DIRECTORY_MIME

def scan_dir(ftp: FTP, directory: PurePosixPath, cur: Optional[sqlite3.Cursor]) -> Dict[str, FileNode]:
    # Get basic data
    ftp.cwd(str(directory))
    names_only = ftp.nlst()
    names_plus = []
    ftp.retrlines('LIST', names_plus.append)
    reasonably_small = len(names_only) < 500000

    if reasonably_small:
        # We sort by size to reduce the chances of mismatching the file names (NLST output) and the file details (LIST output)
        names_only.sort(key=lambda s: len(s), reverse=True)


    # Make nodes to store info
    names2details: Dict[str, FileNode] = {}
    for raw_name in names_only:
        names2details[raw_name] = FileNode(directory / raw_name)

    if reasonably_small:
        # Associate the details
        for node in names2details.values():
            for i, details in enumerate(names_plus):
                if node.path.name in details:
                    node.details = details
                    del names_plus[i]
                    break

    # Adjust mimes
    for node in names2details.values():
        if reasonably_small and "<DIR>" in node.details:
            node.mime = DIRECTORY_MIME
        else:
            node.mime = mimetypes.guess_type(node.path)[0]

    # Get file sizes
    for node in names2details.values():
        try:
            node.size = ftp.size(node.path.name)
        except ftplib.error_perm as e:
            pass

    # Resort alphabetically
    if reasonably_small:
        names_only.sort()
        output: Dict[str, FileNode] = {}
        for name in names_only:
            output[name] = names2details[name]
    else:
        output = names2details

    if cur is not None:
        def gen_table():
            for node in output.values():
                yield (str(node.path), node.path.name, node.mime, node.size, node.permission_error, node.details)
        cur.executemany("REPLACE INTO nodes (path, name, mime, size, permission_error, details) VALUES (?, ?, ?, ?, ?, ?)", gen_table())

    return output


def recursive_scan(ftp: FTP, directory: PurePosixPath, con: sqlite3.Connection):
    cur = con.cursor()
    children = scan_dir(ftp, directory, cur)
    con.commit()
    for child in children.values():
        print(child)
        if child.is_dir:
            try:
                recursive_scan(ftp, child.path, con)
            except ftplib.error_perm as e:
                child.permission_error = True
                cur.execute("UPDATE nodes SET permission_error = ? WHERE path = ?", (child.permission_error, str(child.path)))