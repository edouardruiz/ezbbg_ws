# -*- coding: utf-8 -*-

from __future__ import print_function
import shutil
from os import path
import subprocess

FLIST = ["__init__.py", "__main__.py", "client.py", "server.py"]
DEST_DIR = r"F:\GEDS-PAR-PRI\Pricing\Protected\ezbbg_ws"

def get_sha1():
    fmt = '"%h [%ad] %s - %an"'
    cmd = ['git', 'log', '--format='+ fmt, "--date=short", "-n", "1"]
    return subprocess.check_output(cmd).replace('"', '').strip()

def local_modification():
    cmd = ["git", 'status', '-s', '-uno']
    output = subprocess.check_output(cmd).strip()
    if len(output) == 0:
        return False
    return True

if __name__ == "__main__":
    import sys
    if local_modification():
        print("Please commit before pushing files.\nAbort")
        sys.exit(0)
    for fname in FLIST:
        print("Copying '{}'".format(fname))
        shutil.copyfile(fname, path.join(DEST_DIR, fname))
    # Overwrite the current version
    print("Create the file 'version' with this message")
    print(get_sha1())
    with open(path.join(DEST_DIR, "version"), "w") as fobj:
        fobj.writelines(get_sha1())
