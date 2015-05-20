# -*- coding: utf-8 -*-

"""ezbbg Web Service based on Flask.

Run the server side with `python -m ezbbg.ws`
"""

import os.path as osp


__author__ = 'dgaraud111714'


def git_version():
    """Get the Web Service 'version'

    i.e. a representation of the latest Git commit.
    """
    wsdir = osp.dirname(__file__)
    with open(osp.join(wsdir, "version")) as fobj:
        return fobj.read().strip()
