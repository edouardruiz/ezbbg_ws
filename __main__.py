# -*- coding: utf-8 -*-

__author__ = "dgaraud111714"

import os

from .server import main

if __name__ == '__main__':
    debug = os.environ.get("EZBBG_SERVER_DEBUG", False)
    main(debug)
