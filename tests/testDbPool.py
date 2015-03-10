#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
This is a unittest for the DbPool class.

The test requires credential file ~/.lsst/dbAuth-test.txt config file with
the following:
[mysql]
user     = <userName>
passwd   = <passwd> # this is optional
host     = localhost
port     = 3306

@author  Jacek Becla, SLAC
"""

# standard library
import logging as log
import os
import time
import unittest

# local
from lsst.db.db import Db
from lsst.db.dbPool import DbPool, DbPoolException

class TestDbPool(unittest.TestCase):
    CREDFILE = "~/.lsst/dbAuth-test.txt"

    def testBasics(self):
        """
        Basic test: add, get, delete.
        """
        dbPool = DbPool()
        dbPool.addConn("a", Db(read_default_file='~/.lsst/dbAuth-test.txt'), 5)
        dbPool.addConn("b", Db(read_default_file='~/.lsst/dbAuth-test.txt'), 1)
        dbPool.addConn("c", Db(read_default_file='~/.lsst/dbAuth-test.txt'))
        self.assertRaises(DbPoolException, dbPool.addConn, "c",
                          Db(read_default_file='~/.lsst/dbAuth-test.txt'))
        dbPool.getConn("a").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.getConn("b").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.getConn("c").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        time.sleep(2)
        dbPool.getConn("a").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.getConn("b").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.getConn("c").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.delConn("b")
        dbPool.getConn("a").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        self.assertRaises(DbPoolException, dbPool.getConn, "b")
        dbPool.getConn("a").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.getConn("c").execCommand0("SHOW DATABASES LIKE '%Stripe82%'")
        dbPool.delConn("a")
        dbPool.delConn("b")
        dbPool.delConn("c")

####################################################################################
def main():
    log.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=log.DEBUG)

    credFile = os.path.expanduser(TestDbPool.CREDFILE)
    if not os.path.isfile(credFile):
        log.warning("Required file with credentials '%s' not found.", credFile)
    else:
        unittest.main()

if __name__ == "__main__":
    main()
