#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014-2015 LSST Corporation.
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
This is a unittest for the db.utils mdule.

The test requires credential file ~/.lsst/dbAuth-testUtils.ini with the following:

[database]
url = mysql+mysqldb://<userName>:<password>@localhost:13306/?unix_socket=<path to socket>

@author  Jacek Becla, SLAC

"""

import os
import tempfile
import unittest

import lsst.log as log
from lsst.db.engineFactory import getEngineFromFile
from lsst.db import utils


class TestUtils(unittest.TestCase):
    CREDFILE = "~/.lsst/dbAuth-testUtils.ini"

    @classmethod
    def setUpClass(cls):
        credFile = os.path.expanduser(cls.CREDFILE)
        if not os.path.isfile(credFile):
            raise unittest.SkipTest("Required file with credentials"
                                    " '{}' not found.".format(credFile))

    def testLoadSqlScriptFromObject(self):
        conn = getEngineFromFile(self.CREDFILE).connect()
        dbName = "%s_dbWrapperTestDb" % conn.engine.url.username

        commands = ["create database %s;" % dbName,
                    "use %s;" % dbName,
                    "create table t(i int);",
                    "insert into t values (1), (2), (2), (5);"]

        # make file object and pass it to loadSqlScript
        script = tempfile.TemporaryFile()
        script.write('\n'.join(commands))
        script.seek(0)
        utils.loadSqlScript(conn, script)
        utils.dropDb(conn, dbName)

    def testLoadSqlScriptFromPath(self):
        conn = getEngineFromFile(self.CREDFILE).connect()
        dbName = "%s_dbWrapperTestDb" % conn.engine.url.username

        commands = ["create database %s;" % dbName,
                    "use %s;" % dbName,
                    "create table t(i int);",
                    "insert into t values (1), (2), (2), (5);"]

        # make file but pass the name of that file to loadSqlScript
        script = tempfile.NamedTemporaryFile()
        script.write('\n'.join(commands))
        script.seek(0)
        utils.loadSqlScript(conn, script.name)
        utils.dropDb(conn, dbName)


if __name__ == "__main__":
    unittest.main()
