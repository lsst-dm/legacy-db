#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2015 LSST Corporation.
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
This is a unittest for the EngineFactory functions, geared for testing
remote server connections.

The test requires credential file ~/.lsst/dbAuth-testRemote.ini with the following:

[url]
drivername  = <driverName>
username    = <userName>
password    = <password>
host        = <host>
port        = <port>

and ~/.lsst/dbAuth-testRemote.mysql with:

[mysql]
user     = <userName>
passwd   = <passwd> # this is optional
host     = <host>
port     = <port>

It is sufficient if the user has normal privileges.


@author  Jacek Becla, SLAC

"""

# standard library
import logging as log
import os
import tempfile
import time
import unittest

# third party
import sqlalchemy

# local
from lsst.db.engineFactory import getEngineFromFile, getEngineFromArgs
from lsst.db import utils
from lsst.db.testHelper import readCredentialFile


class TestDbRemote(unittest.TestCase):
    CREDFILE = "~/.lsst/dbAuth-testRemote"

    def setUp(self):
        dict = readCredentialFile(self.CREDFILE+".mysql")
        (self._host, self._port, self._user, self._pass) = \
            [dict[k] for k in ('host', 'port', 'user', 'passwd')]
        if self._pass is None:
            self._pass = ''
        self._dbA = "%s_dbWrapperTestDb_A" % self._user
        self._dbB = "%s_dbWrapperTestDb_B" % self._user
        self._dbC = "%s_dbWrapperTestDb_C" % self._user

        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port).connect()
        if utils.dbExists(conn, self._dbA):
            utils.dropDb(conn, self._dbA)
        if utils.dbExists(conn, self._dbB):
            utils.dropDb(conn, self._dbB)
        if utils.dbExists(conn, self._dbC):
            utils.dropDb(conn, self._dbC)
        conn.close()

    def testBasicHostPortConn(self):
        """
        Basic test: connect through port, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testBasicOptionFileConn(self):
        conn = getEngineFromFile(self.CREDFILE+".ini").connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testConn_invalidHost1(self):
        engine = getEngineFromArgs(
            username=self._user, password=self._pass,
            host="invalidHost", port=self._port)
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_invalidHost2(self):
        engine = getEngineFromArgs(
            username=self._user, password=self._pass,
            host="dummyHost", port=3036)
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_invalidPortNo(self):
        engine = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host, port=987654)
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_wrongPortNo(self):
        engine = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=1579)
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_invalidUserName(self):
        # Disabling because this can work, depending on MySQL
        # configuration, for example, it can default to ''@localhost
        pass

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port).connect()
        self.assertFalse(utils.dbExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla", "blaBla"))

        utils.createDb(conn, self._dbA)
        self.assertTrue(utils.dbExists(conn, self._dbA))
        self.assertFalse(utils.dbExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla", "blaBla"))

        utils.createTable(conn, "t1", "(i int)", self._dbA)
        self.assertTrue(utils.dbExists(conn, self._dbA))
        self.assertFalse(utils.dbExists(conn, "bla"))
        self.assertTrue(utils.tableExists(conn, "t1", self._dbA))
        # utils.useDb(conn, self._dbA)
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port,
            database=self._dbA).connect()
        self.assertTrue(utils.tableExists(conn, "t1"))
        self.assertFalse(utils.tableExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla", "blaBla"))
        utils.dropDb(conn, self._dbA)

        conn.close()

####################################################################################


def main():
    log.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=log.DEBUG)

    credFile = os.path.expanduser(TestDbRemote.CREDFILE+".mysql")
    if not os.path.isfile(credFile):
        log.warning("Required file with credentials '%s' not found.", credFile)
        return

    credFile = os.path.expanduser(TestDbRemote.CREDFILE+".ini")
    if not os.path.isfile(credFile):
        log.warning("Required file with credentials '%s' not found.", credFile)
        return

    unittest.main()

if __name__ == "__main__":
    main()
