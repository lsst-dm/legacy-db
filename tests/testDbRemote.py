#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 LSST Corporation.
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
This is a unittest for the Db class, geared for testing remote server connections.
It is sufficient if the user has normal privileges.

It requires ~/.lsst.testRemote.my.cnf config file with the following:
[client]
user     = <username>
password = <password> # this can be ommitted if password is empty
host     = <host>
port     = <port>

@author  Jacek Becla, SLAC

Known issues and todos:
 * none
"""

import ConfigParser
import logging
import os
import tempfile
import time
import unittest
from db import Db, DbException

class TestDbRemote(unittest.TestCase):
    def setUp(self):
        self._credFile = "~/.lsst.testRemote.my.cnf"
        self._initCredentials()
        self._dbA = "%s_dbWrapperTestDb_A" % self._user
        self._dbB = "%s_dbWrapperTestDb_B" % self._user
        self._dbC = "%s_dbWrapperTestDb_C" % self._user

        db = Db(self._user, self._pass, self._host, self._port)
        if db.checkDbExists(self._dbA): db.dropDb(self._dbA)
        if db.checkDbExists(self._dbB): db.dropDb(self._dbB)
        if db.checkDbExists(self._dbC): db.dropDb(self._dbC)
        db.disconnect()

    def _initCredentials(self):
        if self._credFile.startswith('~'): 
            self._credFile = os.path.expanduser(self._credFile)
        if not os.path.isfile(self._credFile):
            raise Exception("Required file '%s' not found" % self._credFile)
        cnf = ConfigParser.ConfigParser()
        cnf.read(self._credFile)
        if not cnf.has_section("client"):
            raise Exception("Missing section 'client' in '%s'" % self._credFile)
        for o in ("host", "port", "user"):
            if not cnf.has_option("client", o):
                raise Exception("Missing option '%s' in '%s'" % (o,self._credFile))
        self._host = cnf.get("client", "host")
        self._port = cnf.get("client", "port")
        self._user = cnf.get("client", "user")
        if cnf.has_option("client", "password"):
            self._pass = cnf.get("client", "password")
        else:
            self._pass = ''

    def testBasicHostPortConn(self):
        """
        Basic test: connect through port, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        db = Db(self._user, self._pass, self._host, self._port)
        db.createDb(self._dbA)
        db.useDb(self._dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(self._dbA)
        db.disconnect()

    def testBasicOptionFileConn(self):
        db = Db(optionFile=self._credFile)
        db.createDb(self._dbA)
        db.dropDb(self._dbA)
        db.disconnect()

    def testConn_invalidHost(self):
        db = Db(self._user, self._pass, "invalidHost", self._port)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidHost(self):
        db = Db(self._user, self._pass, "dummyHost", 3036)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidPortNo(self):
        self.assertRaises(DbException, Db, self._user, self._pass,self._host,987654)

    def testConn_wrongPortNo(self):
        db = Db(self._user, self._pass, self._host, 1579,
                sleepLen=0, maxRetryCount=10)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidUserName(self):
        db = Db("hackr", self._pass, self._host, self._port)
        # Disabling because this can work, depending on mysql 
        # configuration, for example, it can default to ''@localhost
        # self.assertRaises(DbException, db.connect)
        db = Db(self._user, "!MyPw", self._host, self._port)
        # self.assertRaises(DbException, db.connect)

    def testIsConnected(self):
        """
        Test isConnected and isConnectedToDb.
        """
        db = Db(self._user, self._pass, self._host, self._port)
        db.disconnect()
        # not connected at all
        self.assertFalse(db.checkIsConnected())
        # just initialize state, still not connected at all
        db = Db(self._user, self._pass, self._host, self._port)
        self.assertFalse(db.checkIsConnected())
        # connect to server, not to db
        db.connect()
        self.assertTrue(db.checkIsConnected())
        # create db, still don't connect to it
        db.createDb(self._dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertNotEqual(db.getCurrentDbName(), self._dbA)
        self.assertNotEqual(db.getCurrentDbName(), self._dbB)
        # finally connect to it
        db.useDb(self._dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertEqual(db.getCurrentDbName(), self._dbA)
        self.assertNotEqual(db.getCurrentDbName(), self._dbB)
        # delete that database
        db.dropDb(self._dbA)
        self.assertFalse(db.checkIsConnected())

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        db = Db(self._user, self._pass, self._host, self._port)
        self.assertFalse(db.checkDbExists("bla"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))

        db.createDb(self._dbA)
        self.assertTrue(db.checkDbExists(self._dbA))
        self.assertFalse(db.checkDbExists("bla"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))

        db.createTable("t1", "(i int)", self._dbA)
        self.assertTrue(db.checkDbExists(self._dbA))
        self.assertFalse(db.checkDbExists("bla"))
        self.assertTrue(db.checkTableExists("t1", self._dbA))
        db.useDb(self._dbA)
        self.assertTrue(db.checkTableExists("t1"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))
        db.dropDb(self._dbA)

        db.disconnect()

####################################################################################
def main():
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S', 
        level=logging.DEBUG)

    #try:
    unittest.main()
    #except DbException as e:
    #    print e

if __name__ == "__main__":
    main()
