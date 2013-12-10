#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013 LSST Corporation.
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
This is a unittest for the Db class.

@author  Jacek Becla, SLAC


Known issues and todos:
 - implement proper logging instead of print
 - make the connection parameters configurable
"""

import time
import unittest
from db import Db, DbException

theHost = 'localhost'
thePort = 3306
theUser = 'becla'
thePass  = ''
theSock  = ''

dbA = "_dbWrapperTestDb_A"
dbB = "_dbWrapperTestDb_B"
dbC = "_dbWrapperTestDb_C"


# Todo:
# - test dropping tables


class TestDb(unittest.TestCase):
    def setUp(self):
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        if self._db.checkDbExists(dbA): self._db.dropDb(dbA)
        if self._db.checkDbExists(dbB): self._db.dropDb(dbB)
        if self._db.checkDbExists(dbC): self._db.dropDb(dbC)
        self._db.disconnect()

    def testBasic(self):
        """
        Basic test: connect through port, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self._db.createDb(dbA)
        self._db.connectToDb(dbA)
        self._db.createTable("t1", "(i int)")
        self._db.dropDb(dbA)
        self._db.disconnect()

    def testIsConnected(self):
        """
        Test isConnected and isConnectedToDb.
        """
        # not connected at all
        self.assertFalse(self._db.checkIsConnected())
        self.assertFalse(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))
        # just initialize state, still not connected at all
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self.assertFalse(self._db.checkIsConnected())
        self.assertFalse(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))
        # connect to server, not to db
        self._db.connectToMySQLServer()
        self.assertTrue(self._db.checkIsConnected())
        self.assertFalse(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))
        # create db, still don't connect to ti
        self._db.createDb(dbA)
        self.assertTrue(self._db.checkIsConnected())
        self.assertFalse(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))
        # finally connect to it
        self._db.connectToDb(dbA)
        self.assertTrue(self._db.checkIsConnected())
        self.assertTrue(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))
        # delete that database
        self._db.dropDb(dbA)
        self.assertFalse(self._db.checkIsConnected())
        self.assertFalse(self._db.checkIsConnectedToDb(dbA))
        self.assertFalse(self._db.checkIsConnectedToDb(dbB))

    def testMultiDbs(self):
        """
        Try interleaving operations on multiple databases.
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self._db.createDb(dbA)
        self._db.createDb(dbB)
        self._db.createDb(dbC)
        self._db.connectToDb(dbA)
        self._db.createTable("t1", "(i int)", dbB)
        self._db.createTable("t1", "(i int)")
        self._db.createTable("t1", "(i int)", dbC)
        self._db.dropDb(dbB)
        self._db.createTable("t2", "(i int)", dbA)
        self._db.dropDb(dbA)
        self._db.connectToDb(dbC)
        self._db.createTable("t2", "(i int)")
        self._db.createTable("t3", "(i int)", dbC)
        self._db.dropDb(dbC)
        self._db.disconnect()

    def testMultiCreateDef(self):
        """
        Test creating db/table that already exists (in default db).
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self._db.createDb(dbA)
        self.assertRaises(DbException, self._db.createDb, dbA)
        self._db.connectToDb(dbA)
        self.assertRaises(DbException, self._db.createDb, dbA)
        self._db.createTable("t1", "(i int)")
        self.assertRaises(DbException, self._db.createTable, "t1", "(i int)")
        self._db.dropDb(dbA)
        self.assertRaises(DbException, self._db.dropDb, dbA)
        self._db.disconnect()

    def testMultiCreateNonDef(self):
        """
        Test creating db/table that already exists (in non default db).
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self._db.createDb(dbA)
        self.assertRaises(DbException, self._db.createDb, dbA)
        self._db.connectToDb(dbA)
        self._db.createDb(dbB)
        self.assertRaises(DbException, self._db.createDb, dbA)
        self._db.createTable("t1", "(i int)")
        self.assertRaises(DbException, self._db.createTable, "t1", "(i int)")
        self._db.createTable("t2", "(i int)", dbA)
        self.assertRaises(DbException, self._db.createTable, "t1", "(i int)", dbA)
        self.assertRaises(DbException, self._db.createTable, "t2", "(i int)", dbA)

        self._db.createTable("t1", "(i int)", dbB)
        self.assertRaises(DbException, self._db.createTable, "t1", "(i int)", dbB)

        self._db.dropDb(dbA)
        self.assertRaises(DbException, self._db.dropDb, dbA)
        self.assertRaises(DbException, self._db.createTable, "t1", "(i int)", dbB)

        self._db.dropDb(dbB)
        self._db.disconnect()

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock)
        self.assertFalse(self._db.checkDbExists("bla"))
        self.assertFalse(self._db.checkTableExists("bla"))
        self.assertFalse(self._db.checkTableExists("bla", "blaBla"))

        self._db.createDb(dbA)
        self.assertTrue(self._db.checkDbExists(dbA))
        self.assertFalse(self._db.checkDbExists("bla"))
        self.assertFalse(self._db.checkTableExists("bla"))
        self.assertFalse(self._db.checkTableExists("bla", "blaBla"))

        self._db.createTable("t1", "(i int)", dbA)
        self.assertTrue(self._db.checkDbExists(dbA))
        self.assertFalse(self._db.checkDbExists("bla"))
        self.assertTrue(self._db.checkTableExists("t1", dbA))
        self._db.connectToDb(dbA)
        self.assertTrue(self._db.checkTableExists("t1"))
        self.assertFalse(self._db.checkTableExists("bla"))
        self.assertFalse(self._db.checkTableExists("bla", "blaBla"))
        self._db.dropDb(dbA)

        self.assertFalse(self._db.checkUserExists("d_Xx_u12my", "localhost"))
        self.assertTrue(self._db.checkUserExists("root", "localhost"))

        self._db.disconnect()

    def testServerRestart(self):
        """
        Testing recovery from lost connection.
        """
        self._db = Db(host=theHost, port=thePort, user=theUser, passwd=thePass,
                      socket=theSock, maxRetryCount=3)
        self._db.createDb(dbA)
        self._db.connectToDb(dbA)
        self._db.createTable("t1", "(i int)")
        raw_input("\nRun: 'sudo /etc/init.d/mysql stop', then press Enter to "
                 "continue...\n")
        self._db.createTable("t2", "(i int)")
        self._db.dropDb(dbA)

####################################################################################
def main():
    #try:
    unittest.main()
    #except DbException as e:
    #    print e

if __name__ == "__main__":
    main()
