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
 - make the connection parameters configurable
"""

import logging
import time
import unittest
from db import Db, DbException

theHost = 'localhost'
thePort = 3306
theUser = 'becla'
thePass  = ''
theSock  = '/var/run/mysqld/mysqld.sock'

dbA = "_dbWrapperTestDb_A"
dbB = "_dbWrapperTestDb_B"
dbC = "_dbWrapperTestDb_C"


class TestDb(unittest.TestCase):
    def setUp(self):
        db = Db(theUser, thePass, theHost, thePort, theSock)
        if db.checkDbExists(dbA): self._db.dropDb(dbA)
        if db.checkDbExists(dbB): self._db.dropDb(dbB)
        if db.checkDbExists(dbC): self._db.dropDb(dbC)
        db.disconnect()
        pass

    def testBasicHostPortConn(self):
        """
        Basic test: connect through port, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        db = Db(theUser, thePass, theHost, thePort)
        db.createDb(dbA)
        db.connectToDb(dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(dbA)
        db.disconnect()

    def testBasicSocketConn(self):
        """
        Basic test: connect through socket, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        db = Db(theUser, thePass, socket=theSock)
        db.createDb(dbA)
        db.connectToDb(dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(dbA)
        db.disconnect()

    def testConn_invalidHost(self):
        db = Db(theUser, thePass, "invalidHost", thePort)
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidHost(self):
        db = Db(theUser, thePass, theHost, 98761)
        # Disabling becasuse this will actually work: it will default 
        # to socket if the host is "localhost". 
        # See known issues in db.py. 
        # self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidUserName(self):
        db = Db("hackr", thePass, theHost, thePort)
        # Disabling because this can work, depending on mysql 
        # configuration, for example, it can default to ''@localhost
        # self.assertRaises(DbException, db.connectToMySQLServer)
        db = Db(theUser, "!MyPw", theHost, thePort)
        # self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidSocket(self):
        # make sure retry is disabled, otherwise it wil try to reconnect 
        # (it will assume the server is down and socket valid).
        db = Db(theUser, thePass, socket="/x/sock", maxRetryCount=0)
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_badHostPortGoodSocket(self):
        # invalid host, but good socket
        db = Db(theUser, thePass, "invalidHost", thePort, theSock)
        db.connectToMySQLServer()
        db.disconnect()
        # invalid port but good socket
        db = Db(theUser, thePass, theHost, 9876543, theSock)
        db.connectToMySQLServer()
        db.disconnect()
        # invalid socket, but good host/port
        # make sure retry is disabled, otherwise it will try to reconnect
        # (it will assume the server is down and socket valid).
        db = Db(theUser, thePass, theHost, thePort, "/x/sock", maxRetryCount=0)
        db.connectToMySQLServer()
        db.disconnect()

    def testIsConnected(self):
        """
        Test isConnected and isConnectedToDb.
        """
        db = Db(theUser, thePass, socket=theSock)
        db.disconnect()
        # not connected at all
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))
        # just initialize state, still not connected at all
        db = Db(theUser, thePass, theHost, thePort, theSock)
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))
        # connect to server, not to db
        db.connectToMySQLServer()
        self.assertTrue(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))
        # create db, still don't connect to ti
        db.createDb(dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))
        # finally connect to it
        db.connectToDb(dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertTrue(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))
        # delete that database
        db.dropDb(dbA)
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(dbA))
        self.assertFalse(db.checkIsConnectedToDb(dbB))

    def testMultiDbs(self):
        """
        Try interleaving operations on multiple databases.
        """
        db = Db(theUser, thePass, theHost, thePort, theSock)
        db.createDb(dbA)
        db.createDb(dbB)
        db.createDb(dbC)
        db.connectToDb(dbA)
        db.createTable("t1", "(i int)", dbB)
        db.createTable("t1", "(i int)")
        db.createTable("t1", "(i int)", dbC)
        db.dropDb(dbB)
        db.createTable("t2", "(i int)", dbA)
        db.dropDb(dbA)
        db.connectToDb(dbC)
        db.createTable("t2", "(i int)")
        db.createTable("t3", "(i int)", dbC)
        db.dropDb(dbC)
        db.disconnect()

    def testMultiCreateDef(self):
        """
        Test creating db/table that already exists (in default db).
        """
        db = Db(theUser, thePass, theHost, thePort, theSock)
        db.createDb(dbA)
        self.assertRaises(DbException, db.createDb, dbA)
        db.connectToDb(dbA)
        self.assertRaises(DbException, db.createDb, dbA)
        db.createTable("t1", "(i int)")
        self.assertRaises(DbException, db.createTable, "t1", "(i int)")
        db.dropDb(dbA)
        self.assertRaises(DbException, db.dropDb, dbA)
        db.disconnect()

    def testMultiCreateNonDef(self):
        """
        Test creating db/table that already exists (in non default db).
        """
        db = Db(theUser, thePass, theHost, thePort, theSock)
        db.createDb(dbA)
        self.assertRaises(DbException, db.createDb, dbA)
        db.connectToDb(dbA)
        db.createDb(dbB)
        self.assertRaises(DbException, db.createDb, dbA)
        db.createTable("t1", "(i int)")
        self.assertRaises(DbException, db.createTable, "t1", "(i int)")
        db.createTable("t2", "(i int)", dbA)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", dbA)
        self.assertRaises(DbException, db.createTable, "t2", "(i int)", dbA)

        db.createTable("t1", "(i int)", dbB)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", dbB)

        db.dropDb(dbA)
        self.assertRaises(DbException, db.dropDb, dbA)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", dbB)

        db.dropDb(dbB)
        db.disconnect()

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        db = Db(theUser, thePass, theHost, thePort, theSock)
        self.assertFalse(db.checkDbExists("bla"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))

        db.createDb(dbA)
        self.assertTrue(db.checkDbExists(dbA))
        self.assertFalse(db.checkDbExists("bla"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))

        db.createTable("t1", "(i int)", dbA)
        self.assertTrue(db.checkDbExists(dbA))
        self.assertFalse(db.checkDbExists("bla"))
        self.assertTrue(db.checkTableExists("t1", dbA))
        db.connectToDb(dbA)
        self.assertTrue(db.checkTableExists("t1"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))
        db.dropDb(dbA)

        self.assertFalse(db.checkUserExists("d_Xx_u12my", "localhost"))
        self.assertTrue(db.checkUserExists("root", "localhost"))

        db.disconnect()

    def testViews(self):
        """
        Testing functionality related to views.
        """
        db = Db(theUser, thePass, theHost, thePort, theSock)
        db.createDb(dbA)
        db.connectToDb(dbA)
        db.createTable("t1", "(i int, j int)")
        db.execCommand0("CREATE VIEW t2 AS SELECT i FROM t1")
        self.assertFalse(db.isView("t1"))
        self.assertTrue(db.isView("t2"))
        self.assertRaises(DbException, db.isView, "dummy")
        db.dropDb(dbA)

    def testServerRestart(self):
        """
        Testing recovery from lost connection.
        """
        db = Db(theUser, thePass, theHost, thePort, theSock, maxRetryCount=3)
        db.createDb(dbA)
        db.connectToDb(dbA)
        db.createTable("t1", "(i int)")
        raw_input("\nRun: 'sudo /etc/init.d/mysql stop', then press Enter to "
                 "continue...\n")
        db.createTable("t2", "(i int)")
        db.dropDb(dbA)

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
