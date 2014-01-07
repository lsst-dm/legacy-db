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

It requires ~/.lsst.my.cnf config file with the following:
[client]
user     = <username>
password = <password> # this can be ommitted if password is empty
host     = <host>
port     = <port>
socket   = <socket>

@author  Jacek Becla, SLAC

Known issues and todos:
 * it blocks on user input.
"""

import ConfigParser
import logging
import os
import tempfile
import time
import unittest
from db import Db, DbException

class TestDb(unittest.TestCase):
    def setUp(self):
        self._initCredentials("~/.lsst.my.cnf")
        self._dbA = "%s_dbWrapperTestDb_A" % self._user
        self._dbB = "%s_dbWrapperTestDb_B" % self._user
        self._dbC = "%s_dbWrapperTestDb_C" % self._user

        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        if db.checkDbExists(self._dbA): self._db.dropDb(self._dbA)
        if db.checkDbExists(self._dbB): self._db.dropDb(self._dbB)
        if db.checkDbExists(self._dbC): self._db.dropDb(self._dbC)
        db.disconnect()

    def _initCredentials(self, fN):
        if fN.startswith('~'): fN = os.path.expanduser(fN)
        if not os.path.isfile(fN):
            raise Exception("Required file '%s' not found" % fN)
        cnf = ConfigParser.ConfigParser()
        cnf.read(fN)
        if not cnf.has_section("client"):
            raise Exception("Missing section 'client' in file '%s'" % fN)
        for o in ("socket", "host", "port", "user"):
            if not cnf.has_option("client", o):
                raise Exception("Missing option '%s' in file '%s'" % (o, fN))
        self._sock = cnf.get("client", "socket")
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
        db.connectToDb(self._dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(self._dbA)
        db.disconnect()

    def testBasicSocketConn(self):
        """
        Basic test: connect through socket, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        db = Db(self._user, self._pass, socket=self._sock)
        db.createDb(self._dbA)
        db.connectToDb(self._dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(self._dbA)
        db.disconnect()

    def testBasicOptionFileConn(self):
        db = Db(optionFile="~/.lsst.my.cnf")
        db.createDb(self._dbA)
        db.dropDb(self._dbA)
        db.disconnect()

    def testConn_invalidHost(self):
        db = Db(self._user, self._pass, "invalidHost", self._port)
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidHost(self):
        db = Db(self._user, self._pass, "dummyHost", 3036)
        # Disabling because this will actually work: it will default 
        # to socket if the host is "localhost". 
        # See known issues in db.py. 
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidPortNo(self):
        self.assertRaises(DbException, Db, self._user, self._pass,self._host,987654)

    def testConn_wrongPortNo(self):
        db = Db(self._user, self._pass, self._host, 1579)
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidUserName(self):
        db = Db("hackr", self._pass, self._host, self._port)
        # Disabling because this can work, depending on mysql 
        # configuration, for example, it can default to ''@localhost
        # self.assertRaises(DbException, db.connectToMySQLServer)
        db = Db(self._user, "!MyPw", self._host, self._port)
        # self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_invalidSocket(self):
        # make sure retry is disabled, otherwise it wil try to reconnect 
        # (it will assume the server is down and socket valid).
        db = Db(self._user, self._pass, socket="/x/sock", maxRetryCount=0)
        self.assertRaises(DbException, db.connectToMySQLServer)

    def testConn_badHostPortGoodSocket(self):
        # invalid host, but good socket
        db = Db(self._user, self._pass, "invalidHost", self._port, self._sock)
        db.connectToMySQLServer()
        db.disconnect()
        # invalid port but good socket
        db = Db(self._user, self._pass, self._host, 9876543, self._sock)
        db.connectToMySQLServer()
        db.disconnect()
        # invalid socket, but good host/port
        # make sure retry is disabled, otherwise it will try to reconnect
        # (it will assume the server is down and socket valid).
        db = Db(self._user, self._pass, self._host, self._port, "/x/sock",
                maxRetryCount=0)
        db.connectToMySQLServer()
        db.disconnect()

    def testConn_invalidOptionFile(self):
        try:
            db = Db(optionFile="/tmp/dummy.opt.file.xyz")
        except DbException:
            pass

    def testConn_badOptionFile(self):
        # start with an empty file
        f, fN = tempfile.mkstemp(suffix=".cnf", dir="/tmp", text="True")
        try:
            db = Db(optionFile=fN)
            db.connectToMySQLServer()
        except DbException:
            pass
        # add socket only
        f = open(fN,'w')
        f.write('[client]\n')
        f.write('socket = /tmp/sth/wrong.sock\n')
        f.close()
        try:
            db = Db(optionFile=fN)
            db.connectToMySQLServer()
        except DbException:
            pass
        os.remove(fN)

    def testIsConnected(self):
        """
        Test isConnected and isConnectedToDb.
        """
        db = Db(self._user, self._pass, socket=self._sock)
        db.disconnect()
        # not connected at all
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))
        # just initialize state, still not connected at all
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))
        # connect to server, not to db
        db.connectToMySQLServer()
        self.assertTrue(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))
        # create db, still don't connect to it
        db.createDb(self._dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))
        # finally connect to it
        db.connectToDb(self._dbA)
        self.assertTrue(db.checkIsConnected())
        self.assertTrue(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))
        # delete that database
        db.dropDb(self._dbA)
        self.assertFalse(db.checkIsConnected())
        self.assertFalse(db.checkIsConnectedToDb(self._dbA))
        self.assertFalse(db.checkIsConnectedToDb(self._dbB))

    def testMultiDbs(self):
        """
        Try interleaving operations on multiple databases.
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        db.createDb(self._dbA)
        db.createDb(self._dbB)
        db.createDb(self._dbC)
        db.connectToDb(self._dbA)
        db.createTable("t1", "(i int)", self._dbB)
        db.createTable("t1", "(i int)")
        db.createTable("t1", "(i int)", self._dbC)
        db.dropDb(self._dbB)
        db.createTable("t2", "(i int)", self._dbA)
        db.dropDb(self._dbA)
        db.connectToDb(self._dbC)
        db.createTable("t2", "(i int)")
        db.createTable("t3", "(i int)", self._dbC)
        db.dropDb(self._dbC)
        db.disconnect()

    def testMultiCreateDef(self):
        """
        Test creating db/table that already exists (in default db).
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        db.createDb(self._dbA)
        self.assertRaises(DbException, db.createDb, self._dbA)
        db.connectToDb(self._dbA)
        self.assertRaises(DbException, db.createDb, self._dbA)
        db.createTable("t1", "(i int)")
        self.assertRaises(DbException, db.createTable, "t1", "(i int)")
        db.dropDb(self._dbA)
        self.assertRaises(DbException, db.dropDb, self._dbA)
        db.disconnect()

    def testMultiCreateNonDef(self):
        """
        Test creating db/table that already exists (in non default db).
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        db.createDb(self._dbA)
        self.assertRaises(DbException, db.createDb, self._dbA)
        db.connectToDb(self._dbA)
        db.createDb(self._dbB)
        self.assertRaises(DbException, db.createDb, self._dbA)
        db.createTable("t1", "(i int)")
        self.assertRaises(DbException, db.createTable, "t1", "(i int)")
        db.createTable("t2", "(i int)", self._dbA)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", self._dbA)
        self.assertRaises(DbException, db.createTable, "t2", "(i int)", self._dbA)

        db.createTable("t1", "(i int)", self._dbB)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", self._dbB)

        db.dropDb(self._dbA)
        self.assertRaises(DbException, db.dropDb, self._dbA)
        self.assertRaises(DbException, db.createTable, "t1", "(i int)", self._dbB)

        db.dropDb(self._dbB)
        db.disconnect()

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
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
        db.connectToDb(self._dbA)
        self.assertTrue(db.checkTableExists("t1"))
        self.assertFalse(db.checkTableExists("bla"))
        self.assertFalse(db.checkTableExists("bla", "blaBla"))
        db.dropDb(self._dbA)

        self.assertFalse(db.checkUserExists("d_Xx_u12my", "localhost"))
        self.assertTrue(db.checkUserExists("root", "localhost"))

        db.disconnect()

    def testViews(self):
        """
        Testing functionality related to views.
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock)
        db.createDb(self._dbA)
        db.connectToDb(self._dbA)
        db.createTable("t1", "(i int, j int)")
        db.execCommand0("CREATE VIEW t2 AS SELECT i FROM t1")
        self.assertFalse(db.isView("t1"))
        self.assertTrue(db.isView("t2"))
        self.assertRaises(DbException, db.isView, "dummy")
        db.dropDb(self._dbA)

    def testServerRestart(self):
        """
        Testing recovery from lost connection.
        """
        db = Db(self._user, self._pass, self._host, self._port, self._sock, maxRetryCount=3)
        db.createDb(self._dbA)
        db.connectToDb(self._dbA)
        db.createTable("t1", "(i int)")
        raw_input("\nRun: 'sudo /etc/init.d/mysql stop', then press Enter to "
                 "continue...\n")
        db.createTable("t2", "(i int)")
        db.dropDb(self._dbA)

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
