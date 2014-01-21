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
from lsst.db.db import Db, DbException

class TestDbRemote(unittest.TestCase):
    CREDFILE = None

    def setUp(self):
        self._initCredentials()
        self._dbA = "%s_dbWrapperTestDb_A" % self._user
        self._dbB = "%s_dbWrapperTestDb_B" % self._user
        self._dbC = "%s_dbWrapperTestDb_C" % self._user

        db = Db(user=self._user, passwd=self._pass, 
                host=self._host, port=self._port)
        if db.dbExists(self._dbA): db.dropDb(self._dbA)
        if db.dbExists(self._dbB): db.dropDb(self._dbB)
        if db.dbExists(self._dbC): db.dropDb(self._dbC)
        db.disconnect()

    def _initCredentials(self):
        if self.CREDFILE.startswith('~'): 
            self.CREDFILE = os.path.expanduser(self.CREDFILE)
        if not os.path.isfile(self.CREDFILE):
            raise Exception("Required file '%s' not found" % self.CREDFILE)
        cnf = ConfigParser.ConfigParser()
        cnf.read(self.CREDFILE)
        if not cnf.has_section("client"):
            raise Exception("Missing section 'client' in '%s'" % self.CREDFILE)
        for o in ("host", "port", "user"):
            if not cnf.has_option("client", o):
                raise Exception("Missing option '%s' in '%s'" % (o,self.CREDFILE))
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
        db = Db(user=self._user, passwd=self._pass, 
                host=self._host, port=self._port)
        db.createDb(self._dbA)
        db.useDb(self._dbA)
        db.createTable("t1", "(i int)")
        db.dropDb(self._dbA)
        db.disconnect()

    def testBasicOptionFileConn(self):
        db = Db(read_default_file=self.CREDFILE)
        db.createDb(self._dbA)
        db.dropDb(self._dbA)
        db.disconnect()

    def testConn_invalidHost(self):
        db = Db(user=self._user, passwd=self._pass, 
                host="invalidHost", port=self._port)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidHost(self):
        db = Db(user=self._user, passwd=self._pass, 
                host="dummyHost", port=3036)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidPortNo(self):
        db = Db(user=self._user, passwd=self._pass, 
                host=self._host,port=987654)
        self.assertRaises(DbException, db.connect)

    def testConn_wrongPortNo(self):
        db = Db(user=self._user, passwd=self._pass, host=self._host, 
                port=1579, sleepLen=0, maxRetryCount=10)
        self.assertRaises(DbException, db.connect)

    def testConn_invalidUserName(self):
        db = Db(user="hackr", passwd=self._pass, host=self._host, port=self._port)
        # Disabling because this can work, depending on mysql 
        # configuration, for example, it can default to ''@localhost
        # self.assertRaises(DbException, db.connect)
        db = Db(user=self._user, passwd="!MyPw", host=self._host, port=self._port)
        # self.assertRaises(DbException, db.connect)

    def testIsConnected(self):
        """
        Test isConnected and isConnectedToDb.
        """
        db = Db(user=self._user, passwd=self._pass, 
                host=self._host, port=self._port)
        db.disconnect()
        # not connected at all
        self.assertFalse(db.isConnected())
        # just initialize state, still not connected at all
        db = Db(user=self._user, passwd=self._pass, 
                host=self._host, port=self._port)
        self.assertFalse(db.isConnected())
        # connect to server, not to db
        db.connect()
        self.assertTrue(db.isConnected())
        # create db, still don't connect to it
        db.createDb(self._dbA)
        self.assertTrue(db.isConnected())
        # finally connect to it
        db.useDb(self._dbA)
        self.assertTrue(db.isConnected())
        # delete that database
        db.dropDb(self._dbA)

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        db = Db(user=self._user, passwd=self._pass, 
                host=self._host, port=self._port)
        self.assertFalse(db.dbExists("bla"))
        self.assertFalse(db.tableExists("bla"))
        self.assertFalse(db.tableExists("bla", "blaBla"))

        db.createDb(self._dbA)
        self.assertTrue(db.dbExists(self._dbA))
        self.assertFalse(db.dbExists("bla"))
        self.assertFalse(db.tableExists("bla"))
        self.assertFalse(db.tableExists("bla", "blaBla"))

        db.createTable("t1", "(i int)", self._dbA)
        self.assertTrue(db.dbExists(self._dbA))
        self.assertFalse(db.dbExists("bla"))
        self.assertTrue(db.tableExists("t1", self._dbA))
        db.useDb(self._dbA)
        self.assertTrue(db.tableExists("t1"))
        self.assertFalse(db.tableExists("bla"))
        self.assertFalse(db.tableExists("bla", "blaBla"))
        db.dropDb(self._dbA)

        db.disconnect()

####################################################################################
def main():
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
        datefmt='%m/%d/%Y %I:%M:%S', 
        level=logging.DEBUG)

    TestDbRemote.CREDFILE = "~/.lsst.testRemote.my.cnf"
    credFile = os.path.expanduser(TestDbRemote.CREDFILE)
    if not os.path.isfile(credFile):
        print "Required file with credentials '%s' not found." % credFile
    else:
        unittest.main()

if __name__ == "__main__":
    main()
