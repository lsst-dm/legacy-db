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
local server connections.

The test requires credential file ~/.lsst/dbAuth-testLocal.ini with the following:

[url]
drivername = <driverName>
username   = <userName>
password   = <password> # this is optional
host       = 127.0.0.1
port       = 13306
query      = {"unix_socket: <path to socket>"}

and ~/.lsst/dbAuth-testLocal.mysql with:

[mysql]
user    = <userName>
passwd  = <password>
host    = 127.0.0.1
port    = 13306
socket  = <path to socket>


User will need full mysql privileges.


@author  Jacek Becla, SLAC

Known issues and todos:
 * restarting server test - it'd be best to restart it for real (without blocking
   on user input.
"""

# standard library
try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError
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
from lsst.db.testHelper import readCredentialFile, loadSqlScript, CannotExecuteScriptError


class TestDbLocal(unittest.TestCase):
    CREDFILE = "~/.lsst/dbAuth-testLocal"

    def setUp(self):
        dict = readCredentialFile(self.CREDFILE+".mysql")
        (self._sock, self._host, self._port, self._user, self._pass) = \
            [dict.get(k, None) for k in (
                'unix_socket', 'host', 'port', 'user', 'password')]
        if self._pass is None:
            self._pass = ''
        self._dbA = "%s_dbWrapperTestDb_A" % self._user
        self._dbB = "%s_dbWrapperTestDb_B" % self._user
        self._dbC = "%s_dbWrapperTestDb_C" % self._user

        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()

        if utils.dbExists(conn, self._dbA):
            utils.dropDb(conn, self._dbA)
        if utils.dbExists(conn, self._dbB):
            utils.dropDb(conn, self._dbB)
        if utils.dbExists(conn, self._dbC):
            utils.dropDb(conn, self._dbC)
        conn.close()

    def testGetEngine(self):
        """
        Simplest test, just get the engine and check if default backed is mysql
        """
        engine = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port)
        self.assertEqual("mysql", engine.url.get_backend_name())

    def testOverridingUrl(self):
        """
        Test overwriting values from config file.
        """
        engine = getEngineFromFile(self.CREDFILE+".ini",
                                   username="peter",
                                   password="hi")
        self.assertEqual(engine.url.username, "peter")
        self.assertEqual(engine.url.password, "hi")
        engine = getEngineFromFile(self.CREDFILE+".ini",
                                   host="lsst125",
                                   port="1233")
        self.assertEqual(engine.url.host, "lsst125")
        self.assertEqual(engine.url.port, "1233")
        engine = getEngineFromFile(self.CREDFILE+".ini",
                                   database="myBestDB")
        self.assertEqual(engine.url.database, "myBestDB")

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

    def testBasicSocketConn(self):
        """
        Basic test: connect through socket, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testUseDb(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        self.assertRaises(utils.NoSuchDatabaseError,
                          utils.useDb, conn, "invDbName")
        utils.dropDb(conn, self._dbA)
        self.assertRaises(utils.InvalidDatabaseNameError,
                          utils.createTable, conn, "t1", "(i int)")
        utils.createDb(conn, self._dbB)
        utils.useDb(conn, self._dbB)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbB)

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
            username=self._user, password=self._pass,
            host=self._host, port=987654)
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

    def testConn_invalidSocket(self):
        # make sure retry is disabled, otherwise it wil try to reconnect
        # (it will assume the server is down and socket valid).
        engine = getEngineFromArgs(
            username=self._user, password=self._pass,
            query={"unix_socket": "/x/sock"})
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_badHostPortGoodSocket(self):
        # invalid host, but good socket
        engine = getEngineFromArgs(
            username=self._user, password=self._pass, host="invalidHost",
            port=self._port, query={"unix_socket": self._sock})
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

        # invalid port but good socket
        engine = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=9876543, query={"unix_socket": self._sock})
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_badSocketGoodHostPort(self):
        # invalid socket, but good host/port
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        conn.close()

    def testConn_invalidOptionFile(self):
        self.assertRaises(IOError, getEngineFromFile, "/tmp/dummy.opt.file.xyz")

    def testConn_badOptionFile(self):
        # start with an empty file
        f, fN = tempfile.mkstemp(suffix=".cnf", text=True)
        self.assertRaises(NoSectionError, getEngineFromFile, fN)

        # add socket only
        f = open(fN, 'w')
        f.write('[client]\n')
        f.write('socket = /tmp/sth/wrong.sock\n')
        f.close()
        self.assertRaises(NoSectionError, getEngineFromFile, fN)

        os.remove(fN)

    def testMultiDbs(self):
        """
        Try interleaving operations on multiple databases.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        utils.createDb(conn, self._dbA)
        utils.createDb(conn, self._dbB)
        utils.createDb(conn, self._dbC)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)", self._dbB)
        utils.createTable(conn, "t1", "(i int)")
        utils.createTable(conn, "t1", "(i int)", self._dbC)
        utils.dropDb(conn, self._dbB)
        utils.createTable(conn, "t2", "(i int)", self._dbA)
        utils.dropDb(conn, self._dbA)
        utils.useDb(conn, self._dbC)
        utils.createTable(conn, "t2", "(i int)")
        utils.createTable(conn, "t3", "(i int)", self._dbC)
        utils.dropDb(conn, self._dbC)
        conn.close()

    def testListTables(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        utils.createDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)", self._dbA)
        utils.createTable(conn, "t2", "(i int)", self._dbA)
        ret = utils.listTables(conn, self._dbA)
        self.assertEqual(len(ret), 2)
        self.assertIn("t1", ret)
        self.assertIn("t2", ret)
        ret = utils.listTables(conn, self._dbB)
        self.assertEqual(len(ret), 0)

        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"},
            database=self._dbA).connect()
        ret = utils.listTables(conn)
        self.assertEqual(len(ret), 2)
        self.assertIn("t1", ret)
        self.assertIn("t2", ret)
        utils.dropDb(conn, self._dbA)

    def testResults(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(id INT, theValue FLOAT)")
        conn.execute("INSERT INTO t1 VALUES(1, 1.1), (2, 2.2)")
        ret = conn.execute("SELECT * FROM t1")
        self.assertEqual(len(ret.keys()), 2)

    def testMultiCreateDef(self):
        """
        Test creating db/table that already exists (in default db).
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        utils.createDb(conn, self._dbA)
        utils.createDb(conn, self._dbA, mayExist=True)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.useDb(conn, self._dbA)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)

    def testDropDb(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": "/x/sock"}).connect()
        utils.createDb(conn, self._dbA)
        utils.dropDb(conn, self._dbA)
        utils.dropDb(conn, self._dbA, mustExist=False)
        self.assertRaises(utils.NoSuchDatabaseError, utils.dropDb, conn, self._dbA)
        conn.close()

    def testMultiCreateNonDef(self):
        """
        Test creating db/table that already exists (in non default db).
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createDb(conn, self._dbB)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t1", "(i int)")
        utils.createTable(conn, "t2", "(i int)", self._dbA)
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t1", "(i int)", self._dbA)
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t2", "(i int)", self._dbA)

        utils.createTable(conn, "t1", "(i int)", self._dbB)
        utils.createTable(conn, "t1", "(i int)", self._dbB, mayExist=True)
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t1", "(i int)", self._dbB)
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testCreateTableLike(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.createTableLike(conn, self._dbA, "t2", self._dbA, "t1")
        self.assertTrue(utils.tableExists(conn, "t1", self._dbA))
        self.assertRaises(sqlalchemy.exc.NoSuchTableError, utils.createTableLike,
                          conn, self._dbA, "t2", self._dbA, "dummy")

    def testDropTable(self):
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        # using current db
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t2", "(i int)")
        utils.dropTable(conn, "t2")
        utils.dropTable(conn, "t2", mustExist=False)
        self.assertRaises(sqlalchemy.exc.NoSuchTableError, utils.dropTable, conn, "t2")
        utils.dropDb(conn, self._dbA)

        # using no current db
        utils.createDb(conn, self._dbB)
        utils.createTable(conn, "t2", "(i int)", self._dbB)
        utils.dropTable(conn, "t2", dbName=self._dbB)
        utils.dropTable(conn, "t2", dbName=self._dbB, mustExist=False)
        self.assertRaises(sqlalchemy.exc.NoSuchTableError,
                          utils.dropTable, conn, "t2", self._dbB)
        utils.dropDb(conn, self._dbB)

        # mix of current and not current db
        utils.createDb(conn, self._dbA)
        utils.createDb(conn, self._dbB)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t2", "(i int)", self._dbB)
        utils.createTable(conn, "t2", "(i int)")

        utils.dropTable(conn, "t2")
        utils.dropTable(conn, "t2", dbName=self._dbB)
        utils.dropTable(conn, "t2", mustExist=False)
        utils.dropTable(conn, "t2", dbName=self._dbB, mustExist=False)

        self.assertRaises(sqlalchemy.exc.NoSuchTableError, utils.dropTable, conn, "t2")
        self.assertRaises(sqlalchemy.exc.NoSuchTableError, utils.dropTable, conn, "t2", self._dbB)
        utils.dropDb(conn, self._dbA)
        utils.dropDb(conn, self._dbB)

        conn.close()

    def testCheckExists(self):
        """
        Test checkExist for databases and tables.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
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
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock},
            database=self._dbA).connect()
        self.assertTrue(utils.tableExists(conn, "t1"))
        self.assertFalse(utils.tableExists(conn, "bla"))
        self.assertFalse(utils.tableExists(conn, "bla", "blaBla"))
        utils.dropDb(conn, self._dbA)

        self.assertFalse(utils.userExists(conn, "d_Xx_u12my", "localhost"))
        self.assertTrue(utils.userExists(conn, "root", "localhost"))

        conn.close()

    def testOptParams(self):
        """
        Testing optional parameter binding.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i char(64), j char(64))")
        conn.execute("INSERT INTO t1 VALUES(%s, %s)", ("aaa", "bbb"))
        utils.dropDb(conn, self._dbA)

    def testViews(self):
        """
        Testing functionality related to views.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int, j int)")
        conn.execute("CREATE VIEW t2 AS SELECT i FROM t1")
        self.assertFalse(utils.isView(conn, "t1"))
        self.assertFalse(utils.isView(conn, "dummyT"))
        self.assertTrue(utils.isView(conn, "t2"))
        utils.dropDb(conn, self._dbA)

    def testServerRestart(self):
        """
        Testing recovery from lost connection.
        """
        conn = getEngineFromArgs(
            username=self._user, password=self._pass, host=self._host,
            port=self._port, query={"unix_socket": self._sock}).connect()
        utils.createDb(conn, self._dbA)
        # time.sleep(10)
        # ##########################################################################
        # FIXME!!! now getting (OperationalError) (2006, 'MySQL server has gone away
        # ##########################################################################
        utils.createDb(conn, self._dbB)
        utils.dropDb(conn, self._dbA)
        utils.dropDb(conn, self._dbB)

    def testLoadSqlScriptNoDb(self):
        f, fN = tempfile.mkstemp(suffix=".csv", text=True)
        f = open(fN, 'w')
        f.write("create database %s;\n" % self._dbA)
        f.write("use %s;\n" % self._dbA)
        f.write("create table t(i int);\n")
        f.write("insert into t values (1), (2), (2), (5);\n")
        f.close()
        conn = getEngineFromFile(self.CREDFILE+".ini").connect()
        loadSqlScript(fN, username=self._user, host=self._host, port=self._port)
        self.assertEqual(10, conn.execute("select sum(i) from %s.t" % self._dbA).first()[0])
        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

    def testLoadSqlScriptWithDb(self):
        f, fN = tempfile.mkstemp(suffix=".csv", text=True)
        f = open(fN, 'w')
        f.write("create table t(i int, d double);\n")
        f.write("insert into t values (1, 1.1), (2, 2.2);\n")
        f.close()
        conn = getEngineFromFile(self.CREDFILE+".ini").connect()
        utils.createDb(conn, self._dbA)
        loadSqlScript(
            fN, username=self._user, host=self._host, port=self._port, db=self._dbA)
        self.assertEqual(3, conn.execute("select sum(i) from %s.t" % self._dbA).first()[0])
        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

    def testLoadSqlScriptPlainPassword(self):
        # password is disallowed through loadsqlscript, check on that.
        f, fN = tempfile.mkstemp(suffix=".csv", text=True)
        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            host=self._host, port=self._port).connect()
        utils.createDb(conn, self._dbA)
        args = dict()
        args["db"] = self._dbA
        self.assertRaises(CannotExecuteScriptError, loadSqlScript, fN, **args)
        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

    def testLoadDataInFile(self):
        """
        Testing "LOAD DATA INFILE..."
        """
        f, fN = tempfile.mkstemp(suffix=".csv", text=True)
        f = open(fN, 'w')
        f.write('1\n2\n3\n4\n4\n4\n5\n3\n')
        f.close()

        conn = getEngineFromArgs(
            username=self._user, password=self._pass,
            query={"unix_socket": self._sock, "local_infile": "1"}).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        conn.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE t1" % fN)
        x = conn.execute("SELECT COUNT(*) FROM t1")
        self.assertEqual(8, conn.execute("SELECT COUNT(*) FROM t1").first()[0])
        self.assertEqual(3, conn.execute("SELECT COUNT(*) FROM t1 WHERE i=4").first()[0])

        # let's add some confusing data to the loaded file, it will get truncated
        f = open(fN, 'w')
        f.write('11,12,13,14\n2')
        f.close()
        conn.execute("LOAD DATA LOCAL INFILE '%s' INTO TABLE t1" % fN)

        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

####################################################################################


def main():
    log.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=log.DEBUG)

    credFile = os.path.expanduser(TestDbLocal.CREDFILE+".mysql")
    if not os.path.isfile(credFile):
        log.warning("Required file with credentials '%s' not found.", credFile)
        return

    credFile = os.path.expanduser(TestDbLocal.CREDFILE+".ini")
    if not os.path.isfile(credFile):
        log.warning("Required file with credentials '%s' not found.", credFile)
        return

    unittest.main()

if __name__ == "__main__":
    main()
