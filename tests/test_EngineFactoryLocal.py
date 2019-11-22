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

[database]
url = mysql+mysqldb://<userName>:<password>@localhost:13306/?unix_socket=<path to socket>

User will need full mysql privileges.


@author  Jacek Becla, SLAC

Known issues and todos:
 * restarting server test - it'd be best to restart it for real (without blocking
   on user input.
"""

# standard library
from configparser import NoSectionError
import logging as log
import os
import tempfile
import unittest

# third party
import sqlalchemy

# local
from lsst.db.engineFactory import getEngineFromFile, getEngineFromArgs
from lsst.db import utils


class TestDbLocal(unittest.TestCase):
    CREDFILE = "~/.lsst/dbAuth-testLocal.ini"

    @classmethod
    def setUpClass(cls):
        log.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S',
            level=log.DEBUG)

        credFile = os.path.expanduser(cls.CREDFILE)
        if not os.path.isfile(credFile):
            raise unittest.SkipTest("Required file with credentials"
                                    " '{}' not found.".format(credFile))

    def setUp(self):
        self._engine = getEngineFromFile(self.CREDFILE)
        self._dbA = "%s_dbWrapperTestDb_A" % self._engine.url.username
        self._dbB = "%s_dbWrapperTestDb_B" % self._engine.url.username
        self._dbC = "%s_dbWrapperTestDb_C" % self._engine.url.username

        conn = self._engine.connect()

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
        self.assertEqual("mysql", self._engine.url.get_backend_name())

    def testOverridingUrl(self):
        """
        Test overwriting values from config file.
        """
        engine = getEngineFromFile(self.CREDFILE,
                                   username="peter",
                                   password="hi")
        self.assertEqual(engine.url.username, "peter")
        self.assertEqual(engine.url.password, "hi")
        engine = getEngineFromFile(self.CREDFILE,
                                   host="lsst125",
                                   port="1233")
        self.assertEqual(engine.url.host, "lsst125")
        self.assertEqual(engine.url.port, "1233")
        engine = getEngineFromFile(self.CREDFILE,
                                   database="myBestDB")
        self.assertEqual(engine.url.database, "myBestDB")

    def testBasicSocketConn(self):
        """
        Basic test: connect through socket, create db and connect to it, create one
        table, drop the db, disconnect.
        """
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testGetEngineFromArgs(self):
        url = self._engine.url
        conn = getEngineFromArgs(drivername=url.drivername,
                                 username=url.username,
                                 password=url.password,
                                 host=url.host,
                                 port=url.port,
                                 database=url.database,
                                 query=url.query).connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)
        conn.close()

    def testUseDb(self):
        conn = self._engine.connect()
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
        engine = getEngineFromFile(self.CREDFILE, host="invalidHost")
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_invalidHost2(self):
        engine = getEngineFromFile(self.CREDFILE, host="dummyHost", port=3036)
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_invalidUserName(self):
        # Disabling because this can work, depending on MySQL
        # configuration, for example, it can default to ''@localhost
        pass

    def testConn_invalidSocket(self):
        # make sure retry is disabled, otherwise it wil try to reconnect
        # (it will assume the server is down and socket valid).
        engine = getEngineFromFile(self.CREDFILE, host="localhost",
                                   query={"unix_socket": "/x/sock"})
        self.assertRaises(sqlalchemy.exc.OperationalError, engine.connect)

    def testConn_badSocketGoodHostPort(self):
        # invalid socket, but good host/port
        conn = getEngineFromFile(self.CREDFILE, host='127.0.0.1', query={"unix_socket": "/x/sock"}).connect()
        conn.close()

    def testConn_invalidOptionFile(self):
        self.assertRaises(IOError, getEngineFromFile, "/tmp/dummy.opt.file.xyz")

    def testConn_badOptionFile(self):
        # start with an empty file
        fd, fN = tempfile.mkstemp(suffix=".cnf", text=True)
        self.assertRaises(NoSectionError, getEngineFromFile, fN)

        # add socket only
        os.write(fd, '[client]\n')
        os.write(fd, 'socket = /tmp/sth/wrong.sock\n')
        os.close(fd)
        self.assertRaises(NoSectionError, getEngineFromFile, fN)

        os.remove(fN)

    def testMultiDbs(self):
        """
        Try interleaving operations on multiple databases.
        """
        conn = self._engine.connect()
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
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)", self._dbA)
        utils.createTable(conn, "t2", "(i int)", self._dbA)
        ret = utils.listTables(conn, self._dbA)
        self.assertEqual(len(ret), 2)
        self.assertIn("t1", ret)
        self.assertIn("t2", ret)
        ret = utils.listTables(conn, self._dbB)
        self.assertEqual(len(ret), 0)

        conn = getEngineFromFile(self.CREDFILE, database=self._dbA).connect()
        ret = utils.listTables(conn)
        self.assertEqual(len(ret), 2)
        self.assertIn("t1", ret)
        self.assertIn("t2", ret)
        utils.dropDb(conn, self._dbA)

    def testResults(self):
        conn = self._engine.connect()
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
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.createDb(conn, self._dbA, mayExist=True)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.useDb(conn, self._dbA)
        self.assertRaises(utils.DatabaseExistsError, utils.createDb, conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        self.assertRaises(utils.TableExistsError, utils.createTable, conn, "t1", "(i int)")
        utils.dropDb(conn, self._dbA)

    def testDropDb(self):
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.dropDb(conn, self._dbA)
        utils.dropDb(conn, self._dbA, mustExist=False)
        self.assertRaises(utils.NoSuchDatabaseError, utils.dropDb, conn, self._dbA)
        conn.close()

    def testMultiCreateNonDef(self):
        """
        Test creating db/table that already exists (in non default db).
        """
        conn = self._engine.connect()
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
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i int)")
        utils.createTableLike(conn, self._dbA, "t2", self._dbA, "t1")
        self.assertTrue(utils.tableExists(conn, "t1", self._dbA))
        self.assertRaises(sqlalchemy.exc.NoSuchTableError, utils.createTableLike,
                          conn, self._dbA, "t2", self._dbA, "dummy")

    def testDropTable(self):
        conn = self._engine.connect()
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
        conn = self._engine.connect()
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
        conn = getEngineFromFile(self.CREDFILE, database=self._dbA).connect()
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
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.useDb(conn, self._dbA)
        utils.createTable(conn, "t1", "(i char(64), j char(64))")
        conn.execute("INSERT INTO t1 VALUES(%s, %s)", ("aaa", "bbb"))
        utils.dropDb(conn, self._dbA)

    def testViews(self):
        """
        Testing functionality related to views.
        """
        conn = self._engine.connect()
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
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        # time.sleep(10)
        # ##########################################################################
        # FIXME!!! now getting (OperationalError) (2006, 'MySQL server has gone away
        # ##########################################################################
        utils.createDb(conn, self._dbB)
        utils.dropDb(conn, self._dbA)
        utils.dropDb(conn, self._dbB)

    def testLoadSqlScriptNoDb(self):
        fd, fN = tempfile.mkstemp(suffix=".csv", text=True)
        os.write(fd, "create database %s;\n" % self._dbA)
        os.write(fd, "use %s;\n" % self._dbA)
        os.write(fd, "create table t(i int);\n")
        os.write(fd, "insert into t values (1), (2), (2), (5);\n")
        os.close(fd)
        conn = self._engine.connect()
        utils.loadSqlScript(conn, fN)
        self.assertEqual(10, conn.execute("select sum(i) from %s.t" % self._dbA).first()[0])
        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

    def testLoadSqlScriptWithDb(self):
        fd, fN = tempfile.mkstemp(suffix=".csv", text=True)
        os.write(fd, "create table t(i int, d double);\n")
        os.write(fd, "insert into t values (1, 1.1), (2, 2.2);\n")
        os.close(fd)
        conn = self._engine.connect()
        utils.createDb(conn, self._dbA)
        utils.loadSqlScript(conn, fN, self._dbA)
        self.assertEqual(3, conn.execute("select sum(i) from %s.t" % self._dbA).first()[0])
        utils.dropDb(conn, self._dbA)
        conn.close()
        os.remove(fN)

    def testLoadDataInFile(self):
        """
        Testing "LOAD DATA INFILE..."
        """
        fd, fN = tempfile.mkstemp(suffix=".csv", text=True)
        os.write(fd, '1\n2\n3\n4\n4\n4\n5\n3\n')
        os.close(fd)

        query = self._engine.url.query.copy()
        query['local_infile'] = '1'
        conn = getEngineFromFile(self.CREDFILE, query=query).connect()
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


if __name__ == "__main__":
    unittest.main()
