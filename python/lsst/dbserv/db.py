#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
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
This module is a wrapper around MySQLdb. It contains a set of low level basic
database utilities such as connecting to database. It caches connections, and
handles database errors.

@author  Jacek Becla, SLAC


Known issues and todos:
 - need to implement proper logging instead of print.
"""

import _mysql_exceptions
import MySQLdb
import StringIO
import subprocess
from datetime import datetime
from time import sleep


####################################################################################
class DbException(Exception):
    """
    Exception raised by Db class.
    """

    # note: error numbered 1000 - 1200 are used by mysql,
    # see mysqld_ername.h in mysql source code
    SUCCESS                =    0
    ERR_CANT_CONNECT_TO_DB = 1500
    ERR_CANT_EXEC_SCRIPT   = 1505
    ERR_DB_EXISTS          = 1510
    ERR_DB_DOES_NOT_EXIST  = 1515
    ERR_INVALID_DB_NAME    = 1520
    ERR_MISSING_CON_INFO   = 1525
    ERR_MYSQL_CONNECT      = 1530
    ERR_MYSQL_DISCONN      = 1535
    ERR_MYSQL_ERROR        = 1540
    ERR_NO_DB_SELECTED     = 1545
    ERR_NOT_CONNECTED      = 1550
    ERR_TB_DOES_NOT_EXIST  = 1555
    ERR_TB_EXISTS          = 1560
    ERR_INTERNAL           = 9999

    def __init__(self, errNo, extraMsgList=None):
        """
        Initialize the shared data.

        @param errNo      Error number.
        @param extraMsgList  Optional list of extra messages.
        """
        self._errNo = errNo
        self._extraMsgList = extraMsgList

        self._errors = {
            DbException.ERR_CANT_CONNECT_TO_DB: ("Can't connect to database."),
            DbException.ERR_CANT_EXEC_SCRIPT: ("Can't execute script."),
            DbException.ERR_DB_EXISTS: ("Database already exists."),
            DbException.ERR_DB_DOES_NOT_EXIST: ("Database does not exist."),
            DbException.ERR_INVALID_DB_NAME: ("Invalid database name."),
            DbException.ERR_MISSING_CON_INFO: ("Missing connection information -- "
                                        "must provide either host/port or socket."),
            DbException.ERR_MYSQL_CONNECT: ("Unable to connect to mysql server."),
            DbException.ERR_MYSQL_DISCONN: ("Failed to commit transaction and "
                                "disconnect from mysql server."),
            DbException.ERR_MYSQL_ERROR: ("Internal MySQL error."),
            DbException.ERR_NO_DB_SELECTED: ("No database selected."),
            DbException.ERR_NOT_CONNECTED: ("Not connected to MySQL."),
            DbException.ERR_TB_DOES_NOT_EXIST: ("Table does not exist."),
            DbException.ERR_TB_EXISTS: ("Table already exists."),
            DbException.ERR_INTERNAL: ("Internal error.")
        }

    def __str__(self):
        """
        Return string representation of the error.

        @return string  Error message string, including all optional messages.
        """
        msg = self._errors.get(self._errNo, "Undefined database error")
        if self._extraMsgList is not None:
            for s in self._extraMsgList: msg += " (%s)" % s
        return msg

####################################################################################
class Db:
    """
    @brief Wrapper around MySQLdb. 

    This class wraps MySQLdb. It adds extra functionality, like recovering from
    lost connection, It also implements some useful functions, like creating
    databases/tables. Connection is done either through host/port or socket (at
    least one of these must be provided). DbName is optional. Password can be empty.
    """

    def __init__(self, user, passwd=None, host=None, port=None, 
                 socket=None, dbName=None, maxRetryCount=12*60):
        """
        Initialize the shared data. Raise exception if arguments are wrong.

        @param user       User name.
        @param passwd     User's password.
        @param host       Host name.
        @param port       Port number.
        @param socket     Socket.
        @param dbName     Database name.
        @param maxRetryCount Number of retries in case there is connection
                          failure. There is a 5 sec sleep between each retry.
                          Default is one hour: 12*60 * 5 sec sleep

        Initialize shared state. Raise exception if both host/port AND socket are
        invalid.
        """
        if host is None and port is None and socketIsNone:
            raise DbException(DbException.ERR_MISSING_CON_INFO)
        self._conn = None
        self._isConnectedToDb = False
        self._maxRetryCount = maxRetryCount
        self._curRetryCount = 0
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._socket = socket
        self._defaultDbName = dbName

    def __del__(self):
        """
        Disconnect from the server.
        """
        self.disconnect()

    def connectToMySQLServer(self):
        """
        Connect to MySQL Server. Socket has higher priority than host/port.
        """
        while self._curRetryCount <= self._maxRetryCount:
            if self.checkIsConnected(): 
                return
            if self._socket is not None:
                self._connectThroughSocket()
            else:
                self._connectThroughPort()
            if self.checkIsConnected(): 
                self._curRetryCount = 0
                return

    def _connectThroughSocket(self):
        """
        Connect through socket. On failure, try connecting through host/port.
        """
        try:
            self._conn = MySQLdb.connect(user=self._user,
                                         passwd=self._passwd,
                                         unix_socket=self._socket)
        except MySQLdb.Error as e:
            if self._host is not None and self._port is not None:
                self._connectThroughPort()
            else:
                self._handleConnectionFailure(e.args[0], e.args[1])

    def _connectThroughPort(self):
        try:
            self._conn = MySQLdb.connect(user=self._user,
                                         passwd=self._passwd,
                                         host=self._host,
                                         port=self._port)
        except MySQLdb.Error as e:
            self._handleConnectionFailure(e.args[0], e.args[1])

    def _handleConnectionFailure(self, e0, e1):
        self._closeConnection()
        msg = "Couldn't connect to MySQL using socket "
        msg += "'%s' or host:port: '%s:%s'. Error: %d: %s." % \
            (self._socket, self._host, self._port, e0, e1)
        self._curRetryCount += 1
        if e0 == 2002 and self._curRetryCount <= self._maxRetryCount:
            print "Waiting for mysqld to come back..."
            sleep(3)
        else:
            raise DbException(DbException.ERR_MYSQL_CONNECT, [msg])

    def disconnect(self):
        """
        Commit transaction, and disconnect from the server.
        """
        if self._conn == None: return
        try:
            self.commit()
            self._closeConnection()
        except MySQLdb.Error, e:
            msg = "DB Error %d: %s." % \
                                   (e.args[0], e.args[1])
            # self._logger.error(msg)
            raise DbException(DbException.ERR_MYSQL_DISCONN, [msg])
        # self._logger.debug("MySQL connection closed.")
        self._conn = None
        self._isConnectedToDb = False

    def connectToDb(self, dbName=None):
        """
        Connect to database <dbName>, or if <dbName>, to the default database.

        @param dbName     Database name.

        Connect to database <dbName>. If <dbName> is None, the default database
        name will be used. Connect to the server first if connection not open
        already.
        """
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        if self.checkIsConnectedToDb(dbName): return
        try:
            self.connectToMySQLServer()
            self._conn.select_db(dbName)
        except MySQLdb.Error, e:
            # self._logger.debug("Failed to select db '%s'." % dbName)
            raise DbException(DbException.ERR_CANT_CONNECT_TO_DB, [dbName])
        self._isConnectedToDb = True
        self._defaultDbName = dbName
        # self._logger.debug("Connected to db '%s'." % self._defaultDbName)

    def checkIsConnected(self):
        """
        Check if there is connection to the server.
        """
        return self._conn != None

    def checkIsConnectedToDb(self, dbName):
        return (self.checkIsConnected() and
                self._isConnectedToDb and 
                dbName == self.getDefaultDbName())

    def getDefaultDbName(self):
        """
        Get default database name.

        @return string    The name of the default database.
        """
        return self._defaultDbName

    def commit(self):
        """
        Commit a transaction. Raise exception if not connected to the server.
        """
        if not self.checkIsConnected():
            raise DbException(DbException.ERR_NOT_CONNECTED)
        self._conn.commit()

    def checkDbExists(self, dbName=None):
        """
        Check if database <dbName> exists, if <dbName> none, use default database.

        @param dbName     Database name.

        @return boolean   True if the database exists, False otherwise.

        Check if a database <dbName> exists. If it is not set, the default database
        name will be used. Connect to the server first if connection not open
        already.
        """
        if dbName is None and self.getDefaultDbName() is None: return False
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % dbName
        count = self.execCommand1(cmd)
        return count[0] == 1

    def createDb(self, dbName):
        """
        Create database <dbName>.

        @param dbName     Database name.

        Create a new database <dbName>. Raise exception if the database already
        exists. Connect to the server first if connection not open already. Note,
        it will not connect to that database and it will not make it default.
        """
        if dbName is None: 
            raise DbException(DbException.ERR_INVALID_DB_NAME, ["<None>"])
        self.connectToMySQLServer()
        if self.checkDbExists(dbName):
            raise DbException(DbException.ERR_DB_EXISTS, [dbName])
        self.execCommand0("CREATE DATABASE %s" % dbName)

    def dropDb(self, dbName=None):
        """
        Drop database <dbName>.

        @param dbName     Database name.

        Drop a database <dbName>. If <dbName> is None, the default database name
        will be used. Raise exception if the database does not exists. Connect to
        the server first if connection not open already. Disconnect from the
        database if it is the default database.
        """
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        if not self.checkDbExists(dbName):
            raise DbException(DbException.ERR_DB_DOES_NOT_EXIST, [dbName])
        self.execCommand0("DROP DATABASE %s" % dbName)
        if dbName == self.getDefaultDbName():
            self._resetDefaultDbName()

    def checkTableExists(self, tableName, dbName=None):
        """
        Check if table <tableName> exists in database <dbName>.

        @param tableName  Table name.
        @param dbName     Database name.

        @return boolean   True if the table exists, False otherwise.

        Check if table <tableName> exists in database <dbName>. If <dbName> is not
        set, the default database name will be used. Connect to the server first if
        connection not open already.
        """
        if dbName is None and self.getDefaultDbName() is None: return False
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = '%s' AND table_name = '%s'" % \
               (dbName, tableName)
        count = self.execCommand1(cmd)
        return  count[0] == 1

    def createTable(self, tableName, tableSchema, dbName=None):
        """
        Create table <tableName> in database <dbName>.

        @param tableName   Table name.
        @param tableSchema Table schema starting with opening bracket.
        @param dbName      Database name.

        Create a table <tableName> in database <dbName>. If database <dbName> is not
        set, the default database name will be used. Connect to the server first if
        connection not open already.
        """
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        if self.checkTableExists(tableName, dbName):
            raise DbException(DbException.ERR_TB_EXISTS)
        self.execCommand0("CREATE TABLE %s.%s %s" % (dbName,tableName,tableSchema))

    def dropTable(self, tableName, dbName=None):
        """
        Drop table <tableName> in database <dbName>. 

        @param tableName  Table name.
        @param dbName     Database name.

        Drop table <tableName> in database <dbName>. If <dbName> is not set, the
        default database name will be used. Connect to the server first if
        connection not open already.
        """
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        if not self.checkTableExists(tableName, dbName):
            raise DbException(DbException.ERR_TB_DOES_NOT_EXIST)
        self.execCommand0("DROP TABLE %s.%s %s" % (dbName, tableName, tableSchema))

    def getTableContent(self, tableName, dbName=None):
        """
        Get contents of the table <tableName>. Start connection if necessary.

        @param tableName  Table name.
        @param dbName     Database name.

        @return string    Contents of the table.
        """
        dbName = self._getDefaultDbNameIfNeeded(dbName)
        self.connectToMySQLServer()
        ret = self.execCommandN("SELECT * FROM %s.%s" % (dbName, tableName))
        s = StringIO.StringIO()
        s.write(tableName)
        if len(ret) == 0:
            s.write(" is empty.\n")
        else: 
            s.write(':\n')
        for r in ret:
            print >> s, "   ", r
        return s.getvalue()

    def checkUserExists(self, userName, hostName):
        """
        Check if user <hostName>@<userName> exists.
        """
        ret = self.execCommand1(
            "SELECT COUNT(*) FROM mysql.user WHERE user='%s' AND host='%s'" %\
            (userName, hostName))
        return ret[0] != 0

    def loadSqlScript(self, scriptPath, dbName):
        """
        Load sql script from the file in <scriptPath> into database <dbName>.

        @param scriptPath Path the the SQL script.
        @param dbName     Database name.
        """
        # self._logger.debug("loading script %s into db %s" %(scriptPath,dbName))
        if self._passwd:
            if self._socket is None:
                cmd = 'mysql -h%s -P%s -u%s -p%s %s' % \
                (self._host, self._port, self._user, self._passwd, dbName)
            else:
                cmd = 'mysql -S%s -u%s -p%s %s' % \
                (self._socket, self._user,self._passwd, dbName)
        else:
            if self._socket is None:
                cmd = 'mysql -h%s -P%s -u%s %s' % \
                (self._host, self._port, self._user, dbName)
            else:
                cmd = 'mysql -S%s -u%s %s' % \
                (self._socket, self._user, dbName)
        # self._logger.debug("cmd is %s" % cmd)
        with file(scriptPath) as scriptFile:
            if subprocess.call(cmd.split(), stdin=scriptFile) != 0:
                msg = "Failed to execute %s < %s" % (cmd,scriptPath)
                raise DbException(DbException.ERR_CANT_EXEC_SCRIPT, [msg])

    def execCommand0(self, command):
        """
        Execute mysql command that returns no rows.

        @param command    MySQL command that returns no rows.
        """
        self._execCommand(command, 0)

    def execCommand1(self, command):
        """
        Execute mysql command that returns one row.

        @param command    MySQL command that returns one row.

        @return string    Result.
        """
        return self._execCommand(command, 1)

    def execCommandN(self, command):
        """
        Execute mysql command that returns more than one row.

        @param command    MySQL command that returns more than one row.

        @return string    Result.
        """
        return self._execCommand(command, 'n')

    def _execCommand(self, command, nRowsRet):
        """
        Execute mysql command which return any number of rows.

        @param command    MySQL command.
        @param nRowsRet   Expected number of returned rows (valid: '0', '1', 'n').

        @return string Results from the query. Empty string if not results.

        Execute mysql command which return any number of rows. If this function
        is called after mysqld was restarted, or if the connection timed out
        because of long period of inactivity, the command will fail. This function
        catches such problems and recovers by reconnecting and retrying.
        """
        self.connectToMySQLServer()
        if self._conn is None:
            raise DbException(DbException.ERR_INTERNAL, 
                              ["self._conn is <None> in _execCommand"])
        cursor = self._conn.cursor()
        try:
            # self._logger.debug("Executing '%s'." % command)
            print ("Executing: %s." % command)
            cursor.execute(command)
        except (MySQLdb.Error, MySQLdb.OperationalError) as e:
            self._closeConnection()
            self._isConnectedToDb = False
            cursor = None
            msg = "MySQL Error [%d]: %s. Trying to recover..,"%(e.args[0],e.args[1])
            if self.getDefaultDbName() is not None:
                self.connectToDb(self.getDefaultDbName())
            return self._execCommand(command, nRowsRet)
        if nRowsRet == 0:
            ret = ""
        elif nRowsRet == 1:
            ret = cursor.fetchone()
            # self._logger.debug("Got: %s" % str(ret))
        else:
            ret = cursor.fetchall()
            # self._logger.debug("Got: %s" % str(ret))
        cursor.close()
        return ret

    def _getDefaultDbNameIfNeeded(self, dbName):
        """
        Get valid dbName.

        @param dbName     Database name.

        @return string    Return <dbName> if it is valid, otherwise if the the
                          default database name if valid return it.

        Get valid dbName (the one passed, or default database name). If neither is
        valid, raise exception.
        """
        if dbName is not None: 
            return dbName
        dbName = self.getDefaultDbName()
        if dbName is None:
            raise DbException(DbException.ERR_INVALID_DB_NAME, ["<None>"])
        return dbName

    def _closeConnection(self):
        """
        Close connection to the server.
        """
        if self._conn is None: return
        self._conn.close()
        self._conn = None

    def _resetDefaultDbName(self):
        """
        Reset the default database and disconnect from the server.
        """
        self._defaultDbName = None
        self.disconnect()
