#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008-2014 LSST Corporation.
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

Known issues:
 * execCommandN: what if I have a huge number of rows? It'd be nice to also have 
   a way to iterate over results without materializing them all in client memory
   (perhaps via a generator).
 * need to integrate logging into lsst-stack logging
"""

# standard library
import ConfigParser
import contextlib
import copy
import logging
import os.path
import StringIO
import subprocess
import sys
import warnings
from datetime import datetime
from time import sleep

# related third-package library
import MySQLdb

# local
from lsst.db.mysqlInternal import mysql_connectArgToOptionMap as argToOptionMap

####################################################################################
class DbException(Exception, object):
    """
    Database-specific exception class.
    """
    _errorMessages = {}

    def __init__(self, errCode, *messages):
        """
        Create a DbException from an integer error code and an arbitrary number of 
        ancillary messages.

        @param errCode    Error code.
        @param messages   Optional list of ancillary messages.
        """
        self._errCode = errCode
        self._messages = messages

    def __str__(self):
        msg = DbException._errorMessages.get(self.errCode) or (
            "Unrecognized database error: %r" % self.errorCode)
        if self.messages:
            msg = msg + " (" + "), (".join(self.messages) + ")"
        return msg

    @property
    def errCode(self):
        return self._errCode

    @property
    def messages(self):
    	return self._messages

def _defineErr(errCode, errName, errMsg):
    setattr(DbException, errName, errCode)
    DbException._errorMessages[errCode] = errMsg

_defineErr(1500, "CANT_CONNECT_TO_DB", "Can't connect to database.")
_defineErr(1505, "CANT_EXEC_SCRIPT",   "Can't execute script.")
_defineErr(1510, "DB_EXISTS",          "Database already exists.")
_defineErr(1515, "DB_DOES_NOT_EXIST",  "Database does not exist.")
_defineErr(1520, "INVALID_CONN_INFO",  "Invalid connection parameter.") 
_defineErr(1525, "INVALID_DB_NAME",    "Invalid database name.")
_defineErr(1530, "INVALID_OPT_FILE",   "Can't open the option file.")
_defineErr(1532, "PASSWD_NOT_ALLOWED", "Password disallowed, use option file.")
_defineErr(1535, "SERVER_CONNECT",     "Unable to connect to server.")
_defineErr(1540, "SERVER_DISCONN",     "Failed to disconnect from db server.")
_defineErr(1545, "SERVER_ERROR",       "Internal db server error.")
_defineErr(1550, "NO_DB_SELECTED",     "No database selected.")
_defineErr(1555, "NOT_CONNECTED",      "Not connected to the db server.")
_defineErr(1560, "TB_DOES_NOT_EXIST",  "Table does not exist.")
_defineErr(1565, "TB_EXISTS",          "Table already exists.")
_defineErr(1900, "SERVER_WARNING",     "Warning.")
_defineErr(9999, "INTERNAL",           "Internal error.")

####################################################################################
class Db(object):
    """
    @brief Wrapper around MySQLdb.

    This class wraps MySQLdb. It adds extra functionality, like recovering from
    lost connection. It also implements some useful functions, like creating
    databases/tables. Connection is done either through host/port or socket (at
    least one of these must be provided). Password can be empty. If it can't
    connect, it will retry (and sleep). Public functions do not have to call 
    "connect()" - ff connection is needed and is not ready, the functions will
    establish it first.
    """

    # Map of MySQL-specific errors this wrapper is sensitive to into DbException
    # errors. The errors that map to SERVER_CONNECT are typically recoverable by
    # reconnecting.
    _mysqlErrorMap = {
        1007: DbException.DB_EXISTS,
        1008: DbException.DB_DOES_NOT_EXIST,
        1050: DbException.TB_EXISTS,
        1051: DbException.TB_DOES_NOT_EXIST,
        2002: DbException.SERVER_CONNECT,
        2003: DbException.SERVER_CONNECT,
        2006: DbException.SERVER_CONNECT,
        2013: DbException.SERVER_CONNECT
    }

    def __init__(self, **kwargs):
        """
        Create a Db instance.

        @param kwargs     Arguments for mysql connect:

        host
          string, host to connect

        user
          string, user to connect as

        passwd
          string, password to use

        db
          string, database to use

        port
          integer, TCP/IP port to connect to

        unix_socket
          string, location of unix_socket to use

        connect_timeout
          number of seconds to wait before the connection attempt
          fails.

        compress
          if set, compression is enabled

        named_pipe
          if set, a named pipe is used to connect (Windows only)

        read_default_file
          file from which default client values are read

        charset
          If supplied, the connection character set will be changed
          to this character set (MySQL-4.1 and newer). This implies
          use_unicode=True.

        local_infile
          integer, non-zero enables LOAD LOCAL INFILE; zero disables

        Note that if the host is set to "localhost", "127.0.0.1" will be used
        instead, because MySQL silently switches to a default socket if
        "localhost" is used.
        """
        self._conn = None
        self._logger = logging.getLogger("lsst.db.Db")
        self._kwargs = copy.deepcopy(kwargs)
        self._sleepLen = self._kwargs.pop("sleepLen", 3)
        self._attemptMaxNo = 1 + self._kwargs.pop("maxRetryCount", 0)
        for k in self._kwargs:
            if k not in argToOptionMap:
                raise DbException(DbException.INVALID_CONN_INFO, repr(k))
        # If a MySQL defaults file will be read, make sure to pull values from the
        # [mysql] group, just like the mysql executable does. The [client] group
        # will automatically be read.
        if "read_default_file" in self._kwargs:
            self._kwargs["read_default_group"] = "mysql"
        # MySQL will use socket for "localhost". 127.0.0.1 forces TCP.
        if self._kwargs.get("host", "") == "localhost":
            self._kwargs["host"] = "127.0.0.1"
            self._logger.warning('"localhost" specified, switching to 127.0.0.1')
        if "port" in self._kwargs:
            self._kwargs["port"] = int(self._kwargs["port"])
        # Map MySQL warnings to exceptions
        warnings.filterwarnings("error", category=MySQLdb.Warning)
        # Log the connection parameters
        self._logger.info("Created lsst.db.Db with connection parameters " + \
                          "(password not shown): %s" % \
                              str(["%s:%s" % (x, self._kwargs[x]) \
                                   for x in self._kwargs if not x == "passwd"]))

    def __del__(self):
        """
        Disconnect from the server.
        """
        self.disconnect()

    ##### Connection-related functions #############################################
    def isConnected(self):
        """
        Return True if connection is established, False otherwise.
        """
        if self._conn is None:
            return False
        try:
            self._logger.info("Pinging server")
            self._conn.ping()
        except MySQLdb.OperationalError as e:
            if self._isConnectionError(e.args[0]):
                self._logger.debug("Ping failed with error %d: %s" % error.args[:2])
                self._conn = None
                return False
            raise
        return True

    def connect(self, dbName=None):
        """
        Connect to Database Server. If dbName is provided, connect to that database.
        """
        if not self.isConnected():
            self._doConnect()
        if dbName is not None:
            try:
                self._logger.info("Selecting db %s." % dbName)
                self._conn.select_db(dbName)
            except:
                self._handleException(sys.exc_info()[1])

    def _doConnect(self):
        n = 1
        while True:
            self._logger.info("Connecting (attempt %d of %d)" %
                              (n, self._attemptMaxNo))
            try:
                self._logger.debug("mysql.connect.")
                self._conn = MySQLdb.connect(**self._kwargs)
                return
            except MySQLdb.Error as e:
                msg = "MySQL error %d: %s" % e.args[:2]
                self._logger.error(msg)
                if (n >= self._attemptMaxNo or 
                    not self._isConnectionError(e.args[0])):
                    errCode = self._getErrCode(e.args[0])
                    self._logger.error("Can't recover, sorry")
                    raise DbException(errCode, msg)
                # try again
                n += 1
                if self._sleepLen > 0:
                    sleep(self._sleepLen)
            except:
                self._handleException(sys.exc_info()[1])

    def disconnect(self):
        """
        Disconnect from the server.
        """
        self._logger.info("closing connection")
        if self._conn is None:
            return
        try:
            self._conn.close()
        except:
            pass
        self._conn = None

    def close(self):
        self.disconnect()

    def _handleException(self, e):
        if isinstance(e, MySQLdb.Error):
            msg = "MySQL error %d: %s" % e.args[:2]
            errCode = self._getErrCode(e.args[0])
        elif isinstance(e, MySQLdb.Warning):
            msg = "MySQL warning %s" % e.message
            errCode = DbException.SERVER_WARNING
        else:
            self._logger.error("Unexpected exception: %s" % e)
            raise e
        self._logger.error(msg)
        raise DbException(errCode, msg)

    def _getErrCode(self, mysqlErrCode):
        return self._mysqlErrorMap.get(mysqlErrCode, DbException.SERVER_ERROR)

    def _isConnectionError(self, mysqlErrCode):
        return  self._getErrCode(mysqlErrCode) == DbException.SERVER_CONNECT

    #### Database-related functions ################################################
    def createDb(self, dbName, mayExist=False):
        """
        Create database <dbName>.

        @param dbName      Database name.
        @param mayExist    Flag indicating what to do if the database exists.

        Raise exception if the database already exists and mayExist is False.
        Note, it will not connect to that database and it will not make it default.
        """
        if dbName is None: 
            raise DbException(DbException.INVALID_DB_NAME, "<None>")
        try:
            self.execCommand0("CREATE DATABASE `%s`" % dbName)
        except DbException as e:
            if e.errCode == DbException.DB_EXISTS and mayExist:
                self._logger.debug("create db failed, mayExist is True")
                pass
            else:
                raise

    def useDb(self, dbName):
        """
        Connect to database <dbName>.

        @param dbName     Database name.
        """
        if dbName is not None:
            self.connect(dbName)

    def dbExists(self, dbName):
        """
        Return True if database <dbName> exists, False otherwise.

        @param dbName     Database name.
        """
        if dbName is None:
            return False
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % dbName
        count = self.execCommand1(cmd)
        return count[0] == 1

    def dropDb(self, dbName, mustExist=True):
        """
        Drop database <dbName>.

        @param dbName     Database name.
        @param mustExist  Flag indicating what to do if the database does not exist.

        Raise exception if the database does not exists and the flag mustExist is
        not set to False. Disconnect from the database if it is the current
        database.
        """
        try:
            self.execCommand0("DROP DATABASE `%s`" % dbName)
        except DbException as e:
            if e.errCode == DbException.DB_DOES_NOT_EXIST and not mustExist:
                self._logger.debug("dropDb failed, mustExist is False")
            else:
                raise

    #### Table-related functions ###################################################
    def tableExists(self, tableName, dbName=None):
        """
        Return True if table <tableName> exists in database <dbName>.

        @param tableName  Table name.
        @param dbName     Database name.

        If <dbName> is not set, the current database name will be used.
        """
        dbNameStr = "'%s'" % dbName if dbName is not None else "DATABASE()"
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = %s AND table_name = '%s'" % \
               (dbNameStr, tableName)
        count = self.execCommand1(cmd)
        return count[0] == 1

    def createTable(self, tableName, tableSchema, dbName=None, mayExist=False):
        """
        Create table <tableName> in database <dbName>.

        @param tableName   Table name.
        @param tableSchema Table schema starting with opening bracket.
        @param dbName      Database name.
        @param mayExist    Flag indicating what to do if the database exists.

        If database <dbName> is not set, and "use <database>" was called earlier,
        it will use that database.
        Raise exception if the table already exists and mayExist flag is not say to
        True.
        """
        dbNameStr = "`%s`." % dbName if dbName is not None else ""
        try:
            self.execCommand0("CREATE TABLE %s`%s` %s" % \
                                  (dbNameStr, tableName, tableSchema))
        except  DbException as e:
            if e.errCode == DbException.TB_EXISTS and mayExist:
                self._logger.debug("create table failed, mayExist is True")
            else:
                raise

    def dropTable(self, tableName, dbName=None, mustExist=True):
        """
        Drop table <tableName> in database <dbName>. 

        @param tableName  Table name.
        @param dbName     Database name.
        @param mustExist  Flag indicating what to do if the database does not exist.

        If <dbName> is not set, the current database name will be used. Raise
        exception if the table does not exist and the mustExist flag is not set to
        False.
        """
        dbNameStr = "`%s`." % dbName if dbName is not None else ""
        try:
            self.execCommand0("DROP TABLE %s`%s`" % (dbNameStr, tableName))
        except DbException as e:
            if e.errCode == DbException.TB_DOES_NOT_EXIST and not mustExist:
                self._logger.debug("dropTable failed, mustExist is False")
            else:
                raise

    def isView(self, tableName, dbName=None):
        """
        Return True if the table <tableName> is a view, False otherwise.

        @param tableName  Table name.
        @param dbName     Database name.

        @return boolean   True if the table is a view. False otherwise.

        If <dbName> is not set, the current database name will be used.
        """
        dbNameStr = "'%s'" % dbName if dbName is not None else "DATABASE()"
        rows = self.execCommandN("SELECT table_type FROM information_schema.tables "
               "WHERE table_schema=%s AND table_name='%s'" % (dbNameStr, tableName))
        return len(rows) == 1 and rows[0][0] == 'VIEW'

    def getTableContent(self, tableName, dbName=None):
        """
        Get contents of the table <tableName>.

        @param tableName  Table name.
        @param dbName     Database name.

        @return string    Contents of the table.
        """
        dbNameStr = "`%s`." % dbName if dbName is not None else ""
        ret = self.execCommandN("SELECT * FROM %s`%s`" % (dbNameStr, tableName))
        s = StringIO.StringIO()
        s.write(tableName)
        if len(ret) == 0:
            s.write(" is empty.\n")
        else: 
            s.write(':\n')
        for r in ret:
            print >> s, "   ", r
        return s.getvalue()

    #### Executing command related functions #######################################
    def execCommand0(self, command):
        """
        Execute SQL command and discard any result.

        @param command    SQL command that returns no rows.
        """
        self._execCommand(command, 0)

    def execCommand1(self, command):
        """
        Execute SQL command that returns a single row (a sequence of column values),
        or None if the statemetn returned no results.

        @param command    SQL command that returns one row.

        @return string    Result.
        """
        return self._execCommand(command, 1)

    def execCommandN(self, command):
        """
        Execute SQL command that returns a sequence of all statement result rows,
        which are themselves sequences of column values.

        @param command    SQL command. that returns more than one row.

        @return string    Result.
        """
        return self._execCommand(command, 'n')

    def _execCommand(self, command, nRowsRet):
        """
        Execute SQL command which return any number of rows.

        @param command    SQL command.
        @param nRowsRet   Expected number of returned rows (valid: '0', '1', 'n').

        @return string Results from the query. Empty string if not results.

        Establish connection if it hasn't been established, but do not attempt to
        recover from any failures -- this is left up to user.
        """
        if self._conn is None:
            self.connect()
        with contextlib.closing(self._conn.cursor()) as cursor:
            self._logger.debug("Executing '%s'." % command)
            try:
                cursor.execute(command)
            except:
                self._handleException(sys.exc_info()[1])
            if nRowsRet == 0:
                ret = None
            elif nRowsRet == 1:
                ret = cursor.fetchone()
                self._logger.debug("Got: %s" % str(ret))
            else:
                ret = cursor.fetchall()
                self._logger.debug("Got: %s" % str(ret))
            return ret

    #### All others ################################################################
    def userExists(self, userName, hostName):
        """
        Return True if user <hostName>@<userName> exists, False otherwise.
        """
        ret = self.execCommand1(
            "SELECT COUNT(*) FROM mysql.user WHERE user='%s' AND host='%s'" % \
            (userName, hostName))
        return ret[0] != 0

    def loadSqlScript(self, scriptPath, dbName=None):
        """
        Load sql script from the file <scriptPath> into database <dbName>.

        @param scriptPath Path the the SQL script.
        @param dbName     Database name (optional).

        Note that in order to avoid exposing password, this function disallows
        passing password through arguments. Option file containing credentials
        must be used instead.
        """
        if "passwd" in self._kwargs:
            raise DbException(DbException.PASSWD_NOT_ALLOWED)
        connectArgs = self._kwargs.copy()
        if "read_default_group" in connectArgs:   # remove option that is not valid
            connectArgs.pop("read_default_group") # for MySQL client program
        if dbName is not None:
            connectArgs["db"] = dbName
        mysqlArgs = ["mysql"]
        if "read_default_file" not in connectArgs:
            # Match MySQLdb driver behavior and skip reading the usual options
            # files (/etc/my.cnf, ~/.my.cnf, etc...). Note, this argument needs to
            # be first, or MySQL will complain "unknown option".
            mysqlArgs.append("--no-defaults")
        for k in connectArgs:
            if k in argToOptionMap:
                s = "--%s=%s" % (argToOptionMap[k], connectArgs[k])
                # MySQL gets confused unless this option is first
                if k == "read_default_file":
                    mysqlArgs.insert(1, s)
                else:
                    mysqlArgs.append(s)
        dbInfo = " into db '%s'." % dbName if dbName is not None else ""
        self._logger.info("Loading script %s%s. Args are: %s" % \
                              (scriptPath, dbInfo, str(mysqlArgs)))
        with file(scriptPath) as scriptFile:
            if subprocess.call(mysqlArgs, stdin=scriptFile) != 0:
                msg = "Failed to execute %s < %s" % (connectArgs, scriptPath)
                raise DbException(DbException.CANT_EXEC_SCRIPT, msg)
