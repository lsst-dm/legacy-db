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
 * pinging server right before every command. That seems heavy. Need to think about
   more lightweight approach.
"""

import ConfigParser
import logging
import _mysql_exceptions
import MySQLdb
import os.path
import StringIO
import subprocess
import warnings
from datetime import datetime
from time import sleep


####################################################################################
class DbException(Exception, object):
    """
    Database-specific exception class.
    """

    CANT_CONNECT_TO_DB = 1500
    CANT_EXEC_SCRIPT   = 1505
    DB_EXISTS          = 1510
    DB_DOES_NOT_EXIST  = 1515
    INVALID_DB_NAME    = 1520
    INVALID_OPT_FILE   = 1522
    MISSING_CON_INFO   = 1525
    SERVER_CONNECT     = 1530
    SERVER_DISCONN     = 1535
    SERVER_ERROR       = 1540
    NO_DB_SELECTED     = 1545
    NOT_CONNECTED      = 1550
    TB_DOES_NOT_EXIST  = 1555
    TB_EXISTS          = 1560
    SERVER_WARNING     = 1900
    INTERNAL           = 9999

    _errorMessages = {
        CANT_CONNECT_TO_DB: ("Can't connect to database."),
        CANT_EXEC_SCRIPT: ("Can't execute script."),
        DB_EXISTS: ("Database already exists."),
        DB_DOES_NOT_EXIST: ("Database does not exist."),
        INVALID_DB_NAME: ("Invalid database name."),
        INVALID_OPT_FILE: ("Can't open the option file."),
        MISSING_CON_INFO: ("Missing connection information."),
        SERVER_CONNECT: "Unable to connect to the database server.",
        SERVER_DISCONN: ("Failed to disconnect from the database server."),
        SERVER_ERROR: ("Internal database server error."),
        NO_DB_SELECTED: ("No database selected."),
        NOT_CONNECTED: "Not connected to the database server.",
        TB_DOES_NOT_EXIST: ("Table does not exist."),
        TB_EXISTS: ("Table already exists."),
        SERVER_WARNING: ("Warning."),
        INTERNAL: ("Internal error.")
    }

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

####################################################################################
class Db(object):
    """
    @brief Wrapper around MySQLdb.

    This class wraps MySQLdb. It adds extra functionality, like recovering from
    lost connection, It also implements some useful functions, like creating
    databases/tables. Connection is done either through host/port or socket (at
    least one of these must be provided). Password can be empty. If it can't
    connect, it will retry (and sleep).
    """

    def __init__(self, user=None, passwd=None, host=None, port=None, socket=None,
                 optionFile=None, local_infile=0, sleepLen=3, maxRetryCount=0):
        """
        Create a Db instance.

        @param user       User name.
        @param passwd     User's password.
        @param host       Host name.
        @param port       Port number.
        @param socket     Socket.
        @param optionFile Option file. Note that it can also contain parameters
                          like host/port/user/password.
        @param local_infile local_infile flag. Allowed values: 0, 1
        @param sleepLen   Length of sleep time in betwen retries, in seconds.
                          Default is 3
        @param maxRetryCount Number of retries in case there is connection
                          failure. There is a 3 sec sleep between each retry.
                          Default is 1200 (which translates to one hour).

        Raise exception if both host/port AND socket are invalid.
        """
        self._conn = None
        self._logger = logging.getLogger("DBWRAP")
        self._logger.debug("db __init__")
        self._isConnectedToDb = False
        self._sleepLen = sleepLen
        self._attemptMaxNo = 1+maxRetryCount
        self._attemptNo = 1
        self._socket = socket
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._optionFile = optionFile
        self._local_infile = local_infile

        if self.optionFile is not None:
            self.optionFile = os.path.expanduser(optionFile)
            ret = self._parseOptionFile()
            if "socket"   in ret and socket is None: self.socket = ret["socket"]
            if "host"     in ret and host   is None: self.host   = ret["host"]
            if "port"     in ret and port   is None: self.port   = ret["port"]
            if "user"     in ret and user   is None: self.user   = ret["user"]
            if "password" in ret and passwd is None: self.passwd = ret["password"]
        if self.port is not None:
            self.port = int(self.port)
        if self.passwd is None:
            self.passwd = ''
        # MySQL defaults to socket if it sees "localhost". 127.0.0.1 will force TCP.
        if self.host == "localhost":
            self.host = "127.0.0.1"
            self._logger.warning('"localhost" specified, switching to 127.0.0.1')
        # MySQL connection-related error numbers. 
        # These are typically recoverable by reconnecting.
        self._mysqlConnErrors = [2002, 2003, 2006, 2013] 
        # treat MySQL warnings as errors (catch them and throw DbException
        # with a special error code)
        warnings.filterwarnings('error', category=MySQLdb.Warning)

        if self.user is None:
            self._logger.error("Missing user credentials: user is None.")
            raise DbException(DbException.MISSING_CON_INFO, "invalid username")
        if self.socket is None:
            if self.host is None:
                self._logger.error("Missing connection info: socket=None, " +
                                   "host=None")
                raise DbException(DbException.MISSING_CON_INFO, 
                                  "invalid socket and host name")
            elif self.port<1 or self.port>65535:
                self._logger.error("Missing connection info: socket=None, " +
                                   "port is invalid (must be within 1-65535), " +
                                   "got: %d" % self.port)
                raise DbException(DbException.MISSING_CON_INFO, 
                                  "invalid port number, must be within 1-65535")

    def __del__(self):
        """
        Disconnect from the server.
        """
        self._logger.debug("db __del__")
        self.disconnect()

    def connect(self, dbName=None, preferTcp=False):
        """
        Connect to Database Server. If dbName is provided. it connects to the
        database. Priority of socket vs host/port is given through preferTcp flag.
        """
        if self.checkIsConnected():
            return
        kwargs = { 
            "user":         self.user,
            "passwd":       self.passwd,
            "local_infile": self.local_infile,
        }
        if self.optionFile:
            kwargs["read_default_file"] = self.optionFile
        if dbName is not None:
            kwargs["dbName"] = dbName
        self._logger.info("Connecting as '%s', attempt %d of %d" % \
                              (self.user, self._attemptNo, self._attemptMaxNo))
        if (self._host is not None and self._port is not None and
            (self.socket is None or preferTcp)):
            cProt = "tcp"
        elif self.socket is not None:
            cProt = "socket"
        else:
            raise DbException(DbException.MISSING_CON_INFO, "either a socket "
                              "file or a host name and port must be specified")
        # try connecting using preferred method
        if self._tryConnect(kwargs, cProt):
            self._attemptNo = 1
            return
        self._logger.debug("Connecting via '%s' failed, trying alternative" % cProt)
        ret = None
        if cProt == "tcp" and self.socket is not None:
            ret = self._tryConnect(kwargs, "socket")
        elif cProt == "socket" and self.host is not None and self.port is not None:
            ret = self._tryConnect(kwargs, "tcp")
        if ret:
            self._attemptNo = 1
        else:
            if self._attemptMaxNo == 1:
                raise DbException(DbException.SERVER_CONNECT)
            self._attemptNo += 1
            self._logger.debug("sleeping %s sec" % self.sleepLen)
            sleep(self.sleepLen)
            self.connect(dbName, preferTcp)

    def _tryConnect(self, kwargs, connProtocol):
        """
        Try to connect using kwargs arguments, use method is instructed by
        connectProtocol. Return True on success, False on recoverable error, and
        raise exception on non-recoverable error.
        """
        self._logger.debug("tryConnect, %s, %s" % (connProtocol, str(kwargs)))
        if connProtocol == "tcp":
            kwargs["host"] = self.host
            kwargs["port"] = self.port
            if "unix_socket" in kwargs: del kwargs["unix_socket"]
            self._logger.info("Using %s:%s'" % (self.host, self.port))
        elif connProtocol == "socket":
            kwargs["unix_socket"] = self.socket
            if "host" in kwargs: del kwargs["host"]
            if "port" in kwargs: del kwargs["port"]
            self._logger.info("Using socket '%s'" % self.socket)
        else:
            self._logger.debug("No valid protocol in _tryConnect")
            return False
        conn = None
        try:
            self._logger.debug("mysql.connect. " + str(kwargs))
            conn = MySQLdb.connect(**kwargs)
        except MySQLdb.Error as e:
            self._logger.info("Failed to establish MySQL connection " +
                   "using %s. [%d: %s]" % (connProtocol, e.args[0], e.args[1]))
            if e[0] in self._mysqlConnErrors and \
                 (self._attemptNo < self._attemptMaxNo or self._attemptMaxNo == 1):
                return False
            self._logger.error("Can't recover, sorry")
            raise DbException(DbException.SERVER_CONNECT, "%d: %s" % e.args[:2])
        except MySQLdb.Warning as w:
            self._logger.info("MySQL connection warning: %s" % w.message)
            raise DbException(DbException.SERVER_WARNING, w.message)
        else:
            self._conn = conn
            conn = None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass
        return True

    def disconnect(self):
        """
        Disconnect from the server.
        """
        if not self.checkIsConnected(): return
        self._logger.info("disconnecting")
        try:
            self._closeConnection()
        except MySQLdb.Error, e:
            msg = "Failed to disconnect. Error was: %d: %s." % (e.args[0],e.args[1])
            self._logger.error(msg)
            raise DbException(DbException.SERVER_DISCONN, msg)
        except MySQLdb.Warning as w:
            self._logger.warning("Disconnect produced warning: %s" % w.message)
            raise DbException(DbException.SERVER_WARNING, w.message)

        self._logger.debug("Connection to database server closed.")
        self._conn = None
        self._isConnectedToDb = False

    def useDb(self, dbName):
        """
        Connect to database <dbName>.

        @param dbName     Database name.
        """
        cDb = self.getCurrentDbName()
        if cDb is not None and cDb == dbName:
            return
        try:
            if not self.checkIsConnected():
                self._logger.info("in useDb, connecting to server")
                self.connect(dbName)
            else:
                self._logger.info("in useDb, connecting to db")
                self._conn.select_db(dbName)
        except MySQLdb.Error, e:
            self._logger.error("Failed to use db '%s'." % dbName)
            raise DbException(DbException.CANT_CONNECT_TO_DB, dbName)
        except MySQLdb.Warning as w:
            self._logger.warning("Select db '%s' produced warning: %s" % \
                                     (dbName, w.message))
            raise DbException(DbException.SERVER_WARNING, w.message)

        self._isConnectedToDb = True
        self._logger.info("Connected to db '%s'." % dbName)

    def checkIsConnected(self):
        """
        Check if there is connection to the server.
        """
        return self._conn != None and self._conn.open

    def createDb(self, dbName):
        """
        Create database <dbName>.

        @param dbName     Database name.

        Create a new database <dbName>. Raise exception if the database already
        exists. Connect to the server first if connection not open already. Note,
        it will not connect to that database and it will not make it default.
        """
        if dbName is None: 
            raise DbException(DbException.INVALID_DB_NAME, "<None>")
        self.connect()
        if self.checkDbExists(dbName):
            raise DbException(DbException.DB_EXISTS, dbName)
        self.execCommand0("CREATE DATABASE %s" % dbName)

    def checkDbExists(self, dbName):
        """
        Check if database <dbName> exists.

        @param dbName     Database name.

        @return boolean   True if the database exists, False otherwise.

        Check if a database <dbName> exists. If it is not set, the current database
        name will be used. Connect to the server first if connection not open
        already.
        """
        if dbName is None:
            return False
        self.connect()
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % dbName
        return 1 == self.execCommand1(cmd)

    def dropDb(self, dbName):
        """
        Drop database <dbName>.

        @param dbName     Database name.

        Drop a database <dbName>. Raise exception if the database does not exists.
        Connect to the server first if connection not open already. Disconnect from
        the database if it is the current database.
        """
        self.connect()
        if not self.checkDbExists(dbName):
            raise DbException(DbException.DB_DOES_NOT_EXIST, dbName)
        cDb = self.getCurrentDbName()
        self.execCommand0("DROP DATABASE %s" % dbName)
        if cDb == dbName:
            self.disconnect()

    def checkTableExists(self, tableName, dbName=None):
        """
        Check if table <tableName> exists in database <dbName>.

        @param tableName  Table name.
        @param dbName     Database name.

        @return boolean   True if the table exists, False otherwise.

        Check if table <tableName> exists in database <dbName>. If <dbName> is not
        set, the current database name will be used. Connect to the server first if
        connection not open already.
        """
        if dbName is None:
            dbName = self.getCurrentDbName()
            if dbName is None:
                return False
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = '%s' AND table_name = '%s'" % \
               (dbName, tableName)
        return 1 == self.execCommand1(cmd)

    def createTable(self, tableName, tableSchema, dbName=None):
        """
        Create table <tableName> in database <dbName>.

        @param tableName   Table name.
        @param tableSchema Table schema starting with opening bracket.
        @param dbName      Database name.

        Create a table <tableName> in database <dbName>. If database <dbName> is not
        set, the current database name will be used. Connect to the server first if
        connection not open already. Raises exception if the table already exists.
        """
        dbName = self._getCurrentDbNameIfNeeded(dbName)
        if self.checkTableExists(tableName, dbName):
            raise DbException(DbException.TB_EXISTS)
        self.execCommand0("CREATE TABLE %s.%s %s" % (dbName,tableName,tableSchema))

    def dropTable(self, tableName, dbName=None):
        """
        Drop table <tableName> in database <dbName>. 

        @param tableName  Table name.
        @param dbName     Database name.

        Drop table <tableName> in database <dbName>. If <dbName> is not set, the
        current database name will be used. Connect to the server first if
        connection not open already. Raises exception if the table does not exist.
        """
        dbName = self._getCurrentDbNameIfNeeded(dbName)
        if not self.checkTableExists(tableName, dbName):
            raise DbException(DbException.TB_DOES_NOT_EXIST)
        self.execCommand0("DROP TABLE %s.%s %s" % (dbName, tableName, tableSchema))

    def isView(self, tableName, dbName=None):
        """
        Check if the table <tableName> is a view.

        @param tableName  Table name.
        @param dbName     Database name.

        @return boolean   True if the table is a view. False otherwise.

        If <dbName> is not set, the current database name will be used. Connect to
        the server first if connection not open already. Raises exception if the
        table does not exist.
        """
        dbName = self._getCurrentDbNameIfNeeded(dbName)
        if not self.checkTableExists(tableName, dbName):
            raise DbException(DbException.TB_DOES_NOT_EXIST)
        return self.execCommand1("SELECT COUNT(*) FROM information_schema.tables "
                                 "WHERE table_schema='%s' AND table_name='%s' AND "
                                 "table_type=\'VIEW\'" % (dbName, tableName))

    def getTableContent(self, tableName, dbName=None):
        """
        Get contents of the table <tableName>. Start connection if necessary.

        @param tableName  Table name.
        @param dbName     Database name.

        @return string    Contents of the table.
        """
        dbName = self._getCurrentDbNameIfNeeded(dbName)
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
        return 0 != self.execCommand1(
            "SELECT COUNT(*) FROM mysql.user WHERE user='%s' AND host='%s'" % \
            (userName, hostName))

    def loadSqlScript(self, scriptPath, dbName):
        """
        Load sql script from the file in <scriptPath> into database <dbName>.

        @param scriptPath Path the the SQL script.
        @param dbName     Database name.
        """
        self._logger.info("loading script %s into db %s" %(scriptPath,dbName))
        if self.passwd:
            if self.socket is None:
                cmd = 'mysql -h%s -P%s -u%s -p%s %s' % \
                (self.host, self.port, self.user, self.passwd, dbName)
            else:
                cmd = 'mysql -S%s -u%s -p%s %s' % \
                (self.socket, self.user,self.passwd, dbName)
        else:
            if self.socket is None:
                cmd = 'mysql -h%s -P%s -u%s %s' % \
                (self.host, self.port, self.user, dbName)
            else:
                cmd = 'mysql -S%s -u%s %s' % \
                (self.socket, self.user, dbName)
        self._logger.debug("cmd is %s" % cmd)
        with file(scriptPath) as scriptFile:
            if subprocess.call(cmd.split(), stdin=scriptFile) != 0:
                msg = "Failed to execute %s < %s" % (cmd,scriptPath)
                raise DbException(DbException.CANT_EXEC_SCRIPT, msg)

    def execCommand0(self, command):
        """
        Execute SQL command that returns no rows.

        @param command    SQL command that returns no rows.
        """
        self._execCommand(command, 0)

    def execCommand1(self, command):
        """
        Execute SQL command that returns one row.

        @param command    SQL command that returns one row.

        @return string    Result.
        """
        return self._execCommand(command, 1)

    def execCommandN(self, command):
        """
        Execute SQL command that returns more than one row.

        @param command    SQL command that returns more than one row.

        @return string    Result.
        """
        return self._execCommand(command, 'n')

    def _execCommand(self, command, nRowsRet):
        """
        Execute SQL command which return any number of rows.

        @param command    SQL command.
        @param nRowsRet   Expected number of returned rows (valid: '0', '1', 'n').

        @return string Results from the query. Empty string if not results.

        If this function is called after database server was restarted, or if the
        connection timed out because of long period of inactivity, the command will
        fail. This function catches such problems and recovers by reconnecting and
        retrying.
        """
        self.connect()
        try:
            self._logger.info("pinging server")
            self._conn.ping()
        except MySQLdb.OperationalError as e:
            if e.args[0] in self._mysqlConnErrors:
                self._logger.info(
                    "reconnecting because of [%d: %s]" % (e.args[0], e.args[1]))
                self.disconnect()
                self.connect()

        cursor = self._conn.cursor()
        try:
            self._logger.debug("Executing '%s'." % command)
            cursor.execute(command)
        except (MySQLdb.Error, MySQLdb.OperationalError) as e:
            msg = "Database Error [%d]: %s." % (e.args[0],e.args[1])
            self._logger.error("Command '%s' failed: %s" % (command, msg))
            raise DbException(DbException.SERVER_ERROR, msg)
        except MySQLdb.Warning as w:
            self._logger.warning("Command '%s' produced warning: %s" % \
                                     (command, w.message))
            raise DbException(DbException.SERVER_WARNING, w.message)
        if nRowsRet == 0:
            ret = ""
        elif nRowsRet == 1:
            ret = cursor.fetchone()[0]
            self._logger.debug("Got: %s" % str(ret))
        else:
            ret = cursor.fetchall()
            self._logger.debug("Got: %s" % str(ret))
        cursor.close()
        return ret

    def getCurrentDbName(self):
        """
        Return the name of current database.
        """
        self.connect()
        cmd = "SELECT DATABASE()"
        return self.execCommand1(cmd)

    def _getCurrentDbNameIfNeeded(self, dbName):
        """
        Get valid dbName.

        @param dbName     Database name.
        @return string    Return <dbName> if it is valid, otherwise if the
                          current database name if valid return it.

        Get valid dbName (the one passed, or current database name). If neither is
        valid, raise exception.
        """
        if dbName is not None: 
            return dbName
        dbName = self.getCurrentDbName()
        if dbName is None:
            raise DbException(DbException.INVALID_DB_NAME, "<None>")
        return dbName

    def _closeConnection(self):
        """
        Close connection to the server.
        """
        self._logger.info("closing connection")
        if self._conn is None: return
        self._conn.close()
        self._conn = None

    def _parseOptionFile(self):
        """
        Returns a dictionary containing values for socket, host, port, user and
        password specified through optionFile (None for each value not given.)
        """
        # it is better to parse the option file and explicitly check if socket,
        # host, port, username etc are valid, otherwise MySQL will try to default
        # to standard socket if something is wrong with the option file, and we
        # don't want any surprises.
        ret = {}
        options = ("socket", "host", "port", "user", "password")
        for o in options: ret[o] = None

        if not os.path.isfile(self.optionFile):
            self._logger.error("Can't find '%s'." % self.optionFile)
            raise DbException(DbException.INVALID_OPT_FILE, self.optionFile)

        cnf = ConfigParser.ConfigParser()
        cnf.read(self.optionFile)
        if cnf.has_section("client"):
            for o in options:
                if cnf.has_option("client", o): ret[o] = cnf.get("client", o)
        self._logger.info("connection info from option file '%s': %s" % \
                              (self.optionFile, str(ret)))
        return ret

    @property
    def sleepLen(self):
        return self._sleepLen

    @property
    def socket(self):
        return self._socket

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def passwd(self):
        return self._passwd

    @property
    def optionFile(self):
        return self._optionFile

    @property
    def local_infile(self):
        return self._local_infile

    @socket.setter
    def socket(self, v):
        self._socket = v

    @host.setter
    def host(self, v):
        self._host = v

    @port.setter
    def port(self, v):
        self._port = v

    @user.setter
    def user(self, v):
        self._user = v

    @passwd.setter
    def passwd(self, v):
        self._passwd = v

    @optionFile.setter
    def optionFile(self, f):
        self._optionFile = f
