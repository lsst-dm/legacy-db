# LSST Data Management System
# Copyright 2014-2015 LSST Corporation.
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
This module contains utilities / helpers that perform common tasks,
in particular these that are specific to different drivers.

@author  Jacek Becla, SLAC
"""


# standard library
import logging as log
import re
import os

# third party
import sqlalchemy
from sqlalchemy.exc import DBAPIError, InvalidRequestError, NoSuchModuleError, \
                           NoSuchTableError, OperationalError, ProgrammingError
from sqlalchemy.sql import text
from sqlalchemy.inspection import inspect

from MySQLdb.constants import FIELD_TYPE


# MySQL errors that we are catching
# Names and numbers from include/mysql/mysql.h
class MySqlErr:
    ER_DB_CREATE_EXISTS = 1007
    ER_DB_DROP_EXISTS = 1008
    ER_NO_DB_ERROR = 1046
    ER_BAD_DB_ERROR = 1049
    ER_TABLE_EXISTS_ERROR = 1050
    ER_BAD_TABLE_ERROR = 1051
    ER_NO_SUCH_TABLE = 1146


class NoSuchDatabaseError(ProgrammingError):
    """Database does not exist."""

class InvalidDatabaseNameError(ProgrammingError):
    """Invalid database name."""

class DatabaseExistsError(ProgrammingError):
    """Database already exists."""

class TableExistsError(ProgrammingError):
    """Table already exists."""

#### Database-related functions ####################################################
def createDb(conn, dbName, mayExist=False):
    """
    Create database <dbName>.

    @param conn        Database connection or engine.
    @param dbName      Database name.
    @param mayExist    Flag indicating what to do if the database exists.

    Raises InvalidDatabaseNameError if database name is invalid.
    Raises DatabaseExistsError if the database already exists and mayExist is False.
    Raises sqlalchemy exceptions.

    Note, it will not connect to that database and it will not make it default.
    """
    if dbName is None:
        raise InvalidDatabaseNameError("CREATE DATABASE",
                                       "None passed as database name")

    # consider using create_database from helpers:
    # http://sqlalchemy-utils.readthedocs.org/en/latest/database_helpers.html
    if conn.engine.url.get_backend_name() == "mysql":
        try:
            conn.execute("CREATE DATABASE `%s`" % dbName)
        except ProgrammingError as e:
            if e.orig.args[0] == MySqlErr.ER_DB_CREATE_EXISTS:
                if not mayExist:
                    raise DatabaseExistsError("CREATE DATABASE", dbName, e.orig)
            else:
                raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())

def useDb(conn, dbName):
    """
    Connect to database <dbName>.

    @param conn        Database connection or engine.
    @param dbName      Database name.

    Raises NoSuchDatabaseError if the databases does not exists.
    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        try:
            conn.execute("USE `%s`" % dbName)
        except DBAPIError as e:
            if e.orig.args[0] == MySqlErr.ER_BAD_DB_ERROR:
                raise NoSuchDatabaseError("USE", dbName, e.orig)
            raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def dbExists(conn, dbName):
    """
    Return True if database <dbName> exists, False otherwise.

    @param conn        Database connection or engine.
    @param dbName      Database name.

    Raises sqlalchemy exceptions.
    """
    return dbName in inspect(conn).get_schema_names()


def dropDb(conn, dbName, mustExist=True):
    """
    Drop database <dbName>.

    @param conn        Database connection or engine.
    @param dbName      Database name.
    @param mustExist   Flag indicating what to do if the database does not exist.

    Raises NoSuchDatabaseError if the database does not exist and the
    flag mustExist is set to True.
    Raises sqlalchemy exceptions.

    Disconnect from the database if it is the current database.
    """
    if not mustExist and not dbExists(conn, dbName):
        return
    # consider using create_database from helpers:
    # http://sqlalchemy-utils.readthedocs.org/en/latest/database_helpers.html
    if conn.engine.url.get_backend_name() == "mysql":
        try:
            conn.execute("DROP DATABASE `%s`" % dbName)
        except DBAPIError as e:
            if e.orig.args[0] == MySqlErr.ER_DB_DROP_EXISTS:
                raise NoSuchDatabaseError("DROP DATABASE", dbName, e.orig)
            else:
                raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())

def listDbs(conn):
    """
    Return list of databases.

    @param engine      Database engine.

    Raises sqlalchemy exceptions.
    """
    return inspect(engine).get_schema_names()


#### Table-related functions #######################################################
def tableExists(conn, tableName, dbName=None):
    """
    Return True if table <tableName> exists in database <dbName>.

    @param conn        Database connection or engine.
    @param tableName   Table name.
    @param dbName      Database name.

    If <dbName> is not set, the current database name will be used.

    Raises sqlalchemy exceptions.
    """

    # sqlalchemy will throw exception if we call has_table("nonExistentDb", "t")
    # and we are not connected to any database. The code below fixes that bug
    if dbName:
        if not dbExists(conn, dbName):
            return False
        return conn.engine.has_table(tableName, dbName)
    elif not conn.engine.url.database:
        return False
    return conn.engine.has_table(tableName)


def createTable(conn, tableName, tableSchema, dbName=None, mayExist=False):
    """
    Create table <tableName> in database <dbName>.

    @param conn        Database connection or engine.
    @param tableName   Table name.
    @param tableSchema Table schema starting with opening bracket. Note that it can
                       NOT contain "--" or ";".
    @param dbName      Database name.
    @param mayExist    Flag indicating what to do if the database exists.

    If database <dbName> is not set, and "use <database>" was called earlier,
    it will use that database.

    Raises TableExistsError if the table already exists and mayExist flag
    is say to False.
    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        dbNameStr = "`%s`." % dbName if dbName is not None else ""
        cmd = "CREATE TABLE %s`%s` %s" % (dbNameStr, tableName, tableSchema)
        try:
            conn.execute(cmd)
        except DBAPIError as e:
            if e.orig.args[0] == MySqlErr.ER_NO_DB_ERROR:
                raise InvalidDatabaseNameError(cmd, dbNameStr, e.orig)
            elif e.orig.args[0] == MySqlErr.ER_TABLE_EXISTS_ERROR:
                if mayExist:
                    return
                raise TableExistsError(cmd, dbNameStr + tableName, e.orig)
            else:
                raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def createTableLike(conn, dbName, tableName, templDb, templTable):
    """
    Create table <dbName>.<tableName> like <templDb>.<templTable>

    @param conn        Database connection or engine.
    @param dbName      Name of the database where the tables should be created
    @param tableName   Name of the table to create
    @param templDb     Name of the database where the template table is
    @param templTable  Name of the template table

    Raises TableExistsError if the to-be-created table already exists.
    Raises NoSuchTableError if the template table does not exists.
    Raises sqlalchemy exceptions.
    """

    if conn.engine.url.get_backend_name() == "mysql":
        query = "CREATE TABLE {0}.{1} LIKE {2}.{3}".format(dbName, tableName,
                                                           templDb, templTable)
        try:
            conn.execute(query)
        except OperationalError as e:
            if e.orig.args[0] == MySqlErr.ER_TABLE_EXISTS_ERROR:
                raise TableExistsError(dbName + '.' + tableName)
            raise
        except ProgrammingError as e:
            if e.orig.args[0] == MySqlErr.ER_NO_SUCH_TABLE:
                raise NoSuchTableError(templTable)
            raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def createTableFromSchema(conn, schema):
    """
    Create database table from given schema.

    @param conn        Database connection or engine.
    @param schema      String containing full schema of the table (it can be a dump
                       containing "CREATE TABLE", "DROP TABLE IF EXISTS", comments, etc.

    Raises TableExistsError if the table already exists.
    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        try:
            conn.execute(schema)
        except OperationalError as exc:
            log.error('Exception when creating table: %s', exc)
            if exc.orig.args[0] == MySqlErr.ER_TABLE_EXISTS_ERROR:
                raise TableExistsError()
            raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def dropTable(conn, tableName, dbName=None, mustExist=True):
    """
    Drop table <tableName> in database <dbName>.

    @param tableName   Table name.
    @param dbName      Database name.
    @param mustExist   Flag indicating what to do if the database does not exist.

    If <dbName> is not set, the current database name will be used.

    Raises NoSuchTableError if the table does not exist and the mustExist flag
    is set to True.
    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        dbNameStr = "`%s`." % dbName if dbName is not None else ""
        try:
            conn.execute("DROP TABLE %s`%s`" % (dbNameStr, tableName))
        except DBAPIError as e:
            if e.orig.args[0] == MySqlErr.ER_BAD_TABLE_ERROR:
                if mustExist:
                    raise NoSuchTableError(dbNameStr + tableName)
                return
            raise
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def listTables(conn, dbName=None):
    """
    Return list of tables in a given database. If dbName is none, it uses
    current database.

    Raises sqlalchemy exceptions.
    """
    if dbName == None:
        dbName = conn.engine.url.database

    # consider using inspector.get_table_names(). Issue: it needs to connect
    # to the database
    if conn.engine.url.get_backend_name() == "mysql":
        cmd = "SELECT TABLE_NAME FROM information_schema.TABLES "
        cmd += "WHERE TABLE_SCHEMA='%s'" % dbName
        rows = conn.execute(cmd)
        return [x[0] for x in rows]
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


def isView(conn, tableName, dbName=None):
    """
    Return True if the table <tableName> is a view, False otherwise.

    @param tableName   Table name.
    @param dbName      Database name.

    @return boolean    True if the table is a view. False otherwise.

    If <dbName> is not set, the current database name will be used.

    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        dbNameStr = "'%s'" % dbName if dbName is not None else "DATABASE()"
        rows = conn.execute("SELECT table_type FROM information_schema.tables "
               "WHERE table_schema=%s AND table_name='%s'" % (dbNameStr, tableName))
        row = rows.first()
        if not row:
            return False
        return row[0] == 'VIEW'
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


#### User-related functions ########################################################
def userExists(conn, userName, hostName):
    """
    Return True if user <hostName>@<userName> exists, False otherwise.

    Raises sqlalchemy exceptions.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        return conn.execute(
            "SELECT COUNT(*) FROM mysql.user WHERE user='%s' AND host='%s'" % \
                (userName, hostName)).scalar() == 1
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())


#### Unclassified functions ########################################################
def typeCode2Name(conn, code):
    """
    Convert type code to type name, returns None if there is no mapping.
    """
    if conn.engine.url.get_backend_name() == "mysql":
        for name in dir(FIELD_TYPE):
            if getattr(FIELD_TYPE, name) == code:
                return name
        return None
    else:
        raise NoSuchModuleError(conn.engine.url.get_backend_name())
