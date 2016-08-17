# LSST Data Management System
# Copyright 2015 LSST Corporation.
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
This module contains functions used by testEngineFactory*.py

@author  Jacek Becla, SLAC
"""

# standard library
try:
    import ConfigParser as configparser
except ImportError:
    import configparser
import logging as log
import os
import subprocess

# third party
from sqlalchemy.exc import InvalidRequestError


class MissingOptFileError(InvalidRequestError):
    """Missing option file."""


class InvalidOptFileError(InvalidRequestError):
    """Invalid option file."""


class PasswordNotAllowedError(InvalidRequestError):
    """Password is not allowed."""


class CannotExecuteScriptError(InvalidRequestError):
    """Cannot execute script."""

# Map of MySQLdb driver connect() keywords to mysql executable option names.
# Note: 'read_default_group' is not supported, since it can cause
# the MySQLdb driver and the mysql executable to connect differently.
connectArgToOptionMap = {
    'host': 'host',
    'user': 'user',
    'passwd': 'password',
    'db': 'database',
    'port': 'port',
    'unix_socket': 'socket',
    'connect_timeout': 'connect_timeout',
    'compress': 'compress',
    'named_pipe': 'pipe',
    'read_default_file': 'defaults-file',
    'charset': 'default-character-set',
    'local_infile': 'local-infile'
}

####################################################################################

# Map of mysql executable option names to MySQLdb driver connect() keywords.
# Note: 'read_default_group' is not supported, since it can cause
# the MySQLdb driver and the mysql executable to connect differently.
optionToConnectArgMap = {v: k for k, v in connectArgToOptionMap.items()}

####################################################################################


def readCredentialFile(fName):
    """
    Reads all supported key/value pairs from fName and return a dictionary
    containing these key/value pairs translated to names accepted by connect()
    as needed). Hint, to get a subset, do something like:
    dict = readCredentialFile(fN)
    (hst, prt, usr, pwd) = [dict[k] for k in ('host', 'port', 'user', 'passwd')].
    This function only reads from the [mysql] section, e.g., it is not full
    equivalent to how mysql command like utility which obtains the value from the
    last occurrence of k in section [mysql] or [client] in the file.

    Raises MissingOptFileError if the file can't be open.
    Raises InvalidOptFileError if the file does not contain requires section.
    """
    ret = {}
    fName = os.path.expanduser(fName)
    if not os.path.isfile(fName):
        raise MissingOptFileError(fName)
    cnf = configparser.ConfigParser()
    cnf.read(fName)

    theSection = "mysql"
    if not cnf.has_section(theSection):
        raise InvalidOptFileError(fName + ", Missing section '%s'" % theSection)
    for o in optionToConnectArgMap:
        if cnf.has_option(theSection, o):
            theKey = optionToConnectArgMap.get(o, o)
            ret[theKey] = cnf.get(theSection, o)
    return ret

####################################################################################


def loadSqlScript(scriptPath, **kwargs):
    """
    Load sql script from the file <scriptPath> into database <dbName>.

    @param scriptPath Path the the SQL script.

    @param kwargs     key-value pairs: host, user, db, port, unix_socket,
                      connection_timeout, compress, named_pipe, read_default_file,
                      char-set, local_infile. Note that for security reasons,
                      'passwd' is disallowed. Option file containing credentials
                      must be used to pass password.

    Raises PasswordNotAllowedError if password is specified through kwargs.
    Raises CannotExecuteScriptError if script can't be executed.
    """
    if "passwd" in kwargs:
        raise PasswordNotAllowedError()
    connectArgs = kwargs.copy()
    if "read_default_group" in connectArgs:   # remove option that is not valid
        connectArgs.pop("read_default_group")  # for MySQL client program
    if "host" in connectArgs and connectArgs["host"] == "localhost":
        connectArgs["host"] = "127.0.0.1"
    mysqlArgs = ["mysql"]
    if "read_default_file" not in connectArgs:
        # Match MySQLdb driver behavior and skip reading the usual options
        # files (/etc/my.cnf, ~/.my.cnf, etc...). Note, this argument needs to
        # be first, or MySQL will complain "unknown option".
        mysqlArgs.append("--no-defaults")
    for k in connectArgs:
        if k in connectArgToOptionMap:
            s = "--%s=%s" % (connectArgToOptionMap[k], connectArgs[k])
            # MySQL gets confused unless this option is first
            if k == "read_default_file":
                mysqlArgs.insert(1, s)
            else:
                mysqlArgs.append(s)
    dbInfo = " into db '%s'." % kwargs["db"] if "db" in kwargs else ""
    log.debug("Loading script %s%s. Args are: %s", scriptPath, dbInfo, mysqlArgs)
    with open(scriptPath) as scriptFile:
        if subprocess.call(mysqlArgs, stdin=scriptFile) != 0:
            msg = "Failed to execute %s < %s" % (connectArgs, scriptPath)
            raise CannotExecuteScriptError(msg)
