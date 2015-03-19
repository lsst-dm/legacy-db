#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008-2015 LSST Corporation.
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
This module implements a very basic connection pool - a dictionary  of named "db"
objects. Optionally, each db object can have it's connection refreshed to protect
against connection timeout.

@author  Jacek Becla, SLAC

"""

# standard library imports
import logging as log
from time import time

# local imports
from lsst.db.db import Db
from lsst.db.exception import produceExceptionClass

####################################################################################

DbPoolException = produceExceptionClass('DbException', [
    (1600, "ENTRY_NOT_FOUND", "Requested Db entry not found."),
    (1605, "ENTRY_EXISTS", "The Db entry already exists.")])

####################################################################################

class DbEntry:
    def __init__(self, checkFreq, lastChecked, **kwargs):
        self.kwargs = kwargs
        self.checkFreq = checkFreq
        self.lastChecked = lastChecked
        self.dbObj = None

####################################################################################

class DbPool(object):
    """
    @brief A pool of Db objects.

    This class implements a pool of Db objects. Optionally, it can maintain live
    connection and automatically recover from time outs.
    """

    def __init__(self):
        """
        Create a DbPool instance.
        """
        self._pool = {}
        self._log = log.getLogger("lsst.db.DbPool")

    ##### Connection-related functions #############################################
    def addConn(self, cName, checkFreq=600, **kwargs):
        """
        Add Db Connection Object to the pool.

        @param cName      Name of this connection
        @param dbConn     Db Connection Object
        @param checkFreq  Frequency of rechecking if connection is alive in seconds,
                          -1 for "don't check it at all"

        If will raise ENTRY_EXISTS exception if the entry already exists.
        """
        if cName in self._pool:
            raise DbPoolException(DbPoolException.ENTRY_EXISTS, cName)

        # Remember timestamp when connection was last checked.
        lastChecked = 0

        # the pool of named Db objects along with checkFreq and lastCheck time
        self._pool[cName] = DbEntry(checkFreq, lastChecked, **kwargs)

    def delConn(self, cName):
        """
        Remove Db Connection Object corresponding to a given name from the pool.

        If will raise ENTRY_NOT_FOUND exception if the entry is not found for a
        given name.
        """
        if cName not in self._pool:
            raise DbPoolException(DbPoolException.ENTRY_NOT_FOUND, cName)
        del self._pool[cName]

    def getConn(self, cName):
        """
        Return Db Connection Object corresponding to a given name, optionally
        ensure connection did not time out and reconnect if needed.

        If will raise ENTRY_NOT_FOUND exception if the entry is not found for a
        given name. It can raise MySQLdb.* exceptions if connection checking is on.
        """
        if cName not in self._pool:
            raise DbPoolException(DbPoolException.ENTRY_NOT_FOUND, cName)

        entry = self._pool[cName]
        if entry.dbObj is None:
            entry.dbObj = Db(**entry.kwargs)
        elif entry.checkFreq != -1:
            if time() - entry.lastChecked > entry.checkFreq:
                self._log.debug(
                    "Checking connection for '%s' before executing query.", cName)
                db = entry.dbObj
                if not db.isConnected():
                    self._log.debug("Attempting to reconnect for '%s'.", cName)
                    db.connect()
                entry.lastChecked = time()
        return entry.dbObj
