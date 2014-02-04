#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014 LSST Corporation.
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
This module defines an exception class from an integer and arbitrary number of
ancillary messages.

@author  Jacek Becla, SLAC

Known issues:
 * None
"""


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
            "Unrecognized error: %r" % self.errorCode)
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
def _defineErr(errCode, errName, errMsg):
    setattr(DbException, errName, errCode)
    DbException._errorMessages[errCode] = errMsg
