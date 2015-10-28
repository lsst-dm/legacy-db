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
This module defines a function that produces an exception class from an integer and
arbitrary number of ancillary messages.

@author  Jacek Becla, SLAC

Known issues:
 * None
"""

def _myEx_init(self, errCode, *messages):
    self._errCode = errCode
    self._messages = messages

def _myEx_str(self):
    msg = self._errorMessages.get(self._errCode) or (
            "Unrecognized error: %r" % self._errCode)
    if self._messages:
        msg = msg + " (" + "), (".join(self._messages) + ")"
    return msg

def _myEx_errCode(self):
    return self._errCode

def _myEx_messages(self):
    return self._messages

def produceExceptionClass(theName, theList):
    """
    Produce exception class.

    @param theName   Name of the class
    @param theList   List of (error code, error code symbolic name, error message)
                     tuples
    """
    TheException = type(theName, 
                        (Exception, object,), 
                        dict(_errorMessages = {},
                             __init__ = _myEx_init,
                             __str__  = _myEx_str,
                             errCode  = _myEx_errCode,
                             messages = _myEx_messages))
    for x in theList:
        (errCode, errName, errMsg) = x
        setattr(TheException, errName, errCode)
        TheException._errorMessages[errCode] = errMsg
    return TheException
