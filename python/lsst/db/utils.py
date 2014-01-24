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
This module contains small utilities / helpers that perform common tasks related to
the Db wrapper.


@author  Jacek Becla, SLAC
"""


# standard library
import ConfigParser
import logging
import os.path
import sys

# local
from lsst.db.mysqlInternal import mysql_optionToConnectArgMap as optNameToConnArgMap


def readCredentialFile(fName, logger):
    """
    Reads all supported key/value pairs from fName and return a dictionary
    containing these key/value pairs translated to names accepted by connect()
    as needed). Hint, to get a subset, do something like:
    dict = readCredentialFile(fN)
    (hst, prt, usr, pwd) = [dict[k] for k in ('host', 'port', 'user', 'passwd')]
    """
    ret = {}
    theSection = "mysql"
    if fName.startswith('~'): 
        fName = os.path.expanduser(fName)
    if not os.path.isfile(fName):
        raise Exception("Required file '%s' not found" % fName)
    cnf = ConfigParser.ConfigParser()
    cnf.read(fName)
    if not cnf.has_section(theSection):
        raise Exception("Missing section '%s' in '%s'" % (theSection, fName))
    for o in optNameToConnArgMap:
        if cnf.has_option(theSection, o):
            logger.debug("'%s' --> '%s'" % (o, cnf.get(theSection, o)))
            theKey = optNameToConnArgMap.get(o, o)
            ret[theKey] = cnf.get(theSection, o)
    logger.info("fetched %s from '%s' (password not shown)" % (
            str(["%s:%s" % (x, ret[x]) for x in ret if not x == "passwd"]), fName))
    return ret
