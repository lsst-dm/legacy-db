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
This module captures various mysql values and names needed in the Db wrapper.

@author  Jacek Becla, SLAC
"""


# Map of MySQLdb driver connect() keywords to mysql executable option names.
# Note: 'read_default_group' is not supported, since it can cause
# the MySQLdb driver and the mysql executable to connect differently.
mysql_connectArgToOptionMap = {
    'host':              'host',
    'user':              'user',
    'passwd':            'password',
    'db':                'database',
    'port':              'port',
    'unix_socket':       'socket',
    'connect_timeout':   'connect_timeout',
    'compress':          'compress',
    'named_pipe':        'pipe',
    'read_default_file': 'defaults-file',
    'charset':           'default-character-set',
    'local_infile':      'local-infile'
}

# Map of mysql executable option names to MySQLdb driver connect() keywords.
# Note: 'read_default_group' is not supported, since it can cause
# the MySQLdb driver and the mysql executable to connect differently.
mysql_optionToConnectArgMap = dict (zip(
        mysql_connectArgToOptionMap.values(),
        mysql_connectArgToOptionMap.keys()))
