#!/usr/bin/env python

from lsst.dbserv.dbSetup import DbSetup
from lsst.dbserv.policyReader import PolicyReader
import getpass

r = PolicyReader()
(host, port) = r.readAuthInfo()
(globalDbName, dcVersion, dcDbName, dummy1, dummy2) = r.readGlobalSetup()

usr = raw_input("Enter mysql account name: ")
pwd = getpass.getpass()

x = DbSetup(host, port, usr, pwd)
x.setupUserDb(dcVersion)
