# -*- python -*-

env = Environment()
Export('env')

pFiles = SConscript('python/SConscript')

Export('pFiles')
SConscript('tests/SConscript')
