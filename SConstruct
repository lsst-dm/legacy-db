# -*- python -*-

env = Environment()
Export('env')

SConscript('python/SConscript')

for f in env.Glob("tests/*.py"):
    print "running", f

