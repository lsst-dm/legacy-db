# -*- python -*-

import subprocess
env = Environment()
Export('env')

SConscript('python/SConscript')

for f in env.Glob("tests/*.py"):
    print "Running test: ", f
    app = str(f.abspath)
    if subprocess.call(app):
        print f, "FAILED"
        Exit(2)
