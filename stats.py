#!/usr/bin/env python
import os

p = "./"
mask = [".java", ".py", ".sql", ".bat", ".sh", ".jsp", ".xml", "intpd", ".properties"]

files = 0
lines = 0
for dirpath, dnames, fnames in os.walk(p):
    if "target" in dirpath:
        continue
    if "out" in dirpath:
        continue
    if ".idea" in dirpath:
        continue
    for f in fnames:
        satisfy = False
        for x in mask:
            if f.endswith(x):
                satisfy = True
                break
        if not satisfy:
            continue
        files += 1
        fn = os.path.join(dirpath, f)
        print fn
        with open(fn, 'r') as ff:
            l = ff.readlines()
            lines += len(l)

print "-" * 80
print "files:", files
print "lines:", lines
