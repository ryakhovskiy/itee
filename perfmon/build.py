#!/usr/bin/env python

import os
import sys
import shutil
import zipfile

if not sys.platform.startswith("win"):
    print "build script can be run in windows only."
    exit(1)

print "starting build"
print "checking environment variables"

if 'JAVA_HOME' in os.environ:
    print "JAVA_HOME found"
else:
    print "No Java found. Please validate JAVA_HOME environment variable points to valid JAVA installation."
    exit(1)

if 'M2_HOME' in os.environ:
    print "M2_HOME found"
else:
    print "No Maven found. Please validate M2_HOME environment variable points to valid Maven installation."
    exit(1)

if 'PG_HOME' in os.environ:
    print "PG_HOME found"
else:
    print "No ProGuard found. Please validate PG_HOME environment variable points to valid ProGuard installation."
    exit(1)

if 'INTP_HOME' in os.environ:
    print "INTP_HOME found"
else:
    print "No INTP_HOME found."
    exit(1)


intp_home = os.environ['INTP_HOME']


def updateversion():
    print "updating version"
    import xml.etree.ElementTree as ET
    root = ET.parse(os.path.join(intp_home, "perfmon", "pom.xml")).getroot()
    root.findall("version")
    v = ''
    for x in root:
        if x.tag.endswith('version'):
            v = x.text
            break
    if len(v) == 0:
        return
    m = os.path.join(intp_home, "perfmon", "src", "main", "java", "org", "kr", "db", "loader", "ui", "AppMain.java")
    lines = []
    newline = ''
    with open(m, "r") as f:
        lines = f.readlines()
        i = 0
        for l in lines:
            if "public static final String APP_VERSION = " in l:
                oldver = l[l.index('=') + 2:l.index(';') + 1]
                newline = l.replace(oldver, '"' + v + '";')
                break
            i += 1
        lines[i] = newline
    with open(m, "w+") as f:
        f.writelines(lines)


def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            zip.write(os.path.join(root, file))

os.chdir(intp_home + os.sep + "perfmon")
updateversion()

print "building jar-package with dependencies"
r = os.system("mvn -DskipTests clean dependency:copy-dependencies package")
if r != 0:
    print "Cannot make build"
    exit(1)

pmjar = ''
for f in os.listdir("target"):
    if f.startswith('perfmon-') and f.endswith('.jar'):
        pmjar = f
        break
if pmjar == '':
    print "cannot find target jar"
    exit(1)

version = pmjar[len('perfmon-'):pmjar.index('.jar')]

print "preparing directories tree"
dpath = os.path.join(intp_home, "perfmon", "out", "perfmon")
if os.path.isdir(dpath):
    shutil.rmtree(dpath)
os.makedirs(dpath)
os.makedirs(dpath + os.sep + "lib")
os.makedirs(dpath + os.sep + "bin")

shutil.copy2("target" + os.sep + pmjar, dpath + os.sep + "_perfmon.jar")

for dir, d, files in os.walk("target" + os.sep + "dependency"):
    for x in files:
        shutil.copy2(dir + os.sep + x, dpath + os.sep + "lib" + os.sep + x)

print "obfuscating..."
command = os.path.join(os.environ['JAVA_HOME'], "bin", "java.exe") + ' -jar '
command += os.path.join(os.environ['PG_HOME'], "lib", "proguard.jar") + " @config.pro -verbose"
r = os.system(command)

if r != 0:
    print "Cannot obfuscate"
    exit(1)

os.remove(dpath + os.sep + "_perfmon.jar")
os.rename(dpath + os.sep + "perfmon.jar", os.path.join(dpath, "lib", "perfmon-" + version + ".jar"))

pmdir = os.path.join(intp_home, "perfmon")
for dir, d, files in os.walk(pmdir + os.sep + "bin"):
    for x in files:
        shutil.copy2(dir + os.sep + x, dpath + os.sep + "bin" + os.sep + x)

shutil.copy2(os.path.join(pmdir, "src", "main", "resources", "run.bat"), dpath + os.sep + "run.bat")
shutil.copy2(pmdir + os.sep + "mon.properties", dpath + os.sep + "mon.properties")

print "preparing zip"
os.chdir(pmdir + os.sep + "out")
zipf = zipfile.ZipFile('perfmon.zip', 'w')
zipdir(os.curdir + os.sep + "perfmon", zipf)
zipf.close()
shutil.rmtree("perfmon")
shutil.move("perfmon.zip", os.path.join(intp_home, "out", "perfmon.zip"))

print "done"