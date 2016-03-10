#!/usr/bin/python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Usage:
#   $  python fetch-docs.py
import traceback
import subprocess
import os
import shutil

def run_cmd(cmd):
    try:
        print "Running command: %s" % cmd
        output = subprocess.check_output(cmd, shell=True)
        return output
    except:
        traceback.print_exc()
        sys.exit(1)

def create_pom(branch):
    f1 = open('pom.template', 'r')
    f2 = open('trunk/releases/%s/pom.xml' % branch, 'w')
    for line in f1:
        f2.write(line.replace('${branch}', branch))
    f1.close()
    f2.close()

branch = raw_input("Enter the Falcon branch from which you wish to copy the docs :")
# Copy docs from the branch into asf-site
print("Copy docs from https://git-wip-us.apache.org/repos/asf/falcon.git, branch %s" % branch)
os.mkdir("trunk/releases/%s" % branch)
run_cmd("git checkout origin/%s docs/src" % branch)
run_cmd("cp -r docs/src/site/ trunk/general/src/site")
os.rename("docs/src", "trunk/releases/%s/src" % branch)
run_cmd("git rm -r -f docs")
# Update poms
create_pom(branch)
print("Copied docs from branch %s " % branch)
print("To publish the new docs to falcon site do the following:")
print("1. Update trunk/releases/pom.xml to add the new doc module fetched")
print("2. Update trunk/pom.xml to new version. Update the team members list, if required.")
print("3. Update trunk/general/src/site/site.xml to add links to the new release and doc")
print("4. cd to trunk and build : mvn clean install")
print("5. git add the new files, commit and git push")
