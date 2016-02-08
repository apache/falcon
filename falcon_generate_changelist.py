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
# Creates CHANGES.txt from git history.
# "Inspired by" https://github.com/apache/spark/blob/master/dev/create-release/generate-changelist.py
#
# Usage:
#   First set the new release version and previous release Git tag against which the changelog needs to be generated.
#   $  python generate-changelist.py


import os
import sys
import subprocess
import time
import traceback
import re
import glob

## Set the below variables before generating the CHANGES
NEW_RELEASE_VERSION = "0.9"
PREV_RELEASE_GIT_TAG = "release-0.8-rc0"
CAPITALIZED_PROJECT_NAME = "falcon".upper()

CHANGELIST = "CHANGES.txt"
OLD_CHANGELIST = "%s.old" % (CHANGELIST)
NEW_CHANGELIST = "%s.new" % (CHANGELIST)
TMP_CHANGELIST = "%s.tmp" % (CHANGELIST)

LOG_FILE_NAME = "changes_%s" % time.strftime("%h_%m_%Y_%I_%M_%S")
LOG_FILE = open(LOG_FILE_NAME, 'w')


def run_cmd(cmd):
    try:
        print >> LOG_FILE, "Running command: %s" % cmd
        output = subprocess.check_output(cmd, shell=True, stderr=LOG_FILE)
        print >> LOG_FILE, "Output: %s" % output
        return output
    except:
        traceback.print_exc()
        cleanup()
        sys.exit(1)


def append_to_changelist(file_name, string):
    with open(file_name, "a") as f:
        print >> f, string


def cleanup(ask=True):
    if ask is True:
        print "OK to delete temporary and log files? (y/N): "
        response = raw_input()
    if ask is False or (ask is True and response == "y"):
        tmp_files = glob.glob(os.path.join(TMP_CHANGELIST + "*"))
        for f in tmp_files:
            os.remove(f)
        if os.path.isfile(OLD_CHANGELIST):
            os.remove(OLD_CHANGELIST)
        LOG_FILE.close()
        os.remove(LOG_FILE_NAME)


print "Generating new %s for FALCON release %s" % (CHANGELIST, NEW_RELEASE_VERSION)
if os.path.isfile(TMP_CHANGELIST):
    os.remove(TMP_CHANGELIST)
if os.path.isfile(OLD_CHANGELIST):
    os.remove(OLD_CHANGELIST)

print "Getting commits between tag %s and HEAD" % PREV_RELEASE_GIT_TAG
hashes = run_cmd("git log %s..HEAD --pretty='%%h'" % PREV_RELEASE_GIT_TAG).split()

print "Getting details of %s commits" % len(hashes)

for h in hashes:
    subject = run_cmd("git log %s -1 --pretty='%%s' | head -1" % h).strip()
    body = run_cmd("git log %s -1 --pretty='%%b'" % h)
    committer = run_cmd("git log %s -1 --pretty='%%cn <%%ce>' | head -1" % h).strip()
    body_lines = body.split("\n")

    match = re.search("\[(\w+)\]" , subject)
    # Default category
    category = "BUG"
    if match:
        category = match.group(1).upper()
        print "Subject : %s, Category : %s " % (subject, category)
        subject = re.sub("\[(\w+)\]", "", subject)
    file_name = TMP_CHANGELIST + "_" + category
    if not (os.path.isfile(file_name)):
        append_to_changelist(file_name, category+"S")
        append_to_changelist(file_name, "=======================================")
    if "Merge pull" in subject:
        # Parse old format commit message
        append_to_changelist(file_name, "  %s" % subject)
        append_to_changelist(file_name, "  [%s]" % body_lines[0])
        append_to_changelist(file_name, "")

    elif "maven-release" not in subject:
        # Parse new format commit message
        # Get authors from commit message, committer otherwise
        authors = [committer]
        if "Author:" in body:
            authors = [line.split(":")[1].strip() for line in body_lines if "Author:" in line]

        # Generate GitHub PR URL for easy access if possible
        github_url = ""
        if "Closes #" in body:
            pr_num = [line.split()[1].lstrip("#") for line in body_lines if "Closes #" in line][0]
            github_url = "github.com/apache/falcon/pull/%s" % pr_num

        append_to_changelist(file_name, "  %s" % subject)
        append_to_changelist(file_name, "  %s" % ', '.join(authors))
        # for author in authors:
        #     append_to_changelist("  %s" % author)
        if len(github_url) > 0:
            append_to_changelist(file_name, "  Commit: %s, %s" % (h, github_url))
        else:
            append_to_changelist(file_name, "  Commit: %s" % h)
        append_to_changelist(file_name, "")

append_to_changelist(TMP_CHANGELIST, "FALCON Change Log")
append_to_changelist(TMP_CHANGELIST, "----------------")
append_to_changelist(TMP_CHANGELIST, "")
append_to_changelist(TMP_CHANGELIST, "Release %s" % NEW_RELEASE_VERSION)
append_to_changelist(TMP_CHANGELIST, "")
run_cmd("cat %s >> %s" % (TMP_CHANGELIST + "_*", TMP_CHANGELIST))
# Append old change list
print "Appending changelist from tag %s" % PREV_RELEASE_GIT_TAG
run_cmd("git show %s:%s | tail -n +3 >> %s" % (PREV_RELEASE_GIT_TAG, CHANGELIST, TMP_CHANGELIST))
run_cmd("cp %s %s" % (TMP_CHANGELIST, NEW_CHANGELIST))
print "New change list generated as %s" % NEW_CHANGELIST
cleanup(False)
