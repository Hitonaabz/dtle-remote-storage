#!/usr/bin/python -O
# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 WALLIX. All rights reserved.
#
# Licensed computer software. Property of WALLIX.
# Product Name: WALLIX Bastion v6.0
# Author(s): Duc Toan Le <dtle@wallix.com>
# Module description:  Script to manage traces deposit to remote storage.
# Features :
# - Check if remote storage is mounted
# - Daily (tbd) transfer of traces
# - Integrity check on daily transfer
# - Archive files with file age

import os
#import subprocess
#import sys
import time
#import glob
import hashlib
import filecmp
import shutil
import logging
#import datetime
import re

BLOCKSIZE = 65536
REMOTE_PATH = "/var/wab/recorded/remote/"
RECORDING_PATH = "/var/wab/recorded/"
MOUNT_STATE = "0"
RECORDING_INDEX_PATH = "/tmp/"
RECORDING_INDEX = "recording_index.log"
RECORDING_INDEX_HASH = "recording_index.log.md5"
RECORDING_INDEX_HASH_OLD = "recording_index.log.md5.old"
LOG_FILE = "/tmp/storage.log"
LAST_EXEC_FILE = "/tmp/last_exec.log"
EXPR = re.compile('\\d{2}:\\d{2}:\\d{2}')

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='[%(levelname)s] %(asctime)s : %(message)s', datefmt='%Y/%m/%d %H:%M:%S')

open(LOG_FILE, "w").close() # Emptying the log file

# Checking if remote storage is mounted
if os.path.ismount(REMOTE_PATH) is True:
    logging.info("Remote storage is mounted")
    MOUNT_STATE = "0"
else:
    MOUNT_STATE = "1"
    logging.error("An issue has occured with the mountpoint, ending script...")
    exit()

# Transfer of traces of the day
# Check mount state
if MOUNT_STATE is "0":
    logging.info("Starting transfer process...")
    logging.info("[STEP 1] Listing created/modified traces to %s%s", RECORDING_INDEX_PATH, RECORDING_INDEX)
# List files modified today
    open(RECORDING_INDEX_PATH + RECORDING_INDEX, "w").close() # Emptying the index
    logging.info("Emptying the index...")
    LAST_EXECUTION = open(LAST_EXEC_FILE).readline()
    logging.info("Checking last execution")

# For RDP
    logging.info("Checking RDP traces...")
    DIR = os.path.dirname(RECORDING_PATH + "/rdp/")
    for fname in os.listdir(DIR):
        modtime = os.stat(os.path.join(DIR, fname)).st_mtime
        out = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modtime))
        if out > LAST_EXECUTION:
            logging.info("%s has changed since last execution. ", fname)
            f = open(RECORDING_INDEX_PATH + RECORDING_INDEX, "a+")
            f.write(fname + time.strftime('%H:%M:%S') + "\r\n")
            f.close()
        else:
            logging.debug("%s has not changed since last execution. ", fname)
# For SSH
    logging.info("Checking SSH traces...")
    DIR = os.path.dirname(RECORDING_PATH + "/ssh/")
    for fname in os.listdir(DIR):
        modtime = os.stat(os.path.join(DIR, fname)).st_mtime
        out = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modtime))
        if out > LAST_EXECUTION:
            logging.info("%s has changed since last execution. ", fname)
            f = open(RECORDING_INDEX_PATH + RECORDING_INDEX, "a+")
            f.write(fname + time.strftime('%H:%M:%S') + "\r\n")
            f.close()
        else:
            logging.debug("%s has not changed since last execution. ", fname)

logging.info("[STEP 2] Creating Hash file for comparison")

# Hash list of modified files for comparison
HASHER = hashlib.md5()
with open(RECORDING_INDEX_PATH + RECORDING_INDEX, 'rb') as afile:
    BUF = afile.read(BLOCKSIZE)
    while len(BUF) > 0:
        HASHER.update(BUF)
        BUF = afile.read(BLOCKSIZE)
        F = open(RECORDING_INDEX_PATH + RECORDING_INDEX_HASH, "w+")
        F.write(HASHER.hexdigest())
        F.close()

logging.info("[STEP 3] Comparing new Hash with Hash of D-1")
if (filecmp.cmp(RECORDING_INDEX_PATH + RECORDING_INDEX_HASH, RECORDING_INDEX_PATH + RECORDING_INDEX_HASH_OLD)) is True:
    logging.info("new Hash and old Hash are equals, exiting...")
    exit()
else:
    logging.warning("Hashes are different, starting file transfer...")
    logging.info("[STEP 4] Copying traces files from local storage to remote storage...")
    with open(RECORDING_INDEX_PATH + RECORDING_INDEX) as fp:
        for cnt, line in enumerate(fp):
            logging.debug("Trace file : %s", line)
            line = line.rstrip()
            line = re.sub(EXPR, '', line)
            if line.endswith(('.rdptrc', '.wrm', '.mwrm', '.log')):
                logging.debug("Copying : %s to remote storage", line)
                shutil.copy(RECORDING_PATH + "rdp/" + line, REMOTE_PATH)
            elif line.endswith('.ttyrec'):
                logging.debug("Copying : %s to remote storage", line)
                shutil.copy(RECORDING_PATH + "ssh/" + line, REMOTE_PATH)
            else:
                logging.warning("File : %s is not RDP nor SSH trace, ignoring...", line)

# Copying new Hash to old
logging.info("Renewing Hash file")
shutil.copy(RECORDING_INDEX_PATH + RECORDING_INDEX_HASH, RECORDING_INDEX_PATH + RECORDING_INDEX_HASH_OLD)

# Setting last execution date
LAST_EXECUTION = time.strftime('%Y-%m-%d %H:%M:%S')
with open(LAST_EXEC_FILE, "w") as f:
    f.write(LAST_EXECUTION)
    f.close()
