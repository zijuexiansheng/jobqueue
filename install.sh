#!/usr/bin/env bash

mkdir -p bin/
mkdir -p bin/aux/jobqueue
cp src/jobqueue.py bin/jobqueue

sed -i.bak 's#=>replace_me<=#'"${PWD}/bin/aux"'#' bin/jobqueue
rm bin/jobqueue.bak
chmod +x bin/jobqueue
