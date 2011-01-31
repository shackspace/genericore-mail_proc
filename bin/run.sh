#!/bin/bash
BINDIR="`dirname "$0"`"/../src
mongod &
python2 $BINDIR/main.py "$@"
