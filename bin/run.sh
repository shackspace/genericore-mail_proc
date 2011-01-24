#!/bin/bash
mongod &
BINDIR=`pwd`/../src
python2 $BINDIR/main.py "$@"
