#!/bin/sh

#
# Load classes and create users
#


. $HOME/.profile

USERCOUNT=1000000

cd ../jars

java ${JVMOPTS} -cp /voltdb-javacache-sandbox/jars -jar voltdb-javacache-demo-client.jar `cat $HOME/.vdbhostnames` ${USERCOUNT} 1 3 100 1000 1 0

exit 0

for i in 2 4 8 12 16 20 24 28 32
do
        java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` ${USERCOUNT} $i 300 100 1000  1 0 > $i.lst
done
