#!/bin/sh

#
# Load classes and create users
#


. $HOME/.profile

USERCOUNT=1000000

cd ../jars

if
        [ ! -d jsr107 ]
then
        jar xvf voltdb-javacache-demo-server.jar
fi

if
        [ ! -d ../results ]
then
        mkdir ../results
fi


java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar `cat $HOME/.vdbhostnames` ${USERCOUNT} 1 3 100 1000 1 0

DT=`date '+%Y%m%d_%H%M%S'`

for i in 2 4 8 12 16 20 24 28 32
do
        java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` ${USERCOUNT} $i 300 100 1000  1 0 > ../results/${DT}_$i.lst
done
