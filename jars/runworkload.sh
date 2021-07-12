#!/bin/sh

#
# Load classes and create users
#

USERCOUNT=1000000

java -jar voltdb-javacache-demo-client.jar `cat $HOME/.vdbhostnames` ${USERCOUNT} 10 3 100 1000 1 voltdb-javacache-demo-client.jar

for i in 2 4 8 12 16 20 24 28 32 
do 
	java -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` ${USERCOUNT} $i 300 100 1000 0 voltdb-javacache-demo-client.jar > $i.lst
done
