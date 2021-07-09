#!/bin/sh

#
# Load classes and create users
#

java -jar voltdb-javacache-demo-client.jar `cat $HOME/.vdbhostnames` 10000000 10 3 100 1000 1

for i in 2 4 8 12 16 20 24 28 32 
do 
	java -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` 10000000 $i 300 100 1000 0  > $i.lst
done
