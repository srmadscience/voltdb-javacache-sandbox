#!/bin/sh

#
# Load classes and create users
#

USERCOUNT=100000

. $HOME/.profile
cd ../jars

java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar `cat $HOME/.vdbhostnames` ${USERCOUNT} 10 3 100 1000 1 voltdb-javacache-demo-client.jar

for i in 2 4 8 12 16 20 24 28 32 
do 
	for lob in 1000 2000 4000 8000 
	do
		echo threads = $i lobsize = $lob
		java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` ${USERCOUNT} $i 300 100 ${lob} 1 voltdb-javacache-demo-client.jar > $i.lst
		sleep 60
	done
done
