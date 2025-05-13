#!/bin/sh

#
# Load classes and create users
#


USERCOUNT=100000

. $HOME/.profile
cd ../jars

for i in 1 2 4 8  16 32 
do 
	for lob in 10 1000 2000 4000 8000 
	do
		echo threads = $i lobsize = $lob
		java ${JVMOPTS} -jar voltdb-javacache-demo-client.jar  `cat $HOME/.vdbhostnames` ${USERCOUNT} $i 300 100 ${lob} 1 voltdb-javacache-demo-client.jar > $i.lst
		sleep 60
	done
done
