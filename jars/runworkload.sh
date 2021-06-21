#!/bin/sh

for i in 2 4 8 12 16 20 24 28 32 
do 
	java -jar voltdb-javacache-demo-client.jar vdb1,vdb2,vdb3 10000000 $i 300 100 1000 0  > $i.lst
done
