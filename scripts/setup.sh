#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-policysandbox/scripts

sqlcmd --servers=vdb1 < ../ddl/create_db.sql
java -jar $HOME/bin/addtodeploymentdotxml.jar vdb1,vdb2,vdb3 deployment topics.xml
