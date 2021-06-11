#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-javacache-sandbox/scripts

sqlcmd --servers=vdb1 < ../ddl/create_db.sql
sqlcmd --servers=vdb1 < ../ddl/demo_db.sql
java -jar $HOME/bin/addtodeploymentdotxml.jar vdb1 deployment topics.xml

cd ../jars
jar xvf voltdb-javacache-demo.jar
