#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-javacache-sandbox/scripts

sqlcmd --servers=vdb1 < ../ddl/create_db.sql
sqlcmd --servers=vdb1 < ../ddl/create_demo_db.sql
$HOME/bin/reload_dashboards.sh voltdb-javacache-sandbox.json
java ${JVMOPTS} -jar $HOME/bin/addtodeploymentdotxml.jar vdb1 deployment topics.xml


