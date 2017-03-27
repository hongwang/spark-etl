#!/usr/bin/env bash

HOSTS=(${1//,/ })

for host in ${HOSTS[@]}
do
    RESULT=`curl -s "http://$host/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"`
    if [[ $RESULT =~ "active" ]];then
        HOST=$host
    fi
done

echo ${HOST}