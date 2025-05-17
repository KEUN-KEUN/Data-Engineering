#!/bin/bash
# HDFS 초기화 및 데몬 실행
/opt/hadoop/bin/hdfs namenode -format -force
/opt/hadoop/sbin/start-dfs.sh
