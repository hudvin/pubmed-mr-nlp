#!/bin/sh
export JAVA_OPTS="-Xmx2048m"

hadoop/bin/hadoop jar myreceipts_importer-1.0-SNAPSHOT.jar  com.thirdsolutions.myreceipts.tools.aggregation.MyReceiptsAggregation  $1 $2 -D mapred.map.tasks = 50