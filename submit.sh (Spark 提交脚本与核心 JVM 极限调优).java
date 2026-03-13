#!/bin/bash

export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf

APP_NAME="TensOfMillions-Spatial-Trajectory-Matcher"
CLASS_NAME="com.spatial.TrajectoryMatcher"
JAR_PATH="../target/trajectory-spatial-matcher-1.0.0-SNAPSHOT.jar"

TRAJ_INPUT="hdfs://cluster/data/trajectory/daily"
FENCE_INPUT="hdfs://cluster/data/geofence/high_risk"
OUTPUT_DIR="hdfs://cluster/data/output/matched_results"

${SPARK_HOME}/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name ${APP_NAME} \
  --class ${CLASS_NAME} \
  --num-executors 30 \
  --executor-cores 4 \
  --executor-memory 16G \
  --driver-memory 8G \
  --conf "spark.executor.memoryOverhead=2G" \
  --conf "spark.sql.autoBroadcastJoinThreshold=209715200" \
  --conf "spark.sql.join.preferSortMergeJoin=false" \
  --conf "spark.network.timeout=600s" \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16M -XX:InitiatingHeapOccupancyPercent=45 -XX:MaxGCPauseMillis=150 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/gc-%t.log" \
  ${JAR_PATH} \
  ${TRAJ_INPUT} \
  ${FENCE_INPUT} \
  ${OUTPUT_DIR}