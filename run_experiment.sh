#!/bin/sh
export HADOOP_CONF_DIR=/etc/hadoop/conf
sbt package
spark-submit --master yarn --driver-memory 20G --class ExperimentRunner target/scala-2.10/sparkgrep_2.10-1.0.jar

