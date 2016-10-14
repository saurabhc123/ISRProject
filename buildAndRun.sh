#!/bin/sh

if [ "$#" -ne 1 ]
then
	  echo "Usage: buildAndRun.sh <training file path on hdfs>"
	    exit 1
fi


echo building the project
sbt package

echo running the script with $1
spark-submit --class scala.SparkGrep target/scala-2.10/sparkgrep_2.10-1.0.jar local[*] "$1" val
