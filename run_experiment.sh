#!/bin/sh

sbt package
spark-submit --driver-memory 20G --class ExperimentRunner target/scala-2.10/sparkgrep_2.10-1.0.jar

