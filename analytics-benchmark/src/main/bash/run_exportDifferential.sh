#!/bin/bash

WORKDIR=/home/artbau/workspace/analytics.git
CLASS_PATH=$WORKDIR/analytics-core/target/analytics-core-0.0.14-SNAPSHOT-assembly.jar
HADOOP_BIN=~/hadoop/bin/hadoop
INPUT_PATH1=$WORKDIR/experiments/bbc.co.uk/2012-12-05
INPUT_PATH2=$WORKDIR/experiments/rottentomatoes.com/2012-12-05
INPUT_PATH3=$WORKDIR/experiments/rkbexplorer.com/2012-12-05

    # Export tables of counters and bar graph for times of computation to latex
    java -cp $CLASS_PATH org.sindice.core.analytics.cascading.statistics.DifferentialTablesCountersExportCLI --input $INPUT_PATH1
    java -cp $CLASS_PATH org.sindice.core.analytics.cascading.statistics.DifferentialTablesCountersExportCLI --input $INPUT_PATH2
    java -cp $CLASS_PATH org.sindice.core.analytics.cascading.statistics.DifferentialTablesCountersExportCLI --input $INPUT_PATH3

