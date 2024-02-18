#!/bin/bash

usage() { echo "Usage: $0 [-i <iterations (default: 1)>] [-s <sources (default: 3)>]" 1>&2; exit 1; }

SOURCES=3
ITERATIONS=1
while getopts ":i:s:" o; do
  case "${o}" in
    i)
      ITERATIONS=${OPTARG}
      ;;
    s)
      SOURCES=${OPTARG}
      ;;
    *)
      usage
      ;;
  esac
done

JVM_OPTS="-XX:+UseG1GC -Xmx4G -Xlog:gc,safepoint:gc.log"

echo "Running benchmark suite for $ITERATIONS iterations using $SOURCES sources"

N_FILES=$(ls -1 data/ 2>/dev/null | wc -l)
if [ $N_FILES -lt $SOURCES ] || [ ! -f "data/$[SOURCES - 1].messages" ]; then
  echo "Running file generator for $SOURCES sources"
  time java -cp zbench-2.0-SNAPSHOT.jar se.motility.zbench.generator.FileGenerator $SOURCES
  wait
  echo "Source generation completed"
fi

for i in $(seq 1 $SOURCES); do
  echo "Starting benchmark for $ITERATIONS iterations using $i sources"
  time java JVM_OPTS -jar zbench-2.0-SNAPSHOT.jar $i $ITERATIONS
  wait
  echo "Benchmark completed for $i sources"
done

echo "Benchmark completed"

