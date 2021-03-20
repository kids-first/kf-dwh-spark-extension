#!/bin/bash
set -x
mkdir -p ~/.ivy2 ~/.sbt ~/.m2 ~/.sbt_cache
docker run --rm -v $(pwd):/app/build \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/build hseeberger/scala-sbt:8u282_1.4.9_2.12.13 \
    sbt -Duser.home=/app clean package

aws s3 cp ./target/scala-2.12/kf-dwh-spark-extension_2.12-0.2.jar s3://kf-strides-variant-parquet-prd/ami_libraries/spark_lib/kf-dwh-spark-extension_2.12-0.2.jar