#!/bin/bash

PROJECT_ROOT="$(pwd)/../.."
export JAVA_HOME=/usr/lib/jvm/jdk-21.0.7-oracle-x64
export PATH=$JAVA_HOME/bin:$PATH

cd "$PROJECT_ROOT/spring-scylla-jpa" || exit

# Clean and package scylla-jpa-1.0-SNAPSHOT.jar
echo "Building scylla-jpa-1.0-SNAPSHOT jar..."
#mvn clean package -Dmaven.test.skip=true
mvn clean package

# Retrieve the version of the spring-scylla-jpa jar
VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
JAR_NAME="scylla-jpa-${VERSION}.jar"
echo "JAR_NAME: $JAR_NAME"


if [[ ! -f "target/${JAR_NAME}" ]]; then
  echo "Error: ${JAR_NAME} not found in target directory."
  exit 1
fi
echo "JAR file has been successfully built."

# Copy the common-scylla jar to all services
#for service_dir in "$PROJECT_ROOT"/{devicehistory,usermessaging,notification}; do
#  SERVICE_NAME=$(basename "$service_dir")
#
#  DEST_DIR="$service_dir/libs/spring-core-common"
#
#  mkdir -p "$DEST_DIR"
#
#  rm -f "$DEST_DIR"/common-scylla-"$VERSION".jar
#
#  cp "target/${JAR_NAME}" "$DEST_DIR/"
#  echo "Copied ${JAR_NAME} to $DEST_DIR"
#done

#echo "JAR file copied successfully to all services."
