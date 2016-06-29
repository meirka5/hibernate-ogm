#!/usr/bin/env bash

RELEASE_VERSION=$1
WORKSPACE=${WORKSPACE:-'.'}

echo "Version '$RELEASE_VERSION'"

if [ -z "$RELEASE_VERSION" ];
then
  echo "ERROR: Release version argument not supplied"
  exit 1
else
  echo "Version is set to '$RELEASE_VERSION'";
fi

pushd $WORKSPACE
mvn clean versions:set -s settings-example.xml -DnewVersion=$RELEASE_VERSION -DgenerateBackupPoms=false -f bom/pom.xml
popd
