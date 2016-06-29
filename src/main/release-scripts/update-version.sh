#!/usr/bin/env bash

RELEASE_VERSION=$1
WORKSPACE=${WORKSPACE:-'.'}

if [ -z "$RELEASE_VERSION" ];
then
  echo "ERROR: Release version argument not supplied"
  exit 1
else
  echo "Setting version to '$RELEASE_VERSION'";
fi

pushd $WORKSPACE
mvn clean versions:set -s settings-example.xml -DnewVersion=$RELEASE_VERSION -DgenerateBackupPoms=false -f bom/pom.xml
popd
