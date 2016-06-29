#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "Version argument not supplied"
fi

$NEW_VERSION=$1

mvn clean versions:set -s settings-example.xml -DnewVersion=$NEW_VERSION -DgenerateBackupPoms=false -f bom/pom.xml

