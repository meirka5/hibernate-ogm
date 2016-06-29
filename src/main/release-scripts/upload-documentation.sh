#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "Release version not supplied"
fi

if [ -z "$2" ]
  then
    echo "Family version not supplied"
fi

RELEASE_VERSION=$1
VERSION_FAMILY=$2

echo "##################################################"
echo "# Hibernate OGM release $RELEASE_VERSION"
echo "# `date`"
echo "# Uploading documentation: Family: $VERSION_FAMILY"
echo "##################################################"

unzip distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.zip -d distribution/target/unpacked
rsync -rzh --progress --delete --protocol=28 distribution/target/unpacked/dist/docs/ filemgmt.jboss.org:/docs_htdocs/hibernate/ogm/$VERSION_FAMILY

