#!/usr/bin/env bash

RELEASE_VERSION=$1
VERSION_FAMILY=$2
WORKSPACE=${WORKSPACE:-'.'}

if [ -z "$RELEASE_VERSION" ]
  then
    echo "Release version not supplied"
    exit 1
fi

if [ -z "$VERSION_FAMILY" ]
  then
    echo "Family version not supplied"
    exit 1
fi

echo "##################################################"
echo "# Hibernate OGM $RELEASE_VERSION"
echo "# Uploading documentation for Family: $VERSION_FAMILY"
echo "##################################################"

echo "Workspace $WORKSPACE"
unzip $WORKSPACE/distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.zip -d $WORKSPACE/distribution/target/unpacked
#rsync -rzh --progress --delete --protocol=28 $WORKSPACE/distribution/target/unpacked/dist/docs/ filemgmt.jboss.org:/docs_htdocs/hibernate/ogm/$VERSION_FAMILY

echo "Documentation uploaded!"
