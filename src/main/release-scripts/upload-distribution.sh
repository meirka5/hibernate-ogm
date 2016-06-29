#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "Release version not supplied"
    exit 1
fi


RELEASE_VERSION=$1
DIST_PARENT_DIR=${2:-'/home/frs/project/hibernate/hibernate-ogm'}
WORKSPACE=${WORKSPACE:-'.'}

echo "##################################################"
echo "# Uploading Hibernate OGM $RELEASE_VEVISION on"
echo "# $DIST_PARENT"
echo "##################################################"

echo "Worksapce: $WORKSPACE"
#(echo mkdir $DIST_PARENT_DIR/$RELEASE_VERSION; echo quit) | sftp -b - frs.sourceforge.net
#scp readme.md frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
#scp changelog.txt frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
#scp distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.zip frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
#scp distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.tar.gz frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
#scp modules/wildfly/target/hibernate-ogm-modules-wildfly10-$RELEASE_VERSION.zip frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
