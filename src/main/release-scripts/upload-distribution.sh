#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "Release version not supplied"
fi


RELEASE_VERSION=$1
DIST_PARENT_DIR=${2:-'/home/frs/project/hibernate/hibernate-ogm'}

echo "##################################################"
echo "# Hibernate OGM release $RELEASE_VERSION"
echo "# `date`"
echo "# Performing deployment, on folder:"
echo "# $DIST_PARENT"
echo "##################################################"

(echo mkdir $DIST_PARENT_DIR/$RELEASE_VERSION; echo quit) | sftp -b - frs.sourceforge.net
scp readme.md frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
scp changelog.txt frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
scp distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.zip frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
scp distribution/target/hibernate-ogm-$RELEASE_VERSION-dist.tar.gz frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
scp modules/wildfly/target/hibernate-ogm-modules-wildfly10-$RELEASE_VERSION.zip frs.sourceforge.net:$DIST_PARENT_DIR/$RELEASE_VERSION
