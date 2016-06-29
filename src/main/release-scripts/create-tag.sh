#!/usr/bin/env bash

RELEASE_VERSION=$1

echo "##################################################"
echo "# Hibernate OGM release"
echo "# `date`"
echo "# Creating tag $RELEASE_VERSION"
echo "##################################################"

git commit -a -m "[Jenkins release job] Preparing release $RELEASE_VERSION"
git tag $RELEASE_VERSION

