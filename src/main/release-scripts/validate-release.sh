#!/usr/bin/env bash

RELEASE_VERSION=$1

if [ `git tag -l | grep $RELEASE_VERSION` ]
then
        echo "ERROR : tag '$RELEASE_VERSION' already exists, aborting. If you really want to release this version, delete the tag in the workspace first."
        exit 1
fi

if ! grep -q $RELEASE_VERSION readme.md ;
then
        echo "ERROR: readme.md has not been updated"
        exit 1
fi

if ! grep -q $RELEASE_VERSION changelog.txt ;
then
        echo "ERROR: changelog.txt has not been updated"
        exit 1
fi
