#!/usr/bin/env bash

if [ -z "$1" ]
  then
    echo "Release version not supplied"
    exit 1
fi

RELEASE_VERSION=$1
BRANCH=${2:-'master'}
PUSH_CHANGES=${3:-false}

git commit -a -m "[Jenkins release job] Preparing next development iteration"

if [ "$PUSH_CHANGES" = true ] ; then
    echo "Pushing changes to the upstream repository."
    git push origin $BRANCH
    git push origin $RELEASE_VERSION
fi
if [ "$PUSH_CHANGES" != true ] ; then
    echo "WARNING: Not pushing changes to the upstream repository."
fi
