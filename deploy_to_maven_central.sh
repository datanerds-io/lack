#!/bin/bash

read -p "Really deploy to maven cetral repository (yes/NO)? " -r DEPLOY

if [ "$DEPLOY" == "yes" ];  then
    read -p "Tag this build in github? (yes/NO)? " -r TAG
    read -p "Version Number (0.0.42) " -r REPLY
    gradle -Pversion=$REPLY clean build uploadArchives
    STATUS=$?
    if [ $STATUS -eq 0 ] && [ "$TAG" == "yes" ]; then
        echo "Pushing Tag to github"
        git tag v$REPLY
        git push --tags
    elif [ $STATUS -eq 1 ]; then
        echo "#################"
        echo "## BUILD ERROR ##"
        echo "#################"
    fi
else
    echo 'No yes, No deploy... :)'
fi
