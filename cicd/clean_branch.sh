#!/bin/bash

###
# Sanitise a git branch name so that it can be used in AWS resource and stack names
#
# e.g. feature/Create_new_logo! will be changed to feature-create-new-logo

SRC_BRANCH=$1

echo $SRC_BRANCH | sed 's/[/_]/-/g' | sed 's/[^a-zA-Z0-9-]//g' | tr '[:upper:]' '[:lower:]' | cut -c 1-30
