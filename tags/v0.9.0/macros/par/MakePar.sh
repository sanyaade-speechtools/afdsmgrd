#!/bin/bash

#
# MakePar.sh -- by Dario Berzano <dario.berzano@gmail.com>
#
# This file is part of afdsmgrd -- see http://code.google.com/p/afdsmgrd
#
# Utility to create PARFiles out of directories. It launches a tar gzip command
# using an option to exclute versioning control files from being included in the
# archive.
#
# PARFiles are created in the same directory that contains MakePar.sh.
#

#
# Functions
#

# Returns a full normalized form of the given path
function NormalizePath() {
  local FN=$(basename "$1")
  local DN=$(dirname "$1")
  DN=$(cd "$DN" ; pwd)
  DN="$DN/$FN"

  while [ "${DN:0:2}" == "//" ]; do
    DN="${DN:1}"
  done

  echo $DN
}

#
# Entry point
#

if [ "$1" == "" ]; then
  echo "To create a PARFile, do: $0 <dirname>"
  exit 1
fi

DIR=$(NormalizePath "$1")
PAR=$(basename "$1")

if [ "$PAR" == "/" ]; then
  echo "Cannot create PARFiles from the root directory!"
  exit 2
fi

PAR=$(dirname "$0")/"$PAR".par
PAR=$(NormalizePath "$PAR")

echo -e "\033[1;35mInput dir\033[m  : $DIR"
echo -e "\033[1;35mOutput PAR\033[m : $PAR"

CDDIR=$(dirname "$DIR")
DIR=$(basename "$DIR")

cd "$CDDIR"
tar czf "$PAR" "$DIR" --exclude-vcs
