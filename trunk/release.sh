#!/bin/bash

#
# Used to make a SVN "tag" and the corresponding package, and to finally upload
# it.
#
# by Dario Berzano <dario.berzano@gmail.com>
#
# THIS IS NOT MEANT TO BE USED BY THE USER!!!
#

export TRUNKURL="https://afdsmgrd.googlecode.com/svn/trunk"
export TAGSURL="https://afdsmgrd.googlecode.com/svn/tags"
export PROG="afdsmgrd"
export WORKDIR="/tmp/$PROG-workdir-$RANDOM"
export ARCHDIR=`dirname $0`/../archives
export TAGSDIR=`dirname $0`/../tags

# Parse arguments
function Main() {

  if [ "$1" == "" ] || [ "$2" == "" ]; then
    echo "Usage: $0 [--tag|--arch|--upload] <version> [<upload_msg>]"
    echo "<version> is expected to be in the form MAJOR.MINOR.PATCHES"
    exit 1
  fi

  case "$1" in

    --tag)
      DoTag "$2"
    ;;

    --arch)
      DoArch "$2"
    ;;

    --upload)
      Upload "$2" "$3"
    ;;

  esac

}

# Make a new tag via svn copy. Also automatically corrects the version number
# in AfVersion.h and commits the change in tag only
function DoTag() {
  local VER=$1
  local ANS
  local RET
  echo "Making a tag for v$VER."
  echo ""
  echo "BEWARE: svn copy acts on the remote repository, so you must think of"
  echo "        committing every change if you want it to be included in this"
  echo "        tag."
  echo ""
  echo -n "Type \"Yes\" to continue: "
  read ANS
  if [ "$ANS" != "Yes" ]; then
    echo "Tagging aborted"
    exit 2
  fi

  svn copy $TRUNKURL $TAGSURL/v$VER -m " * Created tagged revision v$VER"
  RET=$?
  if [ $RET != 0 ]; then
    echo "svn copy failed with error $RET"
    echo -n "Type \"Yes\" to continue anyway: "
    read ANS
    if [ "$ANS" != "Yes" ]; then
      echo "Tagging aborted after failure of svn copy"
      exit $RET
    fi
  fi

  mkdir -p "$WORKDIR"
  cd "$WORKDIR"
  svn co $TAGSURL/v$VER/src/ --depth empty
  cd src
  svn up AfVersion.h
  perl -p -i -e 's/(#define[ \t]+AFVER[ \t]+)(.*)/$1\"v'$VER'\"/g' AfVersion.h
  svn ci -m " * Version number set to $VER in tag"

  RET=$?
  if [ $RET == 0 ]; then
    rm -rf "$WORKDIR"
  else
    echo "Working directory $WORKDIR not removed: svn ci failed with $RET"
  fi

  exit $?
}

# Creates an archive of a tagged version which must exist on svn. It cleans up
# the SVN information before archiving
function DoArch() {
  local VER=$1
  local ANS
  local RET
  echo "Making an archive for v$VER."
  echo -n "Type \"Yes\" to continue: "
  read ANS
  if [ "$ANS" != "Yes" ]; then
    echo "Archiving aborted"
    exit 2
  fi

  mkdir -p "$ARCHDIR"
  ARCHDIR=`cd "$ARCHDIR";pwd`
  local ARCH="$ARCHDIR/$PROG-v$VER.tar.gz"

  mkdir -p "$WORKDIR"
  cd "$WORKDIR"

  svn co $TAGSURL/v$VER $PROG

  RET=$?
  if [ $RET != 0 ]; then
    echo "svn failed with exitcode $RET"
    exit $RET
  fi

  # Eliminate .svn information
  find $PROG -name ".svn" -type d -exec rm -rf '{}' \; 2> /dev/null

  # Finally create archive
  tar czf "$ARCH" $PROG

  RET=$?
  if [ $RET == 0 ]; then
    rm -rf "$WORKDIR"
  else
    echo "Working directory $WORKDIR not removed: tar exited with $RET"
  fi

  exit $?
}

# Uploads a created archive on Google Code via the command line interface
# provided as a Python script on Google SVN
function Upload() {

  local VER="$1"

  cd "$ARCHDIR"

  local GUSER=`head -n1 .googlecode.pwd`
  local GPWD=`tail -n1 .googlecode.pwd`
  local ARCH="$PROG-v$VER.tar.gz"
  local SUMMARY="$PROG v$VER - $2"

  echo "You are about to upload a file to Google Code using the following info:"
  echo ""
  echo " * Username: $GUSER"
  echo " * Password: $GPWD"
  echo " * Filename: $ARCH"
  echo " * Summary:  $SUMMARY"
  echo ""
  echo -n "Is this correct? Type \"Yes\" if it is: "
  read ANS
  if [ "$ANS" != "Yes" ]; then
    echo "Uploading aborted"
    exit 3
  fi

  ./upload -s "$SUMMARY" -p $PROG -u $GUSER -w $GPWD $ARCH
}

#
# Entry point
#

Main "$@"
