#!/bin/bash

#
# Special PARFile to always download the latest version of the afdsutil.C macro
#
# by Dario Berzano <dario.berzano@gmail.com>
#

# Where to find the macro and where to save it
export MACRO_URL="http://afdsmgrd.googlecode.com/svn/trunk/macros/afdsutil.C"
export LOCAL_DEST="/tmp/afdsutil.C"

# It may still exist
rm -f "$LOCAL_DEST"

# Is wget there?
which wget > /dev/null 2>&1
if [ $? == 0 ]; then
  wget "$MACRO_URL" -O "$LOCAL_DEST" -q
  RET=$?
else
  curl -s -o "$LOCAL_DEST" "$MACRO_URL"
  RET=$?
fi

# In case of errors remove the file and return nonzero
if [ $RET != 0 ]; then
  rm -f "$LOCAL_DEST"
  exit 1
fi
