#!/bin/bash

if [ "$1" == "" ]; then
  echo "Do: $0 <dirname>"
  exit 1
fi

tar czf "`basename $1`.par" "$1"
