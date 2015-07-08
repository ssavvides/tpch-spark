#!/bin/bash

##
## date: Feb/16/2015
## author <savvas@purdue.edu>
##
## Limit the tables to a given number of lines
##

# we need exactly 1 argument
if [ "$#" -ne 1 ]; then
  echo "Script requires exactly one argument: the number of lines to limit the files to."
  exit 0
fi


# the number of lines to keep
LINES=$1

for fullfile in $( ls *.tbl ); do

	# get file name without the extension.
    filename=$(basename "$fullfile")
    filename="${filename%.*}"

    # head -${LINES} ${fullfile} > "${filename}.${LINES}"
	
	# replaces original file
	head -${LINES} ${fullfile} > "tmp"
	cat "tmp" > ${fullfile}
done