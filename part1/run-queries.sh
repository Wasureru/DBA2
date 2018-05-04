#!/bin/bash

if [ "$1" != '' ]
then
	java org.apache.derby.tools.ij "$1"
else
	echo "Usage: ./run-queries.sh query-file.sql" 
fi
