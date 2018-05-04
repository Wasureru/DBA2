#!/bin/bash

if [ "$1" != '' ]
then
	chmod 700 add_header_and_split.sh
	chmod 700 importData.sh
	echo "adding header to data file and splitting records for faster load"
	time ./add_header_and_split.sh "$1"
	echo "created data files (BUS_DATA_?.csv)"
	echo "dropping database"
	mongo assignment1 -eval "db.dropDatabase()"
	echo "importing files into mongodb"
	time ./importData.sh
	echo "DONE!"
	echo "test queries"
	time mongo assignment1 -eval "db.business.find().pretty()"
else
	echo "Usage: bash part2.sh input_csv_file"
fi
