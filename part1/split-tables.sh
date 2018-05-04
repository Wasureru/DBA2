#!/bin/bash

if [ "$1" != '' ]
then
	echo "reformatting dates"
	sed -E 's,([0-9]{2})/([0-9]{2})/([0-9]{4}),\1.\2.\3,g' "$1" > "$1".fixeddates
	echo "completed (in $1.fixeddates)"
	echo "splitting data into BUS_NAMES"
	awk -v FS='\t' 'NR>1{print $1","$2}' "$1".fixeddates > BUS_NAMES.csv
	echo "completed (in BUS_NAMES.csv)"
	echo "splitting data into BUS_STATUS"
	awk -v FS='\t' 'NR>1{if($3=="Registered") printf "TRUE"; if($3=="Deregistered") printf "FALSE"; print ","$4","$5","$6}' "$1".fixeddates > BUS_STATUS.csv
	echo -e "FALSE,Unregistered\nTRUE,Registered" > BUS_STATUS_OPTIONS.csv
	echo "completed (in BUS_STATUS.csv)"
	echo "splitting data into BUS_REGS"
#	awk -v FS='\t' 'NR>1{print $7","$8","$9}' "$1".fixeddates > BUS_REGS.csv
	awk -v FS='\t' 'NR>1{printf "%s,", $7; if($8=="ACT")printf "1"; if($8=="NSW") printf "2"; if($8=="NT") printf "3"; if($8=="QLD") printf "4"; if($8=="SA") printf "5"; if($8=="TAS") printf "6"; if($8=="VIC") printf "7"; if($8=="WA") printf "8"; if($8=="") printf "9"; print ","$9}' "$1".fixeddates > BUS_REGS.csv
	echo -e "1,ACT\n2,NSW\n3,NT\n4,QLD\n5,SA\n6,TAS\n7,VIC\n8,WA\n9," > BUS_STATES.csv
	echo "completed (in BUS_REGS.csv and BUS_STATES.csv)"
else
	echo "Usage: ./split-tables.sh input_csv_file"
fi
