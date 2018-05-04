#!/bin/bash

if [ "$1" != '' ]
then
	tail -n +2 "$1" | split -a 1 -d -l 500000 --additional-suffix=.tsv - BUS_DATA_
	for d in BUS_DATA_*
	do
		head -n 1 "$1" | awk -v FS='\t' '{print $1"\t"$2"\t"$3"\tDate."$4"\tDate."$5"\tDate."$6"\t"$7"\t"$8"\t"$9;}' > tmp
		cat $d >> tmp
	   	mv -f tmp $d
	done
else
	echo "Usage: ./add_header_and_split.sh input_csv_file"
fi
