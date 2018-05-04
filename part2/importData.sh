#!/bin/bash
for d in BUS_DATA_*
do
	mongoimport -d assignment1 -c business --type tsv --file $d --headerline
done
