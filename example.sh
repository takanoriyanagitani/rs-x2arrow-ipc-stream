#!/bin/sh

echo example 1 using arrow-cat
./x2arrow-ipc-stream \
	--input ./sample.xlsx \
	--sheet Sheet1 |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./x2arrow-ipc-stream \
	--input ./sample.xlsx \
	--sheet Sheet1 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'rows' \
	--sql "
		SELECT
			*
		FROM rows
	" |
	rs-arrow-ipc-stream-cat
