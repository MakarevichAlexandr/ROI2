#!/bin/sh

hadoop jar ./wordcount.jar pdccourse.hw3.WordCount -D mapreduce.job.reduces=4 \
	./inverted-index/eewiki.xml ./inverted-index/wordcount-xml/output

