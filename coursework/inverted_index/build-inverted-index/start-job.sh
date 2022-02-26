#!/bin/sh

hadoop jar ./buildinvertedindex.jar pdccourse.hw3.BuildInvertedIndex -D mapreduce.job.reduces=4 \
	./inverted-index/eewiki.xml ./inverted-index/build-inverted-index/output ./part-r-00000 15

