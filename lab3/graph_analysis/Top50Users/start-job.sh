#!/bin/sh

hadoop jar ./top50users.jar mapred.graphanalysis.Top50Users -D mapreduce.job.reduces=1 \
	-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=" " -D mapreduce.output.textoutputformat.separator=" " \
		graph-analysis/count-followers graph-analysis/top-50-users

