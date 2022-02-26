#!/bin/sh

hadoop jar ./avgcountfollowers.jar mapred.graphanalysis.AvgCountFollowers -D mapreduce.job.reduces=1 \
	-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=" " -D mapreduce.output.textoutputformat.separator="=" \
		graph-analysis/count-followers graph-analysis/avg-count-followers

