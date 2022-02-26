#!/bin/sh

hadoop jar ./countfollowers.jar mapred.graphanalysis.CountFollowers -D mapreduce.job.reduces=4 \
	-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=" " -D mapreduce.output.textoutputformat.separator=" " \
		./followers.db graph-analysis/count-followers graph-analysis/distribution-count-followers

