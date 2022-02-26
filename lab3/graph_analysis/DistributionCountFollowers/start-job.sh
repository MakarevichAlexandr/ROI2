#!/bin/sh

hadoop jar ./distributioncountfollowers.jar mapred.graphanalysis.DistributionCountFollowers -D mapreduce.job.reduces=4 \
	-D mapreduce.input.keyvaluelinerecordreader.key.value.separator=" " -D mapreduce.output.textoutputformat.separator=" " \
		graph-analysis/count-followers graph-analysis/distribution-count-followers

