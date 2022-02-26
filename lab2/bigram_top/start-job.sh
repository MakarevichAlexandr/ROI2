#!/bin/sh

hadoop jar ./bigramtop.jar mapred.bigram.BigramTop -D mapreduce.job.reduces=1 \
	-D mapreduce.output.textoutputformat.separator="=" ./bigram/bg-ru ./bigram/bg-top 2 20





