#!/bin/sh

hadoop jar ./wordcounttop.jar pdccourse.hw3.WordCountTop -D mapreduce.job.reduces=1 \
	./inverted-index/wordcount-xml/output ./inverted-index/wordcount-top-xml/output

