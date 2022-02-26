#!/bin/sh

hadoop jar ./countdocs.jar pdccourse.hw3.CountDocs -D mapreduce.job.reduces=1 \
	./inverted-index/eewiki.xml ./inverted-index/count-docs-xml/output

