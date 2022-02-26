#!/bin/sh

hadoop jar ./bigramcount.jar mapred.bigram.BigramCount -D mapreduce.job.reduces=4 ./bigram/wp.txt bigram/bg-ru ./skip.en



    
    
