#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes BigramCount.java
jar -cvf bigramcount.jar -C classes .
