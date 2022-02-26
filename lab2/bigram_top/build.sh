#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes BigramTop.java
jar -cvf bigramtop.jar -C classes .
