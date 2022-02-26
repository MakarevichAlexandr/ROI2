#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes CountFollowers.java
jar -cvf countfollowers.jar -C classes .
