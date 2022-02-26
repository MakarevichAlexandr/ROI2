#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes AvgCountFollowers.java
jar -cvf avgcountfollowers.jar -C classes .
