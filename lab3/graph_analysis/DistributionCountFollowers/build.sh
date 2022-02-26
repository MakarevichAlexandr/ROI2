#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes DistributionCountFollowers.java
jar -cvf distributioncountfollowers.jar -C classes .
