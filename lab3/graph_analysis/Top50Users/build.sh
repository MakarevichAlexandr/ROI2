#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes Top50Users.java
jar -cvf top50users.jar -C classes .
