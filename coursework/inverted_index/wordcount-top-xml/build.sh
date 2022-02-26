#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes WordCountTop.java XmlInputFormat.java
jar -cvf wordcounttop.jar -C classes .
