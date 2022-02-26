#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes BuildInvertedIndex.java XmlInputFormat.java
jar -cvf buildinvertedindex.jar -C classes .
