#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes CountDocs.java XmlInputFormat.java
jar -cvf countdocs.jar -C classes .
