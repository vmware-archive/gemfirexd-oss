#!/bin/sh
export CLASSPATH=/s2qa/tangc/workspace/sqlBuild/product-gfxd/lib/gemfirexd.jar
cur_dir=`pwd`
cd oraconverter
javac *.java
jar -cf dataconverters.jar *.class
cd $cur_dir