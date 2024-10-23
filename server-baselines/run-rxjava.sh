#!/bin/bash
export JAVA_HOME=$(dirname $(dirname $(readlink $(which java))))
java --version
exec scala-cli run rxjava.scala -J -Xmx4G
