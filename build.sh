#!/bin/bash

LDBC_CORE="ldbc_driver"
LDBC_CORE_VER="0.11-SNAPSHOT"
LDBC_CORE_JAR="ldbc_driver/core/target/core-"${LDBC_CORE_VER}".jar"

STEPS="steps"
STEPS_VER="0.1-SNAPSHOT"
STEPS_JAR="steps/target/steps-"${STEPS_VER}".jar"

IN_PROJECT_MVN_REPO="lib"

git submodule update --init
rm -rf $IN_PROJECT_MVN_REPO
cd $LDBC_CORE
./build.sh
cd ..

mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$LDBC_CORE_JAR -DgroupId=com.ldbc.driver -DartifactId=core -Dversion=$LDBC_CORE_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$STEPS_JAR -DgroupId=com.neo4j.traversal -DartifactId=steps -Dversion=$STEPS_VER

mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
