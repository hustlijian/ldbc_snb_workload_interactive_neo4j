#!/bin/bash

LDBC_CORE="ldbc_driver"
LDBC_CORE_VER="0.2-SNAPSHOT"
LDBC_CORE_JAR="ldbc_driver/target/jeeves-"${LDBC_CORE_VER}".jar"

STEPS="steps"
STEPS_VER="0.2-SNAPSHOT"
STEPS_JAR="steps/target/steps-"${STEPS_VER}".jar"

JDBC="neo4j-jdbc"
JDBC_VER="2.1.4-SNAPSHOT"
JDBC_JAR="neo4j-jdbc/target/neo4j-jdbc-"${JDBC_VER}".jar"

IN_PROJECT_MVN_REPO="lib"

git submodule update --init
rm -rf $IN_PROJECT_MVN_REPO

cd $LDBC_CORE
./build.sh
cd ..

cd $STEPS
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
cd ..

cd $JDBC
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
cd ..

mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$LDBC_CORE_JAR -DgroupId=com.ldbc.driver -DartifactId=core -Dversion=$LDBC_CORE_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$STEPS_JAR -DgroupId=org.neo4j.traversal -DartifactId=steps -Dversion=$STEPS_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$JDBC_JAR -DgroupId=org.neo4j.jdbc -DartifactId=neo4j-jdbc -Dversion=$JDBC_VER

mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7