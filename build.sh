#!/bin/bash

LDBC_CORE="ldbc_driver"
LDBC_CORE_VER="0.2-SNAPSHOT"
LDBC_CORE_JAR="ldbc_driver/target/core-"${LDBC_CORE_VER}".jar"

STEPS="steps"
STEPS_VER="0.1-SNAPSHOT"
STEPS_JAR="steps/target/steps-"${STEPS_VER}".jar"

NEO4J_JDBC="neo4j-jdbc"
NEO4J_JDBC_VER="2.0.1-SNAPSHOT"
NEO4J_JDBC_JAR="neo4j-jdbc/target/neo4j-jdbc-"${NEO4J_JDBC_VER}".jar"
# git submodule add git@github.com:neo4j-contrib/neo4j-jdbc.git neo4j-jdbc
# neo4j-jdbc-2.0.1-SNAPSHOT.jar	
# neo4j-jdbc-2.0.1-SNAPSHOT-jar-with-dependencies.jar

IN_PROJECT_MVN_REPO="lib"

git submodule update --init
rm -rf $IN_PROJECT_MVN_REPO

cd $LDBC_CORE
./build.sh
cd ..

cd $STEPS
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
cd ..

cd $NEO4J_JDBC
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7
cd ..

mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$LDBC_CORE_JAR -DgroupId=com.ldbc.driver -DartifactId=core -Dversion=$LDBC_CORE_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$STEPS_JAR -DgroupId=org.neo4j.traversal -DartifactId=steps -Dversion=$STEPS_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$NEO4J_JDBC_JAR -DgroupId=org.neo4j.jdbc -DartifactId=neo4j-jdbc -Dversion=$NEO4J_JDBC_VER

mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7