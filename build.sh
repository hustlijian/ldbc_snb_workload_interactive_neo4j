#!/bin/bash
LDBC_DRIVER_DIR="ldbc_driver"
LDBC_DRIVER_ARTIFACT="jeeves"
LDBC_DRIVER_VER="0.2-SNAPSHOT"
LDBC_DRIVER_JAR=${LDBC_DRIVER_DIR}"/target/"${LDBC_DRIVER_ARTIFACT}"-"${LDBC_DRIVER_VER}".jar"

STEPS_DIR="steps"
STEPS_ARTIFACT="steps"
STEPS_VER="0.2-SNAPSHOT"
STEPS_JAR=${STEPS_DIR}"/target/"${STEPS_ARTIFACT}"-"${STEPS_VER}".jar"

JDBC_DIR="neo4j-jdbc"
JDBC_ARTIFACT="neo4j-jdbc"
JDBC_VER="2.1.4"
JDBC_JAR=${JDBC_DIR}"/target/"${JDBC_ARTIFACT}"-"${JDBC_VER}".jar"

IN_PROJECT_MVN_REPO="lib"

git submodule update --init
rm -rf $IN_PROJECT_MVN_REPO

cd $LDBC_DRIVER_DIR
./build.sh
cd ..

cd $STEPS_DIR
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7 -DskipTests
cd ..

cd $JDBC_DIR
mvn clean package -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7 -DskipTests
cd ..

mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$LDBC_DRIVER_JAR -DgroupId=com.ldbc.driver -DartifactId=$LDBC_DRIVER_ARTIFACT -Dversion=$LDBC_DRIVER_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$STEPS_JAR -DgroupId=org.neo4j.traversal -DartifactId=$STEPS_ARTIFACT -Dversion=$STEPS_VER
mvn install:install-file -DlocalRepositoryPath=$IN_PROJECT_MVN_REPO -DcreateChecksum=true -Dpackaging=jar -Dfile=$JDBC_JAR -DgroupId=org.neo4j.jdbc -DartifactId=$JDBC_ARTIFACT -Dversion=$JDBC_VER

mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7