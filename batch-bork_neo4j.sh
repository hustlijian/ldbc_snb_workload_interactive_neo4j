#!/bin/bash
git clone git@github.com:jakewins/neo4j.git
cd neo4j
git fetch origin
git checkout -b batch-bork origin/batch-bork
mvn clean install -DskipTests
