Neo4j Data Importer & Workload Runner for LDBC Social Network Benchmark
---------------------

This is a reference implementation of the LDBC Social Network benchmark, for the [Neo4j](http://www.neo4j.org/) graph database.
It contains code for the following:

1. Importing datasets generated by the [LDBC Social Network Data Generator](https://github.com/ldbc/ldbc_socialnet_bm/tree/master/ldbc_socialnet_dbgen) into Neo4j
2. Running workloads from the [LDBC Social Network Workload Definition](https://github.com/ldbc/ldbc_socialnet_bm/tree/master/ldbc_socialnet_qgen) against Neo4j

**Build**

Initial build:

	https://github.com/ldbc/ldbc_socialnet_bm_neo4j.git
	cd ldbc_socialnet_bm_neo4j
	./build.sh

Subsequent builds:

	mvn clean compile -Dmaven.compiler.source=1.7 -Dmaven.compiler.target=1.7

**Import data into Neo4j**

	mvn exec:java -Dexec.mainClass=com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter
	
The resulting Neo4j instance will have [this schema](https://github.com/ldbc/ldbc_socialnet_bm_neo4j/wiki/Schema)

**Run Workload using [ldbc_driver](https://github.com/alexaverbuch/ldbc_driver)**

	usage: java -cp classesYouNeedOnTheClassPath.jar com.ldbc.driver.Client [-db <classname>] [-l | -t] [-oc <count>] [-P
	       <file1:file2>] [-p <key=value>] [-rc <count>] [-s]  [-tc <count>] [-tu <unit>] [-w <classname>]
	   -db,--database <classname>       classname of the DB to use (e.g. com.ldbc.driver.db.basic.BasicDb)
	   -l,--load                        run the loading phase of the workload
	   -oc,--operationcount <count>     number of operations to execute (default: 0)
	   -P <file1:file2>                 load properties from file(s) - files will be loaded in the order provided
	   -p <key=value>                   properties to be passed to DB and Workload - these will override
		                            properties loaded from files
	   -rc,--recordcount <count>        number of records to create during load phase (default: 0)
	   -s,--status                      show status during run
	   -t,--transaction                 run the transactions phase of the workload
	   -tc,--threadcount <count>        number of worker threads to execute with (default: 2)
	   -tu,--timeunit <unit>            time unit to use when gathering metrics. default:MILLISECONDS,
		                            valid:[NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES]
	   -w,--workload <classname>        classname of the Workload to use (e.g.
		                            com.ldbc.driver.workloads.simple.SimpleWorkload)

**Run Example Using Maven**

	mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client 
	-Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.socialnet.workload.LdbcInteractiveWorkload,
	-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded"
	
The executed workload consists of [these queries](https://github.com/ldbc/ldbc_socialnet_bm_neo4j/wiki/Queries)

**Configuration**

	# Directory where CSV data can be found (from LDBC Data Generator)
	data_dir=/path/to/ldbc/generator/social/network/csv/files/

	# Directory to store Neo4j database instance
	db_dir=db

	# neo4j properties file for configuring the batch importer
	neo4j_import_config=/neo4j_import_dev.properties

	# neo4j properties file for configuring the transactional database
	neo4j_run_config=/neo4j_run_dev.properties

**Other**

Using the LDBC Social Network data generator:

* [Instructions on installing and running](https://github.com/ldbc/ldbc_socialnet_bm/blob/master/ldbc_socialnet_dbgen/README.md)
* [Explanation of format, content, and distributions, and schema of generated data](todo) **TODO**
