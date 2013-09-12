package com.ldbc.socialnet.workload.neo4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ldbc.driver.BenchmarkPhase;
import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.WorkloadParams;
import com.ldbc.socialnet.workload.LdbcInteractiveWorkload;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

/*
 sudo java -server -XX:+UseConcMarkSweepGC -Xmx16G -cp ldbc_driver/core/target/core-0.1-SNAPSHOT.jar:target/neo4j_importer-0.1-SNAPSHOT.jar com.ldbc.driver.Client -oc 10 -rc -1 -tc 1 -s -tu MILLISECONDS -p neo4j.path=/var/neodata/ldbc/neo4j_socialnet_50000_users_10_years_FIX_2_INDEX/neo4jdb/ neo4j.dbtype=embedded -db com.ldbc.socialnet.workload.neo4j.Neo4jDb -w com.ldbc.socialnet.workload.LdbcInteractiveWorkload
 */
public class DoTransactionWorkload
{
    public static void main( String[] args ) throws ClientException
    {
        long operationCount = 20;
        long recordCount = -1;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        Map<String, String> userParams = new HashMap<String, String>();
        userParams.put( "neo4j.path", Config.DB_DIR );
        userParams.put( "neo4j.dbtype", "embedded" );
        WorkloadParams params = new WorkloadParams( userParams, Neo4jDb.class.getName(),
                LdbcInteractiveWorkload.class.getName(), operationCount, recordCount, BenchmarkPhase.TRANSACTION_PHASE,
                threadCount, showStatus, timeUnit );

        Client client = new Client();
        client.start( params );
    }
}
