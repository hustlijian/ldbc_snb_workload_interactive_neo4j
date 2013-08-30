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

public class DoTransactionWorkload
{
    public static void main( String[] args ) throws ClientException
    {
        long operationCount = 20;
        long recordCount = -1;
        int threadCount = WorkloadParams.calculateDefaultThreadPoolSize();
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
