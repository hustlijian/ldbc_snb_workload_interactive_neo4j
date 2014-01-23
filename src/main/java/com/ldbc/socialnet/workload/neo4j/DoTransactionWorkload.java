package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.ParamsException;
import com.ldbc.driver.WorkloadParams;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import java.io.File;

/* 

MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx256m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded,-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.socialnet.workload.LdbcInteractiveWorkload"
sudo java -agentlib:hprof=cpu=samples,interval=10,depth=5 -server -XX:+UseConcMarkSweepGC -Xmx16G -cp ldbc_driver/core/target/core-0.1-SNAPSHOT.jar:target/neo4j_importer-0.1-SNAPSHOT.jar com.ldbc.driver.Client -oc 10 -rc -1 -tc 1 -s -tu MILLISECONDS -p neo4j.path=/var/neodata/ldbc/neo4j_socialnet_50000_users_10_years_FIX_2_INDEX/neo4jdb/ neo4j.dbtype=embedded -db com.ldbc.socialnet.workload.neo4j.Neo4jDb -w com.ldbc.socialnet.workload.LdbcInteractiveWorkload
sudo mvn -DjvmArgs="-server -XX:+UseConcMarkSweepGC -Xmx16G" exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.socialnet.workload.LdbcInteractiveWorkload,-oc,500,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=/var/neodata/ldbc/neo4j_socialnet_50000_users_10_years_FIX_2_INDEX/neo4jdb/,-p,neo4j.dbtype=embedded"
 */
public class DoTransactionWorkload {

    /*
    "-P,ldbc_socnet_interactive.properties,-oc,10,-p,neo4j.dbtype,embedded-cypher,-rf,report/result-embedded-cypher.json,-tc,1,-tu,MILLISECONDS"
    */
    public static void main(String[] args) throws ClientException, ParamsException {
        System.out.println("Neo4j Configuration:");
        System.out.println(MapUtils.prettyPrint(Config.NEO4J_RUN_CONFIG));

        String paramsFilePath = TestUtils.getResource("/ldbc_socnet_interactive.properties").getAbsolutePath();
        String resultFilePath = new File("report/temporary_result_file.json").getAbsolutePath();

        WorkloadParams params = WorkloadParams.fromArgs(new String[]{
                "-P", paramsFilePath,
                "-p", Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_STEPS,
                "-rf", resultFilePath,
                "-tc", "1"});

        Client client = new Client(params);
        client.start();

        // Clean up
        if (new File(resultFilePath).exists()) {
            new File(resultFilePath).delete();
        }
    }
}
