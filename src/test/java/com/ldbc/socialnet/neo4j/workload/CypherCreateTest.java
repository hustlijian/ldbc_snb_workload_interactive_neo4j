package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class CypherCreateTest
{
    public static String dbDir = "tempDb";
    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    @Before
    public void initTestDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        queryEngine = new ExecutionEngine( db );
    }

    @After
    public void cleanupTestTb() throws IOException
    {
        db.shutdown();
    }

    @Test
    public void shouldCreate1()
    {
        String createStatement = "CREATE (n1:SomeLabel {name:'n1'}), (n2 {name:'n2'}), (n3 {name:'n3'}),\n"

        + " (n3)<-[:N2_TO_N3]-(n2)<-[:N1_TO_N2]-(n1)";

        createAndAssertCorrectNumberOfElements( createStatement );

        assertN1ConnectedToN2ViaN1_TO_N2Relationship();
    }

    @Test
    public void shouldCreate2()
    {
        String createStatement = "CREATE (n1:SomeLabel {name:'n1'})-[:N1_TO_N2]->(n2 {name:'n2'})-[:N2_TO_N3]->(n3 {name:'n3'})";

        createAndAssertCorrectNumberOfElements( createStatement );

        assertN1ConnectedToN2ViaN1_TO_N2Relationship();
    }

    @Test
    public void shouldCreate3()
    {
        String createStatement = "CREATE (n1:SomeLabel {name:'n1'}), (n2 {name:'n2'}), (n3 {name:'n3'})\n"

        + "WITH n1, n2, n3\n"

        + "CREATE (n3)<-[:N2_TO_N3]-(n2)<-[:N1_TO_N2]-(n1)";

        createAndAssertCorrectNumberOfElements( createStatement );

        assertN1ConnectedToN2ViaN1_TO_N2Relationship();
    }

    private void createAndAssertCorrectNumberOfElements( String cypher )
    {
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            ExecutionResult result = queryEngine.execute( cypher, MapUtil.map() );
            assertThat( result.getQueryStatistics().getNodesCreated(), is( 3 ) );
            assertThat( result.getQueryStatistics().getRelationshipsCreated(), is( 2 ) );
            assertThat( result.getQueryStatistics().getPropertiesSet(), is( 3 ) );
            assertThat( result.getQueryStatistics().getLabelsAdded(), is( 1 ) );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    private void assertN1ConnectedToN2ViaN1_TO_N2Relationship()
    {
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            String queryString = "MATCH (n1:SomeLabel)\n"

            + "WITH n1\n"

            + "MATCH (n1)-[r]->(nx)\n"

            + "RETURN n1, r, nx";

            ExecutionResult result = queryEngine.execute( queryString );
            // System.out.println( result.dumpToString() );
            Map<String, Object> resultMap = result.iterator().next();
            Node n1 = (Node) resultMap.get( "n1" );
            Relationship r = (Relationship) resultMap.get( "r" );
            Node nx = (Node) resultMap.get( "nx" );

            assertThat( (String) n1.getProperty( "name" ), is( "n1" ) );
            assertThat( r.getStartNode(), is( n1 ) );
            assertThat( r.getEndNode(), is( nx ) );
            assertThat( (String) nx.getProperty( "name" ), is( "n2" ) );
            assertThat( r.getType().name(), is( "N1_TO_N2" ) );

            tx.success();
        }
        catch ( Exception e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }
}
