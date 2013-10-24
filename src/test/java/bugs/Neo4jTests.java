package bugs;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@Ignore
public class Neo4jTests
{
    public enum Labels implements Label
    {
        Type1,
        Type2
    }

    @Test
    public void batchInserterGetNodePropertiesOnValidNodeShouldNotCausePropertyRecordNotInUse() throws IOException
    {
        String propertyKey = "languages";
        String dbDir = "tempDb/";
        FileUtils.deleteRecursively( new File( dbDir ) );
        BatchInserter batchInserter = BatchInserters.inserter( dbDir );
        Map<String, Object> properties = null;

        for ( int i = 0; i < 10000; i++ )
        {
            properties = new HashMap<String, Object>();
            properties.put( "id", 1l );
            properties.put( "firstName", "first" );
            properties.put( "lastName", "last" );
            properties.put( "gender", "female" );
            properties.put( "birthday", 1l );
            properties.put( "creationDate", 1l );
            properties.put( "ip", "1.1.1.1" );
            properties.put( "browser", "chrome" );
            properties.put( "emails", new String[0] );
            properties.put( propertyKey, new String[0] );
            long id = batchInserter.createNode( properties, Labels.Type1 );
            for ( int j = 0; j < 10; j++ )
            {
                properties = batchInserter.getNodeProperties( id );
                String[] oldArray = (String[]) properties.get( propertyKey );
                String[] newArray = Utils.copyArrayAndAddElement( oldArray, "new" + j );
                batchInserter.setNodeProperty( id, propertyKey, newArray );
            }
        }
        for ( int i = 0; i < 10000; i++ )
        {
            properties = new HashMap<String, Object>();
            properties.put( propertyKey, new String[0] );
            long id = batchInserter.createNode( properties, Labels.Type2 );
            for ( int j = 0; j < 10; j++ )
            {
                properties = batchInserter.getNodeProperties( id );
                String[] oldArray = (String[]) properties.get( propertyKey );
                String[] newArray = Utils.copyArrayAndAddElement( oldArray, "new" + j );
                batchInserter.setNodeProperty( id, propertyKey, newArray );
            }
        }
        for ( int i = 0; i < 10000; i++ )
        {
            properties = new HashMap<String, Object>();
            properties.put( "id", 1l );
            properties.put( "firstName", "first" );
            properties.put( "lastName", "last" );
            properties.put( "gender", "female" );
            properties.put( "birthday", 1l );
            properties.put( "creationDate", 1l );
            properties.put( "ip", "1.1.1.1" );
            properties.put( "browser", "chrome" );
            properties.put( "emails", new String[0] );
            properties.put( propertyKey, new String[0] );
            long id = batchInserter.createNode( properties, Labels.Type1 );
            for ( int j = 0; j < 10; j++ )
            {
                properties = batchInserter.getNodeProperties( id );
                String[] oldArray = (String[]) properties.get( propertyKey );
                String[] newArray = Utils.copyArrayAndAddElement( oldArray, "new" + j );
                batchInserter.setNodeProperty( id, propertyKey, newArray );
            }
        }
        for ( int i = 0; i < 10000; i++ )
        {
            properties = new HashMap<String, Object>();
            properties.put( propertyKey, new String[0] );
            long id = batchInserter.createNode( properties, Labels.Type2 );
            for ( int j = 0; j < 10; j++ )
            {
                properties = batchInserter.getNodeProperties( id );
                String[] oldArray = (String[]) properties.get( propertyKey );
                String[] newArray = Utils.copyArrayAndAddElement( oldArray, "new" + j );
                batchInserter.setNodeProperty( id, propertyKey, newArray );
            }
        }

        batchInserter.shutdown();
    }

    @Test
    public void getPropertyKeysShouldNotReturnDuplicates() throws IOException
    {
        // Some will pass, some will fail, it's weird
        String[] changingKeyValues = new String[] { "1", "12", "123", "1234", "12345", "123456", "1234567", "12345678",
                "123456789", "1234567890" };

        for ( String changingKey : changingKeyValues )
        {
            assertGetPropertyKeysDoesNotReturnDuplicates( changingKey );
        }

    }

    void assertGetPropertyKeysDoesNotReturnDuplicates( String changingKey ) throws IOException
    {
        // Given

        String dbDir = "tempDb/";
        // This key appears twice in getPropertyKeys()
        String duplicateKey = "duplicate";
        FileUtils.deleteRecursively( new File( dbDir ) );
        BatchInserter batchInserter = BatchInserters.inserter( dbDir );
        Map<String, Object> properties = null;

        // When

        properties = new HashMap<String, Object>();
        properties.put( changingKey, true );
        properties.put( duplicateKey, new String[0] );
        batchInserter.createNode( properties );

        properties = new HashMap<String, Object>();
        properties.put( duplicateKey, new String[0] );
        long node1 = batchInserter.createNode( properties );

        batchInserter.setNodeProperty( node1, duplicateKey, new String[] { "new" } );

        batchInserter.shutdown();

        // Then

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).newGraphDatabase();
        try (Transaction tx = db.beginTx())
        {
            assertThat( "failed with changingKey=" + changingKey,
                    duplicateKeys( db.getNodeById( node1 ).getPropertyKeys() ), equalTo( new HashSet<String>() ) );
            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    HashSet<String> duplicateKeys( Iterable<String> keys )
    {
        HashSet<String> uniqueKeys = new HashSet<String>();
        HashSet<String> duplicateKeys = new HashSet<String>();
        for ( String key : keys )
            if ( false == uniqueKeys.add( key ) ) duplicateKeys.add( key );
        return duplicateKeys;
    }
}
