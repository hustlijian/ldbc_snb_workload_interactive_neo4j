package bugs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.workload.neo4j.utils.Utils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@Ignore
public class BugTests
{
    enum Things implements Label
    {
        Label1,
        Label2
    }

    @Test
    public void batchInserterShouldAllowMultipleEmptyArrayProperties() throws IOException
    {
        String dbDir = "tempDb/";
        FileUtils.deleteRecursively( new File( dbDir ) );

        BatchInserter batchInserter = BatchInserters.inserter( dbDir );
        Map<String, Object> properties = null;
        String[] old = null;

        /*
         * 1            false
         * 12           pass
         * 123          pass
         * 1234         fail
         * 12345        pass
         * 123456       pass
         * 1234567      pass
         * 12345678     fail
         * 123456789    pass
         * 1234567890   fail
         */
        properties = new HashMap<String, Object>();
        properties.put( "123456789", true );
        properties.put( "duplicate", new String[0] );
        long node0 = batchInserter.createNode( properties, Things.Label1 );

        properties = new HashMap<String, Object>();
        properties.put( "1", 1L );
        properties.put( "duplicate", new String[0] );
        long node1 = batchInserter.createNode( properties, Things.Label2 );

        properties = batchInserter.getNodeProperties( node0 );
        old = (String[]) properties.get( "duplicate" );
        batchInserter.setNodeProperty( node0, "duplicate", Utils.copyArrayAndAddElement( old, "new" ) );
        properties = batchInserter.getNodeProperties( node0 );

        properties = batchInserter.getNodeProperties( node1 );
        old = (String[]) properties.get( "duplicate" );
        batchInserter.setNodeProperty( node1, "duplicate", Utils.copyArrayAndAddElement( old, "new" ) );
        properties = batchInserter.getNodeProperties( node1 );

        batchInserter.shutdown();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).newGraphDatabase();
        try (Transaction tx = db.beginTx())
        {
            assertThat( keysAreUnique( db.getNodeById( node0 ).getPropertyKeys() ), is( true ) );
            assertThat( keysAreUnique( db.getNodeById( node1 ).getPropertyKeys() ), is( true ) );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        db.shutdown();
    }

    boolean keysAreUnique( Iterable<String> keys )
    {
        Set<String> uniqueKeys = new HashSet<String>();
        for ( String key : keys )
            if ( false == uniqueKeys.add( key ) ) return false;
        return true;
    }
}
