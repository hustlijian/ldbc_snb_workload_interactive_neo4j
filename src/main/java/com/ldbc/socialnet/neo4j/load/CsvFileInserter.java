package com.ldbc.socialnet.neo4j.load;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;

public class CsvFileInserter
{
    public static final int DEFAULT_BUFFER_SIZE = 100000;

    private final Object[][] csvReadBuffer;
    private final File file;
    private final CsvFileReader csvReader;
    private final CsvLineInserter lineInserter;
    private final int bufferSize;
    // first line == 0
    private final int startLine;

    public CsvFileInserter( File file, CsvLineInserter lineInserter ) throws FileNotFoundException
    {
        this( DEFAULT_BUFFER_SIZE, file, lineInserter, 1 );
    }

    private CsvFileInserter( int bufferSize, File file, CsvLineInserter lineInserter, int startLine )
                                                                                                     throws FileNotFoundException
    {
        super();
        this.bufferSize = bufferSize;
        this.csvReadBuffer = new Object[bufferSize][];
        this.file = file;
        this.csvReader = new CsvFileReader( file );
        this.lineInserter = lineInserter;
        this.startLine = startLine;
        advanceCsvReaderToStartLine();
    }

    public File getFile()
    {
        return file;
    }

    private void advanceCsvReaderToStartLine()
    {
        for ( int i = 0; i < startLine; i++ )
        {
            if ( csvReader.hasNext() )
            {
                csvReader.next();
            }
            else
            {
                return;
            }
        }
    }

    private int bufferLines()
    {
        if ( csvReader.hasNext() == false ) return 0;
        for ( int line = 0; line < bufferSize; line++ )
        {
            if ( csvReader.hasNext() )
            {
                csvReadBuffer[line] = csvReader.next();
            }
            else
            {
                return line;
            }
        }
        return bufferSize;
    }

    public int insertAllBuffered() throws FileNotFoundException
    {
        int count = 0;
        int bufferedLines = bufferLines();
        while ( bufferedLines > 0 )
        {
            for ( int line = 0; line < bufferedLines; line++ )
            {
                csvReadBuffer[line] = lineInserter.transform( csvReadBuffer[line] );
            }
            if ( false == ( lineInserter.sortComparator() == null ) )
            {
                Arrays.sort( csvReadBuffer, lineInserter.sortComparator() );
            }
            for ( int line = 0; line < bufferedLines; line++ )
            {
                lineInserter.insert( csvReadBuffer[line] );
                count++;
            }
            bufferedLines = bufferLines();
        }
        return count;
    }

    public int insertAll() throws FileNotFoundException
    {
        int count = 0;
        while ( csvReader.hasNext() )
        {
            lineInserter.insert( lineInserter.transform( csvReader.next() ) );
            count++;
        }
        return count;
    }
}
