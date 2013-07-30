package com.ldbc.socialnet.neo4j;

import java.io.File;
import java.io.FileNotFoundException;

public class CsvFileInserter
{
    public static final int DEFAULT_BUFFER_SIZE = 100000;

    private final Object[][] csvReadBuffer;
    private final File file;
    private final CsvFileReader csvReader;
    private final CsvLineInserter lineInserter;
    private final int bufferSize;

    public CsvFileInserter( File file, CsvLineInserter lineInserter ) throws FileNotFoundException
    {
        this( DEFAULT_BUFFER_SIZE, file, lineInserter );
    }

    private CsvFileInserter( int bufferSize, File file, CsvLineInserter lineInserter ) throws FileNotFoundException
    {
        super();
        this.bufferSize = bufferSize;
        this.csvReadBuffer = new Object[bufferSize][];
        this.file = file;
        this.csvReader = new CsvFileReader( file );
        this.lineInserter = lineInserter;
    }

    public File getFile()
    {
        return file;
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
