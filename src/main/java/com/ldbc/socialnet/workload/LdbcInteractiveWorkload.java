package com.ldbc.socialnet.workload;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.wrapper.FilterGeneratorWrapper;
import com.ldbc.driver.generator.wrapper.StartTimeOperationGeneratorWrapper;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.driver.util.temporal.Duration;
import com.ldbc.driver.util.temporal.Time;

/*
MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.driver.Client -Dexec.arguments="-db,com.ldbc.socialnet.workload.neo4j.Neo4jDb,-w,com.ldbc.socialnet.workload.LdbcInteractiveWorkload,-oc,10,-rc,-1,-tc,1,-s,-tu,MILLISECONDS,-p,neo4j.path=db/,-p,neo4j.dbtype=embedded-api-steps"

    com.ldbc.socialnet.workload.LdbcQuery3
        Count:          1
        Min:            18019
        Mean:           18019.0
    com.ldbc.socialnet.workload.LdbcQuery4
        Count:          2
        Min:            353
        Mean:           1077.0
    com.ldbc.socialnet.workload.LdbcQuery5
        Count:          5
        Min:            46506
        Mean:           51906.6
    com.ldbc.socialnet.workload.LdbcQuery1
        Count:          2
        Min:            22
        Mean:           310.5

 */
public class LdbcInteractiveWorkload extends Workload
{
    private SubstitutionParameters substitutionParameters = null;

    @Override
    public void onInit( Map<String, String> properties ) throws WorkloadException
    {
        // TODO from config
        String parametersFilename = "parameters.json";
        if ( false == new File( parametersFilename ).exists() )
        {
            throw new WorkloadException( "Substitution parameters file does not exist: " + parametersFilename );
        }
        File parametersFile = new File( parametersFilename );
        try
        {
            substitutionParameters = SubstitutionParameters.fromJson( parametersFile );
        }
        catch ( Exception e )
        {
            throw new WorkloadException( "Unable to load substitution parameters from " + parametersFile.getName() );
        }
    }

    @Override
    protected void onCleanup() throws WorkloadException
    {
    }

    @Override
    protected Generator<Operation<?>> createLoadOperations( GeneratorFactory generatorBuilder )
            throws WorkloadException
    {
        throw new UnsupportedOperationException( "Load phase not implemented for LDBC workload" );
    }

    @Override
    protected Generator<Operation<?>> createTransactionalOperations( GeneratorFactory generators )
            throws WorkloadException
    {

        /*
         * Create Generators for desired Operations
         */

        Set<Tuple2<Double, Generator<Operation<?>>>> operations = new HashSet<Tuple2<Double, Generator<Operation<?>>>>();

        Generator<String> firstNameGenerator = generators.discreteGenerator( substitutionParameters.firstNames );
        Generator<Long> personIdGenerator = generators.discreteGenerator( substitutionParameters.personIds );
        Generator<Long> postCreationDateGenerator00_66 = generators.uniformNumberGenerator(
                substitutionParameters.postCreationDates.get( 0 ), substitutionParameters.postCreationDates.get( 66 ) );
        Generator<Long> postCreationDateGenerator33_66 = generators.uniformNumberGenerator(
                substitutionParameters.postCreationDates.get( 33 ), substitutionParameters.postCreationDates.get( 66 ) );

        /*
         * Query1
         *  - Select uniformly randomly from person first names
         */
        int query1Limit = 10;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query1Generator( firstNameGenerator,
                query1Limit ) ) );

        /*
         * Query2
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 33perc-66perc of entire date range
         */
        int query2Limit = 20;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query2Generator( personIdGenerator,
                postCreationDateGenerator33_66, query2Limit ) ) );

        /*
         * Query3
         *  - Person ID - select uniformly randomly from person ids
         *  - Post Creation Date - select uniformly randomly a post creation date from between 0perc-66perc of entire date range
         *  - Duration - a number of days (33% of the length of post creation date range)
         *  - Country1 - the first of country pair (file: countryPairs.txt)
         *  - Country2 - the second of country pair (file: countryPairs.txt)
         */
        Long totalDuration = substitutionParameters.postCreationDates.get( 100 )
                             - substitutionParameters.postCreationDates.get( 0 );

        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set( 2010, Calendar.JANUARY, 1 );
        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";
        Date startDate = calendar.getTime();
        int durationDays = 365 * 2;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query3Generator( personId, countryX, countryY,
                startDate, durationDays ) ) );

        /*
         * Query4
         */
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        personId = 143;
        startDate = calendar.getTime();
        durationDays = 300;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query4Generator( personId, startDate,
                durationDays ) ) );

        /*
         * Query5
         */
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        personId = 143;
        Date joinDate = calendar.getTime();
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query5Generator( personId, joinDate ) ) );

        /*
         * Query6
         */
        personId = 143;
        String tagName = "Charles_Dickens";
        int query6Limit = 10;
        operations.add( Tuple.tuple2( 1d,
                (Generator<Operation<?>>) new Query6Generator( personId, tagName, query6Limit ) ) );

        // TODO Query7

        /*
         * Create Discrete Generator from 
         */

        Generator<Operation<?>> operationGenerator = generators.weightedDiscreteDereferencingGenerator( operations );

        /*
         * Filter Interesting Operations
         */

        List<Class<? extends Operation<?>>> operationsToInclude = new ArrayList<Class<? extends Operation<?>>>();
        operationsToInclude.add( LdbcQuery1.class );
        operationsToInclude.add( LdbcQuery3.class );
        operationsToInclude.add( LdbcQuery4.class );
        operationsToInclude.add( LdbcQuery5.class );
        operationsToInclude.add( LdbcQuery6.class );
        Function1<Operation<?>, Boolean> filter = new IncludeOnlyClassesPredicate<Operation<?>>( operationsToInclude );

        Generator<Operation<?>> filteredGenerator = new FilterGeneratorWrapper<Operation<?>>( operationGenerator,
                filter );

        // Generator<Time> startTimeGenerator =
        // GeneratorUtils.randomTimeGeneratorFromNow( generatorBuilder,
        // Time.now(),
        // Time.fromMilli( 100 ).asMilli(), Time.fromMilli( 1000 ).asMilli() );

        Generator<Time> startTimeGenerator = GeneratorUtils.constantTimeGeneratorFromNow( generators, Time.now(),
                Duration.fromMilli( 100 ) );

        return new StartTimeOperationGeneratorWrapper( startTimeGenerator, filteredGenerator );
    }

    class Query1Generator extends Generator<Operation<?>>
    {
        private final Generator<String> firstNames;
        private final int limit;

        protected Query1Generator( Generator<String> firstNames, int limit )
        {
            this.firstNames = firstNames;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery1( firstNames.next(), limit );
        }
    }

    class Query2Generator extends Generator<Operation<?>>
    {
        private final Generator<Long> personIds;
        private final Generator<Long> postCreationDates;
        private final int limit;

        protected Query2Generator( Generator<Long> personIds, Generator<Long> postCreationDates, int limit )
        {
            this.personIds = personIds;
            this.postCreationDates = postCreationDates;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery2( personIds.next(), new Date( postCreationDates.next() ), limit );
        }
    }

    class Query3Generator extends Generator<Operation<?>>
    {
        private final long personId;
        private final String countryY;
        private final String countryX;
        private final Date startDate;
        private final int durationDays;

        protected Query3Generator( final long personId, final String countryX, final String countryY,
                final Date startDate, final int durationDays )
        {
            this.personId = personId;
            this.countryY = countryY;
            this.countryX = countryX;
            this.startDate = startDate;
            this.durationDays = durationDays;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery3( personId, countryX, countryY, startDate, durationDays );
        }
    }

    class Query4Generator extends Generator<Operation<?>>
    {
        private final long personId;
        private final Date startDate;
        private final int durationDays;

        protected Query4Generator( final long personId, final Date startDate, final int durationDays )
        {
            this.personId = personId;
            this.startDate = startDate;
            this.durationDays = durationDays;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery4( personId, startDate, durationDays );
        }
    }

    class Query5Generator extends Generator<Operation<?>>
    {
        private final long personId;
        private final Date joinDate;

        protected Query5Generator( final long personId, final Date joinDate )
        {
            this.personId = personId;
            this.joinDate = joinDate;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery5( personId, joinDate );
        }
    }

    class Query6Generator extends Generator<Operation<?>>
    {
        private final long personId;
        private final String tagName;
        private final int limit;

        protected Query6Generator( long personId, String tagName, int limit )
        {
            this.personId = personId;
            this.tagName = tagName;
            this.limit = limit;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery6( personId, tagName, limit );
        }
    }

    class IncludeOnlyClassesPredicate<T> implements Function1<T, Boolean>
    {
        private final Set<Class<? extends Operation<?>>> includedItems;

        public IncludeOnlyClassesPredicate( Class<? extends Operation<?>>... includedItems )
        {
            this( Arrays.asList( includedItems ) );
        }

        public IncludeOnlyClassesPredicate( List<Class<? extends Operation<?>>> includedItems )
        {
            this.includedItems = new HashSet<Class<? extends Operation<?>>>( includedItems );
        }

        @Override
        public Boolean apply( T input )
        {
            return true == includedItems.contains( input.getClass() );
        }
    }
}
