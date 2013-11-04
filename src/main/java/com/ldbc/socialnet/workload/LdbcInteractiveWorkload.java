package com.ldbc.socialnet.workload;

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

public class LdbcInteractiveWorkload extends Workload
{

    @Override
    public void onInit( Map<String, String> properties ) throws WorkloadException
    {
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

        Generator<String> firstNameSelectGenerator = generators.discreteGenerator( Arrays.asList( SubstitutionParameters.FIRST_NAMES ) );
        // Generator<String> firstNameSelectGenerator =
        // generatorBuilder.discreteGenerator(
        // Arrays.asList( new String[] { "Chen" } ) ).build();

        int limit = 10;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query1Generator( firstNameSelectGenerator,
                limit ) ) );

        Calendar calendar = Calendar.getInstance();
        calendar.set( 2010, Calendar.JANUARY, 1 );
        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";
        Date startDate = calendar.getTime();
        int durationDays = 365 * 2;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query3Generator( personId, countryX, countryY,
                startDate, durationDays ) ) );

        calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        personId = 143;
        startDate = calendar.getTime();
        durationDays = 300;
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query4Generator( personId, startDate,
                durationDays ) ) );

        calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        personId = 143;
        Date joinDate = calendar.getTime();
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query5Generator( personId, joinDate ) ) );

        personId = 143;
        String tagName = "Charles_Dickens";
        operations.add( Tuple.tuple2( 1d, (Generator<Operation<?>>) new Query6Generator( personId, tagName ) ) );

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
        // operationsToInclude.add( LdbcQuery5.class );
        // operationsToInclude.add( LdbcQuery6.class );
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

        protected Query1Generator( final Generator<String> firstNames, final int limit )
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

        protected Query6Generator( final long personId, final String tagName )
        {
            this.personId = personId;
            this.tagName = tagName;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery6( personId, tagName );
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
