package com.ldbc.socialnet.workload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.generator.GeneratorException;
import com.ldbc.driver.generator.wrapper.FilterGeneratorWrapper;
import com.ldbc.driver.generator.wrapper.StartTimeOperationGeneratorWrapper;
import com.ldbc.driver.util.GeneratorUtils;
import com.ldbc.driver.util.Pair;
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
    protected Generator<Operation<?>> createLoadOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        throw new UnsupportedOperationException( "Load phase not implemented for LDBC workload" );
    }

    @Override
    protected Generator<Operation<?>> createTransactionalOperations( GeneratorBuilder generatorBuilder )
            throws WorkloadException
    {
        /*
         * Create Generators for desired Operations
         */

        Set<Pair<Double, Generator<Operation<?>>>> operations = new HashSet<Pair<Double, Generator<Operation<?>>>>();

        operations.add( Pair.create( 1d, (Generator<Operation<?>>) new Query1Generator( "Chen" ) ) );

        Calendar calendar = Calendar.getInstance();
        calendar.set( 2010, Calendar.JANUARY, 1 );
        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";
        Date startDate = calendar.getTime();
        int durationDays = 365 * 2;
        operations.add( Pair.create( 1d, (Generator<Operation<?>>) new Query3Generator( personId, countryX, countryY,
                startDate, durationDays ) ) );

        calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        personId = 143;
        startDate = calendar.getTime();
        durationDays = 300;
        operations.add( Pair.create( 1d, (Generator<Operation<?>>) new Query4Generator( personId, startDate,
                durationDays ) ) );

        /*
         * Create Discrete Generator from 
         */

        Generator<Operation<?>> operationGenerator = generatorBuilder.discreteValuedGenerator( operations ).build();

        /*
         * Filter Interesting Operations
         */

        List<Class<? extends Operation<?>>> operationsToInclude = new ArrayList<Class<? extends Operation<?>>>();
        operationsToInclude.add( LdbcQuery1.class );
        operationsToInclude.add( LdbcQuery3.class );
        operationsToInclude.add( LdbcQuery4.class );
        Predicate<Operation<?>> filter = new IncludeOnlyClassesPredicate<Operation<?>>( operationsToInclude );

        Generator<Operation<?>> filteredGenerator = new FilterGeneratorWrapper<Operation<?>>( operationGenerator,
                filter );

        Generator<Time> startTimeGenerator = GeneratorUtils.randomTimeGeneratorFromNow( generatorBuilder, Time.now(),
                Time.fromMilli( 100 ).asMilli(), Time.fromMilli( 1000 ).asMilli() );

        return new StartTimeOperationGeneratorWrapper( startTimeGenerator, filteredGenerator );
    }

    class Query1Generator extends Generator<Operation<?>>
    {
        private final String firstName;

        protected Query1Generator( final String firstName )
        {
            super( null );
            this.firstName = firstName;
        }

        @Override
        protected Operation<?> doNext() throws GeneratorException
        {
            return new LdbcQuery1( firstName );
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
            super( null );
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
            super( null );
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

    class IncludeOnlyClassesPredicate<T> implements Predicate<T>
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
        public boolean apply( T input )
        {
            return true == includedItems.contains( input.getClass() );
        }
    }
}
