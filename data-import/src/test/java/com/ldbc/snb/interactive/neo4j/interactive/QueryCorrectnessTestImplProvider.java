package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.Iterator;

public interface QueryCorrectnessTestImplProvider<CONNECTION> {

    CONNECTION openConnection(String path) throws Exception;

    void closeConnection(CONNECTION connection) throws Exception;

    Iterator<LdbcQuery1Result> neo4jQuery1Impl(CONNECTION connection, LdbcQuery1 operation) throws Exception;

    Iterator<LdbcQuery2Result> neo4jQuery2Impl(CONNECTION connection, LdbcQuery2 operation) throws Exception;

    Iterator<LdbcQuery3Result> neo4jQuery3Impl(CONNECTION connection, LdbcQuery3 operation) throws Exception;

    Iterator<LdbcQuery4Result> neo4jQuery4Impl(CONNECTION connection, LdbcQuery4 operation) throws Exception;

    Iterator<LdbcQuery5Result> neo4jQuery5Impl(CONNECTION connection, LdbcQuery5 operation) throws Exception;

    Iterator<LdbcQuery6Result> neo4jQuery6Impl(CONNECTION connection, LdbcQuery6 operation) throws Exception;

    Iterator<LdbcQuery7Result> neo4jQuery7Impl(CONNECTION connection, LdbcQuery7 operation) throws Exception;

    Iterator<LdbcQuery8Result> neo4jQuery8Impl(CONNECTION connection, LdbcQuery8 operation) throws Exception;

    Iterator<LdbcQuery9Result> neo4jQuery9Impl(CONNECTION connection, LdbcQuery9 operation) throws Exception;

    Iterator<LdbcQuery10Result> neo4jQuery10Impl(CONNECTION connection, LdbcQuery10 operation) throws Exception;

    Iterator<LdbcQuery11Result> neo4jQuery11Impl(CONNECTION connection, LdbcQuery11 operation) throws Exception;

    Iterator<LdbcQuery12Result> neo4jQuery12Impl(CONNECTION connection, LdbcQuery12 operation) throws Exception;

    Iterator<LdbcQuery13Result> neo4jQuery13Impl(CONNECTION connection, LdbcQuery13 operation) throws Exception;

    Iterator<LdbcQuery14Result> neo4jQuery14Impl(CONNECTION connection, LdbcQuery14 operation) throws Exception;
}
