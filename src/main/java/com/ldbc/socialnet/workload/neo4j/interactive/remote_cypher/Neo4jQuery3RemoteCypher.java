package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class Neo4jQuery3RemoteCypher extends Neo4jQuery3<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery3Result> execute(Connection connection, LdbcQuery3 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(COUNTRY_X, operation.countryX());
            preparedStatement.setString(COUNTRY_Y, operation.countryY());
            preparedStatement.setLong(MIN_DATE, operation.startDateAsMilli());
            preparedStatement.setLong(MAX_DATE, operation.endDateAsMilli());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery3Result> {
        private final ResultSet resultSet;

        private ResultSetIterator(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() {
            try {
                return resultSet.next();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public LdbcQuery3Result next() {
            try {
                return new LdbcQuery3Result(
                        resultSet.getString("friendName"),
                        resultSet.getLong("xCount"),
                        resultSet.getLong("yCount"));
            } catch (SQLException e) {
                throw new RuntimeException("Error while retrieving next row", e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove() not supported by " + getClass().getName());
        }
    }
}
