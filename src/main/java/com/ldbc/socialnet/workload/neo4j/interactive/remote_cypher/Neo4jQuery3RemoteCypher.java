package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Neo4jQuery3RemoteCypher extends Neo4jQuery3<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery3Result> execute(Connection connection, LdbcQuery3 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(COUNTRY_X, operation.countryXName());
            preparedStatement.setString(COUNTRY_Y, operation.countryYName());
            long startDateAsMilli = operation.startDate().getTime();
            int durationHours = operation.durationDays() * 24;
            long endDateAsMilli = Time.fromMilli(startDateAsMilli).plus(Duration.fromHours(durationHours)).asMilli();
            preparedStatement.setLong(MIN_DATE, startDateAsMilli);
            preparedStatement.setLong(MAX_DATE, endDateAsMilli);
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            return new ResultSetIterator(resultSet);
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private class ResultSetIterator implements Iterator<LdbcQuery3Result> {
        private final ResultSet resultSet;
        private boolean hasNext;

        private ResultSetIterator(ResultSet resultSet) throws SQLException {
            this.resultSet = resultSet;
            this.hasNext = resultSet.next();
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public LdbcQuery3Result next() {
            try {
                if (false == hasNext) throw new NoSuchElementException();
                LdbcQuery3Result result = new LdbcQuery3Result(
                        resultSet.getLong("friendId"),
                        resultSet.getString("friendFirstName"),
                        resultSet.getString("friendLastName"),
                        resultSet.getLong("xCount"),
                        resultSet.getLong("yCount"),
                        resultSet.getLong("xyCount"));
                hasNext = resultSet.next();
                return result;
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
