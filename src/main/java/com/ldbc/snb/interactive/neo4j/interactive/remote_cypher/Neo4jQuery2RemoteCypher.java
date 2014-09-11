package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery2RemoteCypher extends Neo4jQuery2<Connection> {


    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery2Result> execute(Connection connection, LdbcQuery2 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setLong(MAX_DATE, operation.maxDate().getTime());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery2Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery2Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery2Result resultSetToLdbcQuery2Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery2Result(
                resultSet.getLong("personId"),
                resultSet.getString("personFirstName"),
                resultSet.getString("personLastName"),
                resultSet.getLong("messageId"),
                resultSet.getString("messageContent"),
                resultSet.getLong("messageDate"));

    }
}
