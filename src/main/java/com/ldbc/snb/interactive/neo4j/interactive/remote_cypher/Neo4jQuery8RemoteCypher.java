package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery8;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery8RemoteCypher extends Neo4jQuery8<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery8Result> execute(Connection connection, LdbcQuery8 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery8Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery8Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery8Result resultSetToLdbcQuery8Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery8Result(
                resultSet.getLong("personId"),
                resultSet.getString("personFirstName"),
                resultSet.getString("personLastName"),
                resultSet.getLong("commentCreationDate"),
                resultSet.getLong("commentId"),
                resultSet.getString("commentContent"));
    }
}
