package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery9;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery9RemoteCypher extends Neo4jQuery9<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery9Result> execute(Connection connection, LdbcQuery9 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setLong(LATEST_DATE, operation.maxDate().getTime());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery9Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery9Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery9Result resultSetToLdbcQuery9Result(ResultSet resultSet) throws SQLException {
        return new LdbcQuery9Result(
                resultSet.getLong("personId"),
                resultSet.getString("personFirstName"),
                resultSet.getString("personLastName"),
                resultSet.getLong("messageId"),
                resultSet.getString("messageContent"),
                resultSet.getLong("messageCreationDate"));
    }
}
