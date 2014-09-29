package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.google.common.collect.Lists;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery1;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Neo4jQuery1RemoteCypher extends Neo4jQuery1<Connection> {
    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(Connection connection, LdbcQuery1 operation) throws DbException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STRING)) {
            preparedStatement.setLong(PERSON_ID, operation.personId());
            preparedStatement.setString(FRIEND_FIRST_NAME, operation.firstName());
            preparedStatement.setInt(LIMIT, operation.limit());
            ResultSet resultSet = preparedStatement.executeQuery();
            List<LdbcQuery1Result> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSetToLdbcQuery1Result(resultSet));
            }
            return results.iterator();
        } catch (SQLException e) {
            throw new DbException("Error while executing query", e);
        }
    }

    private LdbcQuery1Result resultSetToLdbcQuery1Result(ResultSet resultSet) throws SQLException {
        long id = resultSet.getLong("id");
        String lastName = resultSet.getString("lastName");
        int distance = resultSet.getInt("distance");
        long birthday = resultSet.getLong("birthday");
        long creationDate = resultSet.getLong("creationDate");
        String gender = resultSet.getString("gender");
        String browser = resultSet.getString("browser");
        String locationIp = resultSet.getString("locationIp");
        Object emailsObject = resultSet.getObject("emails");
        Iterable<String> emails = (Iterable.class.isAssignableFrom(emailsObject.getClass()))
                ? (Iterable) emailsObject
                : Lists.newArrayList((String[]) emailsObject);
        Object languagesObject = resultSet.getObject("languages");
        Iterable<String> languages = (Iterable.class.isAssignableFrom(languagesObject.getClass()))
                ? (Iterable) languagesObject
                : Lists.newArrayList((String[]) languagesObject);
        String cityName = resultSet.getString("cityName");
        Iterable unis = (Iterable) resultSet.getObject("unis");
        Iterable companies = (Iterable) resultSet.getObject("companies");

        return new LdbcQuery1Result(
                id,
                lastName,
                distance,
                birthday,
                creationDate,
                gender,
                browser,
                locationIp,
                emails,
                languages,
                cityName,
                unis,
                companies
        );
    }
}
