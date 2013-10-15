package com.ldbc.socialnet.workload;

import java.util.Collection;

public class LdbcQuery1Result
{
    private final String firstName;
    private final String lastName;
    private final long birthday;
    private final long creationDate;
    private final String gender;
    private final String[] languages;
    private final String browser;
    private final String ip;
    private final String[] emails;
    private final String personCity;
    private final Collection<String> unis;
    private final Collection<String> companies;

    public LdbcQuery1Result( String firstName, String lastName, long birthday, long creationDate, String gender,
            String[] languages, String browser, String ip, String[] emails, String personCity, Collection<String> unis,
            Collection<String> companies )
    {
        super();
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthday = birthday;
        this.creationDate = creationDate;
        this.gender = gender;
        this.languages = languages;
        this.browser = browser;
        this.ip = ip;
        this.emails = emails;
        this.personCity = personCity;
        this.unis = unis;
        this.companies = companies;
    }

    public String firstName()
    {
        return firstName;
    }

    public String lastName()
    {
        return lastName;
    }

    public long birthday()
    {
        return birthday;
    }

    public long creationDate()
    {
        return creationDate;
    }

    public String gender()
    {
        return gender;
    }

    public String[] languages()
    {
        return languages;
    }

    public String browser()
    {
        return browser;
    }

    public String ip()
    {
        return ip;
    }

    public String[] emails()
    {
        return emails;
    }

    public String personCity()
    {
        return personCity;
    }

    public Collection<String> unis()
    {
        return unis;
    }

    public Collection<String> companies()
    {
        return companies;
    }
}
