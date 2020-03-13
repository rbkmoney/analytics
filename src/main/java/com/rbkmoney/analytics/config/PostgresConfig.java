package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.PostgresDbProperties;
import lombok.RequiredArgsConstructor;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class PostgresConfig {

    private final PostgresDbProperties postgresDbProperties;

    @Bean
    public DataSource postgresDatasource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresDbProperties.getUrl());
        dataSource.setUser(postgresDbProperties.getUser());
        dataSource.setPassword(postgresDbProperties.getPassword());
        dataSource.setCurrentSchema(postgresDbProperties.getSchema());
        return dataSource;
    }

    @Bean
    @Autowired
    public JdbcTemplate postgresJdbcTemplate(DataSource postgresDatasource) {
        return new JdbcTemplate(postgresDatasource);
    }
}
