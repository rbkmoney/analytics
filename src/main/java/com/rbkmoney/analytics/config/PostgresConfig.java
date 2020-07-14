package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.PostgresDbProperties;
import lombok.RequiredArgsConstructor;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class PostgresConfig {

    private final PostgresDbProperties postgresDbProperties;

    @Bean
    @Primary
    public DataSource postgresDatasource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresDbProperties.getUrl());
        dataSource.setUser(postgresDbProperties.getUser());
        dataSource.setPassword(postgresDbProperties.getPassword());
        dataSource.setCurrentSchema(postgresDbProperties.getSchema());
        return dataSource;
    }

//    @Bean
//    public DataSourceConnectionProvider dataSourceConnectionProvider(DataSource postgresDatasource) {
//        return new DataSourceConnectionProvider(new TransactionAwareDataSourceProxy(postgresDatasource));
//    }
//
//    @Bean
//    public DefaultDSLContext dsl(DataSourceConnectionProvider dataSourceConnectionProvider) {
//        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
//        jooqConfiguration.set(dataSourceConnectionProvider);
//        jooqConfiguration
//                .set(new DefaultExecuteListenerProvider());
//
//        return new DefaultDSLContext(jooqConfiguration);
//    }

    @Bean
    @Autowired
    public JdbcTemplate postgresJdbcTemplate(DataSource postgresDatasource) {
        return new JdbcTemplate(postgresDatasource);
    }
}
