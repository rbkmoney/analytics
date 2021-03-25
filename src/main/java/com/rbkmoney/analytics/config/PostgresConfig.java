package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.PostgresDbProperties;
import lombok.RequiredArgsConstructor;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class PostgresConfig {

    private final PostgresDbProperties postgresDbProperties;

    @Bean
    public PGSimpleDataSource postgresDatasource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgresDbProperties.getUrl());
        dataSource.setUser(postgresDbProperties.getUser());
        dataSource.setPassword(postgresDbProperties.getPassword());
        dataSource.setCurrentSchema(postgresDbProperties.getSchema());
        return dataSource;
    }

    @Bean
    public DataSourceConnectionProvider dataSourceConnectionProvider(
            TransactionAwareDataSourceProxy transactionAwareDataSourceProxy) {
        return new DataSourceConnectionProvider(transactionAwareDataSourceProxy);
    }

    @Bean
    public TransactionAwareDataSourceProxy transactionAwareDataSource(DataSource postgresDatasource) {
        return new TransactionAwareDataSourceProxy(postgresDatasource);
    }

    @Bean
    public DataSourceConnectionProvider connectionProvider(
            TransactionAwareDataSourceProxy transactionAwareDataSourceProxy) {
        return new DataSourceConnectionProvider(transactionAwareDataSourceProxy);
    }

    @Bean
    public DefaultConfiguration configuration(DataSourceConnectionProvider dataSourceConnectionProvider) {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
        jooqConfiguration.set(dataSourceConnectionProvider);

        return jooqConfiguration;
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(PGSimpleDataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        return initializer;
    }

    @Bean
    @Autowired
    public JdbcTemplate postgresJdbcTemplate(DataSource postgresDatasource) {
        return new JdbcTemplate(postgresDatasource);
    }
}
