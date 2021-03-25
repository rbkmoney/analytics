package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.ClickHouseDbProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import javax.sql.DataSource;

import java.util.Properties;

@Configuration
@RequiredArgsConstructor
public class ClickHouseConfig {

    private final ClickHouseDbProperties clickHouseDbProperties;

    @Bean
    public ClickHouseDataSource clickHouseDataSource() {
        Properties info = new Properties();
        info.setProperty(ClickHouseQueryParam.USER.getKey(), clickHouseDbProperties.getUser());
        info.setProperty(ClickHouseQueryParam.PASSWORD.getKey(), clickHouseDbProperties.getPassword());
        info.setProperty(ClickHouseQueryParam.COMPRESS.getKey(), String.valueOf(clickHouseDbProperties.getCompress()));
        info.setProperty(ClickHouseQueryParam.CONNECT_TIMEOUT.getKey(),
                String.valueOf(clickHouseDbProperties.getConnectionTimeout())
        );

        return new ClickHouseDataSource(clickHouseDbProperties.getUrl(), info);
    }

    @Bean
    @Autowired
    public JdbcTemplate clickHouseJdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }

}
