package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.dao.repository.postgres.PostgresPartyDao;
import com.rbkmoney.analytics.utils.FileUtil;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import static com.rbkmoney.analytics.listener.PartyFlowGenerator.SETTLEMENT_ID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AnalyticsApplication.class,
        properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {PartyListenerTest.Initializer.class})
public class PartyListenerTest extends KafkaAbstractTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer("postgres:9.6")
            .withStartupTimeout(Duration.ofMinutes(5));

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "postgres.db.url=" + postgres.getJdbcUrl(),
                    "postgres.db.user=" + postgres.getUsername(),
                    "postgres.db.password=" + postgres.getPassword(),
                    "spring.flyway.url=" + postgres.getJdbcUrl(),
                    "spring.flyway.user=" + postgres.getUsername(),
                    "spring.flyway.password=" + postgres.getPassword(),
                    "spring.flyway.enabled=true")
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    @Autowired
    private PostgresPartyDao postgresPartyDao;

    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void testPartyEventSink() throws IOException {
        List<SinkEvent> sinkEvents = PartyFlowGenerator.generatePartyFlow();

        sinkEvents.forEach(this::produceMessageToParty);

        await().atMost(60, SECONDS).until(() -> {
            Integer partyCount = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.party" +
                    " WHERE contractor_identification_level = 'partial'", Integer.class);
            boolean isLastPartyChange = partyCount != null && partyCount > 0;
            Integer shopCount = postgresJdbcTemplate.queryForObject(String.format("SELECT count(*) FROM analytics.shop" +
                    " WHERE account_settlement = '%s'", SETTLEMENT_ID), Integer.class);
            boolean isLastShopChange = shopCount != null && shopCount > 0;
            if (isLastPartyChange && isLastShopChange) {
                return true;
            }
            Thread.sleep(1000);
            return false;
        });
    }

}
