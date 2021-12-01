package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.analytics.utils.RateSinkEventTestUtils;
import com.rbkmoney.analytics.utils.Version;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {AnalyticsApplication.class}, properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {RateListenerTest.Initializer.class})
public class RateListenerTest extends KafkaAbstractTest {

    @ClassRule
    @SuppressWarnings("rawtypes")
    public static PostgreSQLContainer postgres = (PostgreSQLContainer) new PostgreSQLContainer(Version.POSTGRES_VERSION)
            .withStartupTimeout(Duration.ofMinutes(5));
    @Value("${kafka.topic.rate.initial}")
    public String rateTopic;
    @Autowired
    private JdbcTemplate postgresJdbcTemplate;

    @Test
    public void handle() throws InterruptedException {
        String sourceId = "CBR";

        final List<SinkEvent> sinkEvents = RateSinkEventTestUtils.create(sourceId);
        sinkEvents.forEach(event -> produceMessageToTopic(rateTopic, event));

        await().atMost(60, SECONDS).until(() -> {
            Integer count = postgresJdbcTemplate.queryForObject("SELECT count(*) FROM analytics.rate", Integer.class);
            if (count == 0) {
                Thread.sleep(1000);
                return false;
            }
            return true;
        });
        final List<Map<String, Object>> maps = postgresJdbcTemplate.queryForList("SELECT * FROM analytics.rate");

        assertEquals(4, maps.size());
    }

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
            postgres.start();
        }
    }

}
