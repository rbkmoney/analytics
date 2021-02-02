package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.AnalyticsApplication;
import com.rbkmoney.analytics.config.KafkaConfig;
import com.rbkmoney.analytics.config.SerializeConfig;
import com.rbkmoney.analytics.listener.handler.rate.RateMachineEventHandler;
import com.rbkmoney.analytics.utils.KafkaAbstractTest;
import com.rbkmoney.analytics.utils.RateSinkEventTestUtils;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.mg.event.sink.service.ConsumerGroupIdService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {AnalyticsApplication.class}, properties = {"kafka.state.cache.size=0"})
@ContextConfiguration(initializers = {RateListenerTest.Initializer.class})
public class RateListenerTest extends KafkaAbstractTest {

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
            postgres.start();
        }
    }

    @MockBean
    RateMachineEventHandler eventHandler;

    @Value("${kafka.topic.rate.initial}")
    public String rateTopic;

    @Test
    public void handle() throws InterruptedException {
        String sourceId = "CBR";

        final List<SinkEvent> sinkEvents = RateSinkEventTestUtils.create(sourceId);
        sinkEvents.forEach(event -> produceMessageToTopic(rateTopic, event));

        Mockito.verify(eventHandler, Mockito.after(1000).times(1)).handle(any(), any());
    }

}
