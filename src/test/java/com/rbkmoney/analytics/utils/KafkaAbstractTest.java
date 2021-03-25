package com.rbkmoney.analytics.utils;

import com.rbkmoney.analytics.serde.MachineEventDeserializer;
import com.rbkmoney.analytics.serde.PayoutEventDeserializer;
import com.rbkmoney.damsel.payout_processing.Event;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@DirtiesContext
@ContextConfiguration(initializers = KafkaAbstractTest.Initializer.class)
public abstract class KafkaAbstractTest {

    public static final String EVENT_SINK_TOPIC = "event_sink";
    public static final String PAYOUT_TOPIC = "payout";
    public static final String RATE_TOPIC = "mg-events-rates";
    private static final String CONFLUENT_PLATFORM_VERSION = "5.0.1";
    private static final String AGGR = "aggr";
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName
            .parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
            .withEmbeddedZookeeper();

    @Value("${kafka.topic.event.sink.initial}")
    public String eventSinkTopic;

    @Value("${kafka.topic.payout.initial}")
    public String payoutTopic;

    @Value("${kafka.topic.party.initial}")
    public String partyTopic;

    public static <T> Producer<String, T> createProducerAggr() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AGGR);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ThriftSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    static <T> Consumer<String, T> createConsumer(Class clazz) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, clazz);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }

    protected void produceMessageToTopic(String topicName, SinkEvent sinkEvent) {
        try (Producer<String, SinkEvent> producer = createProducerAggr()) {
            ProducerRecord<String, SinkEvent> producerRecord = new ProducerRecord<>(
                    topicName,
                    sinkEvent.getEvent().getSourceId(),
                    sinkEvent);
            producer.send(producerRecord).get();
            log.info("produceMessage to {} sinkEvent: {}", topicName, sinkEvent);
        } catch (Exception e) {
            log.error("Error when produce message to {} e:", topicName, e);
        }
    }

    protected void produceMessageToPayout(Event payoutEvent) {
        try (Producer<String, Event> producer = createProducerAggr()) {
            ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(
                    payoutTopic,
                    payoutEvent.getSource().getPayoutId(),
                    payoutEvent);
            producer.send(producerRecord).get();
            log.info("produceMessageToPayout() payoutEvent: {}", payoutEvent);
        } catch (Exception e) {
            log.error("Error when produceMessageToPayout e:", e);
        }
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    "kafka.bootstrap.servers=" + kafka.getBootstrapServers())
                    .applyTo(configurableApplicationContext.getEnvironment());
            initTopic(EVENT_SINK_TOPIC, MachineEventDeserializer.class);
            initTopic(PAYOUT_TOPIC, PayoutEventDeserializer.class);
            initTopic(RATE_TOPIC, MachineEventDeserializer.class);
            kafka.start();
        }

        @NotNull
        private <T> Consumer<String, T> initTopic(String topicName, Class clazz) {
            Consumer<String, T> consumer = createConsumer(clazz);
            try {
                consumer.subscribe(Collections.singletonList(topicName));
                consumer.poll(Duration.ofMillis(500L));
            } catch (Exception e) {
                log.error("KafkaAbstractTest initialize e: ", e);
            }

            consumer.close();
            return consumer;
        }
    }

}
