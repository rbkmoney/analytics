package com.rbkmoney.analytics.config;

import com.rbkmoney.analytics.config.properties.KafkaSslProperties;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.analytics.serde.MachineEventDeserializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.mg.event.sink.MgEventSinkRowMapper;
import com.rbkmoney.mg.event.sink.converter.SinkEventToEventPayloadConverter;
import com.rbkmoney.mg.event.sink.handler.MgEventSinkHandlerExecutor;
import com.rbkmoney.mg.event.sink.handler.flow.EventHandler;
import com.rbkmoney.mg.event.sink.service.ConsumerGroupIdService;
import com.rbkmoney.mg.event.sink.utils.SslKafkaUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(KafkaSslProperties.class)
public class KafkaConfig {

    public static final String EVENT_SINK_CLIENT_ANALYTICS = "event-sink-client-analytics";
    public static final String EVENT_SINK_CLIENT_ANALYTICS_REFUND = "event-sink-client-analytics-refund";

    private static final String RESULT_ANALYTICS = "result-analytics";
    private static final String RESULT_ANALYTICS_REFUND = "result-analytics-refund";
    private static final String EARLIEST = "earliest";
    public static final String MG_EVENT_SINK_PAYMENT = "mg-event-sink-payment";
    public static final String MG_EVENT_SINK_PAYMENT_REFUND = "mg-event-sink-payment-refund";

    @Value("${kafka.state.cache.size:10}")
    private int cacheSizeStateStoreMb;
    @Value("${kafka.max.poll.records}")
    private String maxPollRecords;
    @Value("${kafka.state.dir}")
    private String stateDir;
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;
    @Value("${kafka.stat.group}")
    private String group;


    @Value("${kafka.topic.event.sink.initial}")
    private String initialEventSink;
    @Value("${kafka.topic.event.sink.aggregated}")
    private String aggregatedSinkTopic;
    @Value("${kafka.topic.event.sink.aggregatedRefund}")
    private String aggregatedSinkTopicRefund;

    @Value("${kafka.streams.replication-factor}")
    private int replicationFactor;
    @Value("${kafka.streams.concurrency}")
    private int concurrencyStream;
    @Value("${kafka.streams.clean-install}")
    private boolean cleanInstall;

    @Value("${kafka.consumer.concurrency}")
    private int concurrencyListeners;

    @Value("${kafka.streams.linger-ms:100}")
    private int lingerMs;
    @Value("${kafka.streams.request-timeout-min:2}")
    private int requestTimeoutMin;
    @Value("${kafka.streams.delivery-timeout-min:600}")
    private int deliveryTimeoutMin;
    @Value("${kafka.streams.retry-backoff-ms:1000}")
    private int retryBackoffMs;

    @Value("${kafka.streams.consumer.session-timout:60000}")
    private int consumerSessionTimout;
    @Value("${kafka.streams.consumer.max-poll-interval:120000}")
    private int consumerMaxPollInterval;
    @Value("${kafka.streams.consumer.max-poll-records:300}")
    private int consumerMaxPollRecords;

    private final ConsumerGroupIdService consumerGroupIdService;
    private final List<EventHandler<MgPaymentSinkRow>> eventHandlers;
    private final List<EventHandler<MgRefundRow>> eventRefundHandlers;
    private final KafkaSslProperties kafkaSslProperties;
//
//    @Bean
//    public Properties eventSinkPaymentStreamProperties() {
//        Properties props = createDefaultProperties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupIdService.generateGroupId(MG_EVENT_SINK_PAYMENT));
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MgPaymentRowSerde.class);
//        props.put(StreamsConfig.CLIENT_ID_CONFIG, EVENT_SINK_CLIENT_ANALYTICS);
//        return props;
//    }
//
//    @Bean
//    public Properties eventSinkRefundStreamProperties() {
//        Properties props = createDefaultProperties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, consumerGroupIdService.generateGroupId(MG_EVENT_SINK_PAYMENT_REFUND));
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MgRefundRowSerde.class);
//        props.put(StreamsConfig.CLIENT_ID_CONFIG, EVENT_SINK_CLIENT_ANALYTICS_REFUND);
//        return props;
//    }
//
//    private Properties createDefaultProperties() {
//        final Properties props = new Properties();
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheSizeStateStoreMb * 1024 * 1024L);
//        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
//        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
//        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, replicationFactor - 1);
//        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, concurrencyStream);
//        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfig.class);
//
//        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), consumerSessionTimout);
//        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), consumerMaxPollInterval);
//        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), consumerMaxPollRecords);
//
//        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), lingerMs);
//        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), requestTimeoutMin * 60 * 1000);
//        props.put(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), deliveryTimeoutMin * 60 * 1000 + lingerMs);
//        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), Integer.MAX_VALUE);
//        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), retryBackoffMs);
//        props.putAll(createSslConfig());
//
//        return props;
//    }

    private Map<String, Object> createSslConfig() {
        log.info("Kafka ssl isEnabled: {}", kafkaSslProperties.isEnabled());
        return SslKafkaUtils.sslConfigure(
                kafkaSslProperties.isEnabled(),
                kafkaSslProperties.getTrustStoreLocation(),
                kafkaSslProperties.getTrustStorePassword(),
                kafkaSslProperties.getKeyStoreLocation(),
                kafkaSslProperties.getKeyStorePassword(),
                kafkaSslProperties.getKeyPassword());
    }
//
//    @Bean
//    public EventSinkAggregationStreamFactoryImpl<String, MgPaymentSinkRow, MgPaymentSinkRow> eventSinkAggregationStreamFactory(
//            MgPaymentAggregator mgPaymentAggregator,
//            MgEventSinkRowMapper<MgPaymentSinkRow> mgEventSinkRowMgEventSinkRowMapper) {
//        return new EventSinkAggregationStreamFactoryImpl<>(
//                initialEventSink,
//                aggregatedSinkTopic,
//                cleanInstall,
//                new SinkEventSerde(),
//                Serdes.String(),
//                new MgPaymentRowSerde(),
//                MgPaymentSinkRow::new,
//                mgPaymentAggregator,
//                mgEventSinkRowMgEventSinkRowMapper,
//                mgEventSinkRow -> mgEventSinkRow.getStatus() != null,
//                (key, value) -> value.getInvoiceId() + "_" + value.getPaymentId());
//    }
//
//    @Bean
//    public EventSinkAggregationStreamFactoryImpl<String, MgRefundRow, MgRefundRow> eventSinkRefundAggregationStreamFactory(
//            MgRefundAggregator mgRefundAggregator,
//            MgEventSinkRowMapper<MgRefundRow> mgRefundRowRowMgEventSinkRowMapper) {
//        return new EventSinkAggregationStreamFactoryImpl<>(
//                initialEventSink,
//                aggregatedSinkTopicRefund,
//                cleanInstall,
//                new SinkEventSerde(),
//                Serdes.String(),
//                new MgRefundRowSerde(),
//                MgRefundRow::new,
//                mgRefundAggregator,
//                mgRefundRowRowMgEventSinkRowMapper,
//                mgRefundRow -> mgRefundRow.getStatus() != null,
//                (key, value) -> value.getInvoiceId() + "_" + value.getPaymentId());
//    }

    @Bean
    public MgEventSinkHandlerExecutor<MgPaymentSinkRow> mgEventSinkHandler(
            SinkEventToEventPayloadConverter sinkEventToEventPayloadConverter) {
        return new MgEventSinkHandlerExecutor<>(sinkEventToEventPayloadConverter, eventHandlers);
    }

    @Bean
    public MgEventSinkRowMapper<MgPaymentSinkRow> mgRefundRowRowMgEventSinkRowMapper(MgEventSinkHandlerExecutor<MgPaymentSinkRow> mgEventSinkHandler) {
        return new MgEventSinkRowMapper<>(mgEventSinkHandler);
    }

    @Bean
    public MgEventSinkRowMapper<MgRefundRow> mgEventSinkRowMgEventSinkRowMapper(MgEventSinkHandlerExecutor<MgRefundRow> mgRefundRowHandler) {
        return new MgEventSinkRowMapper<>(mgRefundRowHandler);
    }

    @Bean
    public MgEventSinkHandlerExecutor<MgRefundRow> mgRefundRowHandler(
            SinkEventToEventPayloadConverter sinkEventToEventPayloadConverter) {
        return new MgEventSinkHandlerExecutor<>(sinkEventToEventPayloadConverter, eventRefundHandlers);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MachineEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MachineEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        String consumerGroup = consumerGroupIdService.generateGroupId(group);
        initDefaultListenerProperties(factory, consumerGroup, new MachineEventDeserializer());
        return factory;
    }

    private <T> void initDefaultListenerProperties(ConcurrentKafkaListenerContainerFactory<String, T> factory,
                                                   String consumerGroup, Deserializer<T> deserializer) {
        final Map<String, Object> props = createDefaultProperties(consumerGroup);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        DefaultKafkaConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(), deserializer);
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrencyListeners);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, MgRefundRow> kafkaListenerRefundContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, MgRefundRow> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        String consumerGroup = consumerGroupIdService.generateGroupId(RESULT_ANALYTICS_REFUND);
//        initDefaultListenerProperties(factory, consumerGroup, new MgRefundRowDeserializer());
//        return factory;
//    }

    @NotNull
    private Map<String, Object> createDefaultProperties(String value) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, value);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.putAll(createSslConfig());
        return props;
    }
}
