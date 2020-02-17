package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.config.properties.PaymentStreamProperties;
import com.rbkmoney.analytics.config.properties.RefundStreamProperties;
import com.rbkmoney.analytics.dao.model.MgPaymentSinkRow;
import com.rbkmoney.analytics.dao.model.MgRefundRow;
import com.rbkmoney.mg.event.sink.EventSinkAggregationStreamFactoryImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class StartupListener implements ApplicationListener<ContextRefreshedEvent> {

    public static final long CLOSE_STREAM_TIMEOUT_SECONDS = 10L;

    @Value("${kafka.streams.event.sink.enable}")
    private boolean enableEventSinkStream;

    @Value("${kafka.streams.refund-stream-enabled:true}")
    private boolean refundStreamEnabled;
    @Value("${kafka.streams.payment-stream-enabled:true}")
    private boolean paymentStreamEnabled;

    private final Properties eventSinkPaymentStreamProperties;
    private final PaymentStreamProperties paymentStreamProperties;
    private final RefundStreamProperties refundStreamProperties;
    private final Properties eventSinkRefundStreamProperties;
    private final EventSinkAggregationStreamFactoryImpl<String, MgPaymentSinkRow, MgPaymentSinkRow> eventSinkAggregationStreamFactory;
    private final EventSinkAggregationStreamFactoryImpl<String, MgRefundRow, MgRefundRow> eventSinkRefundAggregationStreamFactory;

    private List<KafkaStreams> eventSinkStreams = new ArrayList<>();

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        long startPreloadTime = System.currentTimeMillis();
        startEventStream(startPreloadTime);
    }

    private void startEventStream(long startPreloadTime) {
        if (enableEventSinkStream) {
            if (paymentStreamProperties.isCleanInstall()) {
                KafkaStreams eventSinkStream = eventSinkAggregationStreamFactory.create(eventSinkPaymentStreamProperties);
                eventSinkStreams.add(eventSinkStream);
                log.info("StartupListener start stream eventSinkStream: {}", eventSinkStream.allMetadata());
            }
            if (refundStreamProperties.isCleanInstall()) {
                KafkaStreams eventSinkStreamRefund = eventSinkRefundAggregationStreamFactory.create(eventSinkRefundStreamProperties);
                eventSinkStreams.add(eventSinkStreamRefund);
                log.info("StartupListener start stream eventSinkStreamRefund: {}", eventSinkStreamRefund.allMetadata());
            }

            log.info("StartupListener start stream preloadTime: {} ms", System.currentTimeMillis() - startPreloadTime);
        }
    }

    public void stop() {
        if (eventSinkStreams != null && !eventSinkStreams.isEmpty()) {
            eventSinkStreams.forEach(kafkaStreams -> kafkaStreams.close(Duration.ofSeconds(CLOSE_STREAM_TIMEOUT_SECONDS)));
        }
    }

}