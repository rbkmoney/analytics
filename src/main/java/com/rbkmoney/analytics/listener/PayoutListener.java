package com.rbkmoney.analytics.listener;

import com.rbkmoney.damsel.payout_processing.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PayoutListener {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    @KafkaListener(
            autoStartup = "${kafka.listener.payout.enabled}",
            topics = "${kafka.topic.payout.initial}",
            containerFactory = "payoutListenerContainerFactory")
    public void listen(
            List<Event> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offsets,
            Acknowledgment ack) throws InterruptedException {
        log.info("PayoutListener listen offsets: {}, partition: {}, batch.size: {}", offsets, partition, batch.size());
        handleMessages(batch);
        ack.acknowledge();
    }

    private void handleMessages(List<Event> batch) throws InterruptedException {
        try {
            if (!CollectionUtils.isEmpty(batch)) return;

            // TODO [a.romanov]: impl
        } catch (Exception e) {
            log.error("Error when PayoutListener listen e: ", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }
}
