package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.listener.handler.rate.RateMachineEventHandler;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class RateListener {

    private final RateMachineEventHandler rateMachineEventHandler;

    @KafkaListener(autoStartup = "${kafka.listener.rate.enabled}",
            topics = "${kafka.topic.rate.initial}",
            containerFactory = "rateContainerFactory")
    public void handle(List<MachineEvent> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) throws InterruptedException {
        log.info("Got RateListener listen offsets: {}, partition: {}, batch.size: {}", offsets, partition, batch.size());
        rateMachineEventHandler.handle(batch, ack);
        ack.acknowledge();
        log.info("Batch RateListener has been committed, size={}", batch.size());
    }


}
