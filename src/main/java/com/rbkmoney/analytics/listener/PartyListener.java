package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.service.PartyMachineEventHandler;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyListener {

    private final PartyMachineEventHandler partyMachineEventHandler;

    @KafkaListener(autoStartup = "${kafka.listener.party.enabled}",
            topics = "${kafka.topic.party.initial}",
            containerFactory = "partyListenerContainerFactory")
    public void listen(List<MachineEvent> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) throws InterruptedException {
        log.info("PartyListener listen offsets: {} partition: {} batch.size: {}", offsets, partition, batch.size());
        partyMachineEventHandler.handleMessages(batch);
        ack.acknowledge();
    }

}
