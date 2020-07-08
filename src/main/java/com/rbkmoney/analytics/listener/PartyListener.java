package com.rbkmoney.analytics.listener;

import com.rbkmoney.analytics.converter.SourceEventParser;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.handler.HandlerManager;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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
public class PartyListener {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final SourceEventParser eventParser;
    private final HandlerManager<PartyChange, MachineEvent> handlerManager;

    public PartyListener(SourceEventParser eventParser,
                         List<BatchHandler<PartyChange, MachineEvent>> handlers) {
        this.eventParser = eventParser;
        this.handlerManager = new HandlerManager<>(handlers);
    }

    @KafkaListener(autoStartup = "${kafka.listener.event.sink.enabled}",
            topics = "${kafka.topic.event.sink.initial}",
            containerFactory = "partyListenerContainerFactory")
    public void listen(List<MachineEvent> batch,
                       @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                       @Header(KafkaHeaders.OFFSET) int offsets,
                       Acknowledgment ack) throws InterruptedException {
        log.info("PartyListener listen offsets: {} partition: {} batch.size: {}", offsets, partition, batch.size());
        handleMessages(batch);
        ack.acknowledge();
    }

    private void handleMessages(List<MachineEvent> batch) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            batch.stream()
                    .map(machineEvent -> Map.entry(machineEvent, eventParser.parseEvent(machineEvent)))
                    .filter(entry -> entry.getValue().isSetPartyChanges())
                    .map(entry -> entry.getValue().getPartyChanges().stream()
                            .map(partyChange -> Map.entry(entry.getKey(), partyChange))
                            .collect(Collectors.toList()))
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(
                            entry -> Optional.ofNullable(handlerManager.getHandler(entry.getValue())),
                            Collectors.toList()))
                    .forEach((handler, entries) -> {
                        handler.ifPresent(eventBatchHandler -> eventBatchHandler.handle(entries).execute());
                    });
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }


}
