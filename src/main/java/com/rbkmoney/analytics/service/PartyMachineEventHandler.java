package com.rbkmoney.analytics.service;

import com.rbkmoney.analytics.listener.handler.AdvancedBatchHandler;
import com.rbkmoney.analytics.listener.handler.HandlerManager;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyEventData;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PartyMachineEventHandler {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final MachineEventParser<PartyEventData> eventParser;

    private final HandlerManager<PartyChange, MachineEvent> handlerManager;

    public PartyMachineEventHandler(MachineEventParser<PartyEventData> eventParser,
                         List<AdvancedBatchHandler<PartyChange, MachineEvent>> handlers) {
        this.eventParser = eventParser;
        this.handlerManager = new HandlerManager<>(Collections.emptyList(), handlers);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            batch.stream()
                    .map(machineEvent -> Map.entry(machineEvent, eventParser.parse(machineEvent)))
                    .filter(entry -> entry.getValue().isSetChanges())
                    .map(entry -> entry.getValue().getChanges().stream()
                            .map(partyChange -> Map.entry(entry.getKey(), partyChange))
                            .collect(Collectors.toList()))
                    .flatMap(List::stream)
                    .collect(Collectors.groupingBy(
                            entry -> Optional.ofNullable(handlerManager.getAdvancedHandler(entry.getValue())),
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
