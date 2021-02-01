package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyEventData;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.sink.common.parser.impl.MachineEventParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyMachineEventHandler {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final MachineEventParser<PartyEventData> eventParser;
    private final PartyManagementEventHandler shopEventHandler;
    private final PartyManagementEventHandler contractEventHandler;
    private final PartyManagementEventHandler contractorEventHandler;
    private final PartyManagementEventHandler partyEventHandler;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;
            for (MachineEvent machineEvent : batch) {
                handleEvent(machineEvent);
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }

    private void handleEvent(MachineEvent machineEvent) {
        log.debug("Party Machine event: {}", machineEvent);
        PartyEventData eventData = eventParser.parse(machineEvent);
        if (eventData.isSetChanges()) {
            log.debug("Party changes size: {}", eventData.getChanges().size());
            for (PartyChange change : eventData.getChanges()) {
                log.debug("Party change: {}", change);
                partyEventHandler.handle(machineEvent, change);
                contractorEventHandler.handle(machineEvent, change);
                contractEventHandler.handle(machineEvent, change);
                shopEventHandler.handle(machineEvent, change);
            }
        }
    }

}
