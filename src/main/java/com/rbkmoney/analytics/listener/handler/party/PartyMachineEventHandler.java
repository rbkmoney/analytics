package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyService;
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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyMachineEventHandler {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final MachineEventParser<PartyEventData> eventParser;

    private final List<ChangeHandler<PartyChange, MachineEvent, List<Party>>> partyHandlers;

    private final List<ChangeHandler<PartyChange, MachineEvent, List<Shop>>> shopHandlers;

    private final PartyService partyService;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            LocalStorage localStorage = new LocalStorage();
            for (MachineEvent machineEvent : batch) {
                PartyEventData eventData = eventParser.parse(machineEvent);
                if (eventData.isSetChanges()) {
                    for (PartyChange change : eventData.getChanges()) {
                        List<Party> changedParties = partyHandlers.stream()
                                .filter(changeHandler -> changeHandler.accept(change))
                                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent, localStorage).stream())
                                .collect(Collectors.toList());
                        List<Shop> changedShops = shopHandlers.stream()
                                .filter(changeHandler -> changeHandler.accept(change))
                                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent, localStorage).stream())
                                .collect(Collectors.toList());
                        if (!changedParties.isEmpty()) {
                            partyService.saveParty(changedParties);
                        }
                        if (!changedShops.isEmpty()) {
                            partyService.saveShop(changedShops);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }

}
