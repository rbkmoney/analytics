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

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyMachineEventHandler {

    @Value("${kafka.consumer.throttling-timeout-ms}")
    private int throttlingTimeout;

    private final MachineEventParser<PartyEventData> eventParser;

    private final List<ChangeHandler<PartyChange, MachineEvent>> handlers;

    private final PartyService partyService;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch) throws InterruptedException {
        LocalStorage localStorage = new LocalStorage();
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            for (MachineEvent machineEvent : batch) {
                PartyEventData eventData = eventParser.parse(machineEvent);
                if (eventData.isSetChanges()) {
                    for (PartyChange change : eventData.getChanges()) {
                        handlers.stream()
                                .filter(changeHandler -> changeHandler.accept(change))
                                .forEach(changeHandler -> {
                                    changeHandler.handleChange(change, machineEvent, localStorage);
                                });
                    }
                }
            }
            List<Party> parties = localStorage.getParties();
            if (!parties.isEmpty()) {
                log.debug("Save parties: size={}", parties.size());
                partyService.saveParty(parties);
            }
            List<Shop> shops = localStorage.getShops();
            if (!shops.isEmpty()) {
                log.debug("Save shops: size={}", shops.size());
                partyService.saveShop(shops);
            }
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }

}
