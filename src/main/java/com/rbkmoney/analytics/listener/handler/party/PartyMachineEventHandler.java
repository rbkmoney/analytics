package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.analytics.service.model.ShopKey;
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

    private final PartyEventMerger partyEventMerger;

    private final PartyService partyService;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            for (MachineEvent machineEvent : batch) {
                log.debug("Party Machine event: {}", machineEvent);
                PartyEventData eventData = eventParser.parse(machineEvent);
                if (eventData.isSetChanges()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Party changes size: {}", eventData.getChanges());
                    }
                    for (PartyChange change : eventData.getChanges()) {
                        log.debug("Party change: {}", change);
                        List<Party> changedParties = partyHandlers.stream()
                                .filter(changeHandler -> changeHandler.accept(change))
                                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                                .collect(Collectors.groupingBy(Party::getPartyId, Collectors.toList()))
                                .entrySet().stream()
                                .map(entryList -> partyEventMerger.mergeParty(entryList.getKey(), entryList.getValue()))
                                .collect(Collectors.toList());

                        List<Shop> changedShops = shopHandlers.stream()
                                .filter(changeHandler -> changeHandler.accept(change))
                                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                                .collect(Collectors.groupingBy(o -> new ShopKey(o.getPartyId(), o.getShopId()), Collectors.toList()))
                                .entrySet().stream()
                                .map(shopKeyListEntry -> partyEventMerger.mergeShop(shopKeyListEntry.getKey(), shopKeyListEntry.getValue()))
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
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Exception during PartyListener process", e);
            Thread.sleep(throttlingTimeout);
            throw e;
        }
    }


}
