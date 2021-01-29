package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.ContractRef;
import com.rbkmoney.analytics.domain.db.tables.pojos.CurrentContractor;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.domain.db.tables.pojos.Shop;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.GeneralKey;
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
    private final List<ChangeHandler<PartyChange, MachineEvent, List<ContractRef>>> contractRefHandlers;
    private final List<ChangeHandler<PartyChange, MachineEvent, List<CurrentContractor>>> currentContractorHandlers;
    private final PartyEventMerger partyEventMerger;
    private final ShopEventMerger shopEventMerger;
    private final ContractMerger contractMerger;
    private final ContractorEventMerger contractorEventMerger;
    private final PartyManagementService partyManagementService;

    @Transactional(propagation = Propagation.REQUIRED)
    public void handleMessages(List<MachineEvent> batch, Acknowledgment ack) throws InterruptedException {
        try {
            if (CollectionUtils.isEmpty(batch)) return;

            for (MachineEvent machineEvent : batch) {
                log.debug("Party Machine event: {}", machineEvent);
                PartyEventData eventData = eventParser.parse(machineEvent);
                if (eventData.isSetChanges()) {
                    log.debug("Party changes size: {}", eventData.getChanges().size());
                    for (PartyChange change : eventData.getChanges()) {
                        log.debug("Party change: {}", change);
                        updateParties(machineEvent, change);
                        updateContractor(machineEvent, change);
                        updateContractRefs(machineEvent, change);
                        updateShops(machineEvent, change);
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

    private void updateShops(MachineEvent machineEvent, PartyChange change) {
        final List<Shop> shops = shopHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(o -> new GeneralKey(o.getPartyId(), o.getShopId()), Collectors.toList()))
                .entrySet().stream()
                .map(shopKeyListEntry -> shopEventMerger.mergeShop(shopKeyListEntry.getKey(), shopKeyListEntry.getValue()))
                .collect(Collectors.toList());
        if (!shops.isEmpty()) {
            partyManagementService.saveShop(shops);
        }
    }

    private void updateContractRefs(MachineEvent machineEvent, PartyChange change) {
        final List<ContractRef> contractRefs = contractRefHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(o -> new GeneralKey(o.getPartyId(), o.getContractId()), Collectors.toList()))
                .entrySet().stream()
                .map(shopKeyListEntry -> contractMerger.merge(shopKeyListEntry.getKey(), shopKeyListEntry.getValue()))
                .collect(Collectors.toList());
        if (!contractRefs.isEmpty()) {
            partyManagementService.saveContractRefs(contractRefs);
        }
    }

    private void updateContractor(MachineEvent machineEvent, PartyChange change) {
        final List<CurrentContractor> contractors = currentContractorHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(o -> new GeneralKey(o.getPartyId(), o.getContractorId()), Collectors.toList()))
                .entrySet().stream()
                .map(shopKeyListEntry -> contractorEventMerger.merge(shopKeyListEntry.getKey(), shopKeyListEntry.getValue()))
                .collect(Collectors.toList());
        if (!contractors.isEmpty()) {
            partyManagementService.saveContractor(contractors);
        }
    }

    private void updateParties(MachineEvent machineEvent, PartyChange change) {
        final List<Party> parties = partyHandlers.stream()
                .filter(changeHandler -> changeHandler.accept(change))
                .flatMap(changeHandler -> changeHandler.handleChange(change, machineEvent).stream())
                .collect(Collectors.groupingBy(Party::getPartyId, Collectors.toList()))
                .entrySet().stream()
                .map(entryList -> partyEventMerger.mergeParty(entryList.getKey(), entryList.getValue()))
                .collect(Collectors.toList());
        if (!parties.isEmpty()) {
            partyManagementService.saveParty(parties);
        }
    }

}
