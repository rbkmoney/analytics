package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.handler.merger.PartyEventMerger;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyEventHandler implements PartyManagementEventHandler{

    private final List<ChangeHandler<PartyChange, MachineEvent, List<Party>>> partyHandlers;
    private final PartyEventMerger partyEventMerger;
    private final PartyManagementService partyManagementService;

    @Override
    public void handle(MachineEvent machineEvent, PartyChange change) {
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
