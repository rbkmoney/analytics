package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.domain.db.tables.pojos.Contractor;
import com.rbkmoney.analytics.listener.handler.merger.ContractorEventMerger;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyManagementService;
import com.rbkmoney.analytics.service.model.GeneralKey;
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
public class ContractorEventHandler implements PartyManagementEventHandler {

    private final List<ChangeHandler<PartyChange, MachineEvent, List<Contractor>>> currentContractorHandlers;
    private final ContractorEventMerger contractorEventMerger;
    private final PartyManagementService partyManagementService;

    @Override
    public void handle(MachineEvent machineEvent, PartyChange change) {
        final List<Contractor> contractors = currentContractorHandlers.stream()
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

}
