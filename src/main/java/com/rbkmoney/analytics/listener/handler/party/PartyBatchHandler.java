package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.AdvancedBatchHandler;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

@Component
@RequiredArgsConstructor
public class PartyBatchHandler implements AdvancedBatchHandler<PartyChange, MachineEvent> {

    private final RepositoryFacade repositoryFacade;

    private final List<AdvancedMapper<PartyChange, MachineEvent, Party>> mappers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        LocalStorage<Party> localStorage = new LocalStorage<>();
        List<Party> partyRows = changes.stream()
                .map(changeWithParent -> {
                    PartyChange change = changeWithParent.getValue();
                    for (AdvancedMapper<PartyChange, MachineEvent, Party> partyMapper : getMappers()) {
                        if (partyMapper.accept(change)) {
                            Party party = partyMapper.map(change, changeWithParent.getKey(), localStorage);
                            if (party != null) {
                                localStorage.put(party.getPartyId(), party);
                            }

                            return party;
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertParties(partyRows);
    }

    @Override
    public List<AdvancedMapper<PartyChange, MachineEvent, Party>> getMappers() {
        return mappers;
    }
}
