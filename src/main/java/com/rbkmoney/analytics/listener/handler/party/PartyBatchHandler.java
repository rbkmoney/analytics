package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.model.PartyRow;
import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.BatchHandler;
import com.rbkmoney.analytics.listener.mapper.Mapper;
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
public class PartyBatchHandler implements BatchHandler<PartyChange, MachineEvent> {

    private final RepositoryFacade repositoryFacade;

    private final List<Mapper<PartyChange, MachineEvent, PartyRow>> mappers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        List<PartyRow> partyRows = changes.stream()
                .map(changeWithParent -> {
                    PartyChange change = changeWithParent.getValue();
                    for (Mapper<PartyChange, MachineEvent, PartyRow> partyMapper : getMappers()) {
                        if (partyMapper.accept(change)) {
                            return partyMapper.map(change, changeWithParent.getKey());
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(toList());

        return () -> repositoryFacade.insertParties(partyRows);
    }

    @Override
    public List<Mapper<PartyChange, MachineEvent, PartyRow>> getMappers() {
        return mappers;
    }
}
