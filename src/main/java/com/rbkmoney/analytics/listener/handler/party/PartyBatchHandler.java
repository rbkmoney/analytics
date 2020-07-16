package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.dao.repository.RepositoryFacade;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.Processor;
import com.rbkmoney.analytics.listener.handler.AdvancedBatchHandler;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class PartyBatchHandler implements AdvancedBatchHandler<PartyChange, MachineEvent> {

    private final RepositoryFacade repositoryFacade;

    @Getter
    private final List<ChangeHandler<PartyChange, MachineEvent, Party>> handlers;

    @Override
    public Processor handle(List<Map.Entry<MachineEvent, PartyChange>> changes) {
        return () -> {
            LocalStorage<Party> storage = new LocalStorage<>();
            for (Map.Entry<MachineEvent, PartyChange> change : changes) {
                handlePartyChange(change, storage);
            }
        };
    }

    private void handlePartyChange(Map.Entry<MachineEvent, PartyChange> changeEntry, LocalStorage<Party> storage) {
        PartyChange change = changeEntry.getValue();
        for (ChangeHandler<PartyChange, MachineEvent, Party> partyHandler : getHandlers()) {
            if (partyHandler.accept(change)) {
                partyHandler.handleChange(change, changeEntry.getKey(), storage);
            }
        }
    }

}
