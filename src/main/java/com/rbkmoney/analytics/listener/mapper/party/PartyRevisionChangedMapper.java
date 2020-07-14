package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyRevisionChanged;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PartyRevisionChangedMapper implements AdvancedMapper<PartyChange, MachineEvent, Party> {

    private final PartyService partyService;

    @Override
    public Party map(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        PartyRevisionChanged partyRevisionChanged = change.getRevisionChanged();
        String partyId = event.getSourceId();

        Party party = partyService.getParty(partyId, storage);
        party.setRevisionId(String.valueOf(partyRevisionChanged.getRevision()));
        party.setRevisionChangedAt(TypeUtil.stringToLocalDateTime(partyRevisionChanged.getTimestamp()));

        return party;
    }

    @Override
    public EventType getChangeType() {
        return EventType.REVISION_CHANGED;
    }
}
