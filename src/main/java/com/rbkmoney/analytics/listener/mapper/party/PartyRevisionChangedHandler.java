package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyRevisionChanged;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PartyRevisionChangedHandler implements ChangeHandler<PartyChange, MachineEvent, List<Party>> {

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        PartyRevisionChanged partyRevisionChanged = change.getRevisionChanged();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setRevisionId(String.valueOf(partyRevisionChanged.getRevision()));
        party.setRevisionChangedAt(TypeUtil.stringToLocalDateTime(partyRevisionChanged.getTimestamp()));

        return List.of(party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.REVISION_CHANGED;
    }
}
