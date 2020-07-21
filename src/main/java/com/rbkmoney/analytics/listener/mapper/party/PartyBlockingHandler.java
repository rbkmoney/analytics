package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PartyBlockingHandler implements ChangeHandler<PartyChange, MachineEvent, List<Party>> {

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        Blocking partyBlocking = change.getPartyBlocking();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setBlocking(TBaseUtil.unionFieldToEnum(partyBlocking, com.rbkmoney.analytics.domain.db.enums.Blocking.class));
        if (partyBlocking.isSetBlocked()) {
            party.setBlockedReason(partyBlocking.getBlocked().getReason());
            party.setBlockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getBlocked().getSince()));
        } else if (partyBlocking.isSetUnblocked()) {
            party.setUnblockedReason(partyBlocking.getUnblocked().getReason());
            party.setUnblockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getUnblocked().getSince()));
        }

        return List.of(party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_BLOCKING;
    }
}
