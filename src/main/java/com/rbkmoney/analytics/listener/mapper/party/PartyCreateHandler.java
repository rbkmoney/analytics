package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.enums.Blocking;
import com.rbkmoney.analytics.domain.db.enums.Suspension;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyCreated;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyCreateHandler implements ChangeHandler<PartyChange, MachineEvent, List<Party>> {

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        PartyCreated partyCreated = change.getPartyCreated();
        LocalDateTime partyCreatedAt = TypeUtil.stringToLocalDateTime(partyCreated.getCreatedAt());
        Party party = new Party();
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setPartyId(partyCreated.getId());
        party.setCreatedAt(partyCreatedAt);
        party.setEmail(partyCreated.getContactInfo().getEmail());
        party.setBlocking(Blocking.unblocked);
        party.setBlockedSince(partyCreatedAt);
        party.setSuspension(Suspension.active);
        party.setUnblockedSince(partyCreatedAt);
        party.setSuspensionActiveSince(partyCreatedAt);
        party.setRevisionId("0");
        party.setRevisionChangedAt(partyCreatedAt);

        return List.of(party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_CREATED;
    }
}
