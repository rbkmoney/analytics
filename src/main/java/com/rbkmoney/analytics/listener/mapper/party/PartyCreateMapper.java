package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.enums.Blocking;
import com.rbkmoney.analytics.domain.db.enums.Suspension;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyCreated;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class PartyCreateMapper implements AdvancedMapper<PartyChange, MachineEvent, Party> {

    @Override
    public Party map(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        PartyCreated partyCreated = change.getPartyCreated();
        LocalDateTime partyCreatedAt = TypeUtil.stringToLocalDateTime(partyCreated.getCreatedAt());
        Party party = new Party();
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

        return party;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_CREATED;
    }
}
