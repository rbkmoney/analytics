package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.SuspensionType;
import com.rbkmoney.analytics.dao.model.PartyRow;
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
public class PartyCreateMapper implements Mapper<PartyChange, MachineEvent, PartyRow> {

    @Override
    public PartyRow map(PartyChange change, MachineEvent event) {
        PartyCreated partyCreated = change.getPartyCreated();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        LocalDateTime partyCreatedAt = TypeUtil.stringToLocalDateTime(partyCreated.getCreatedAt());
        PartyRow partyRow = new PartyRow();
        partyRow.setPartyId(partyCreated.getId());
        partyRow.setEventTime(eventCreatedAt);
        partyRow.setCreatedAt(partyCreatedAt);
        partyRow.setEmail(partyCreated.getContactInfo().getEmail());
        partyRow.setBlocking(BlockingType.unblocked);
        partyRow.setBlockedSince(partyCreatedAt);
        partyRow.setSuspension(SuspensionType.active);
        partyRow.setUnblockedSince(partyCreatedAt);
        partyRow.setSuspensionActiveSince(partyCreatedAt);
        partyRow.setRevisionId("0");
        partyRow.setRevisionChangedAt(partyCreatedAt);

        return partyRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_CREATED;
    }
}
