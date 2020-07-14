package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PartyBlockingMapper implements AdvancedMapper<PartyChange, MachineEvent, Party> {

    private final PartyService partyService;

    @Override
    public Party map(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        Blocking partyBlocking = change.getPartyBlocking();
        String partyId = event.getSourceId();

        Party party = partyService.getParty(partyId, storage);
        party.setBlocking(TBaseUtil.unionFieldToEnum(partyBlocking, com.rbkmoney.analytics.domain.db.enums.Blocking.class));
        if (partyBlocking.isSetBlocked()) {
            party.setBlockedReason(partyBlocking.getBlocked().getReason());
            party.setBlockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getBlocked().getSince()));
        } else if (partyBlocking.isSetUnblocked()) {
            party.setUnblockedReason(partyBlocking.getUnblocked().getReason());
            party.setUnblockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getUnblocked().getSince()));
        }

        return party;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_BLOCKING;
    }
}
