package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.AdvancedMapper;
import com.rbkmoney.analytics.listener.mapper.LocalStorage;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PartySuspensionMapper implements AdvancedMapper<PartyChange, MachineEvent, Party> {

    private final PartyService partyService;

    @Override
    public Party map(PartyChange change, MachineEvent event, LocalStorage<Party> storage) {
        Suspension partySuspension = change.getPartySuspension();
        String partyId = event.getSourceId();

        Party party = partyService.getParty(partyId, storage);
        party.setSuspension(TBaseUtil.unionFieldToEnum(partySuspension, com.rbkmoney.analytics.domain.db.enums.Suspension.class));
        if (partySuspension.isSetActive()) {
            party.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(partySuspension.getActive().getSince()));
        } else if (partySuspension.isSetSuspended()) {
            party.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(partySuspension.getSuspended().getSince()));
        }

        return party;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_SUSPENSION;
    }
}
