package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.mapper.ChangeHandler;
import com.rbkmoney.analytics.service.PartyService;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PartySuspensionHandler implements ChangeHandler<PartyChange, MachineEvent, List<Party>> {

    @Override
    public List<Party> handleChange(PartyChange change, MachineEvent event) {
        Suspension partySuspension = change.getPartySuspension();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setSuspension(TBaseUtil.unionFieldToEnum(partySuspension, com.rbkmoney.analytics.domain.db.enums.Suspension.class));
        if (partySuspension.isSetActive()) {
            party.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(partySuspension.getActive().getSince()));
        } else if (partySuspension.isSetSuspended()) {
            party.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(partySuspension.getSuspended().getSince()));
        }

        return List.of(party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_SUSPENSION;
    }
}
