package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.handler.merger.PartyEventMerger;
import com.rbkmoney.analytics.listener.handler.ChangeHandler;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartySuspensionHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final PartyDao partyDao;
    private final PartyEventMerger partyEventMerger;

    @Override
    public void handleChange(PartyChange change, MachineEvent event) {
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

        final Party mergedParty = partyEventMerger.mergeParty(partyId, party);
        partyDao.saveParty(mergedParty);

        log.debug("Party suspension event saveParty: {}", party);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_SUSPENSION;
    }
}
