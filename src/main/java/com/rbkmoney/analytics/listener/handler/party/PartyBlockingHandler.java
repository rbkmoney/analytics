package com.rbkmoney.analytics.listener.handler.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.repository.postgres.party.management.PartyDao;
import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.listener.handler.ChangeHandler;
import com.rbkmoney.analytics.listener.handler.merger.PartyEventMerger;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PartyBlockingHandler implements ChangeHandler<PartyChange, MachineEvent> {

    private final PartyDao partyDao;
    private final PartyEventMerger partyEventMerger;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void handleChange(PartyChange change, MachineEvent event) {
        Blocking partyBlocking = change.getPartyBlocking();
        String partyId = event.getSourceId();

        Party party = new Party();
        party.setPartyId(partyId);
        party.setEventId(event.getEventId());
        party.setEventTime(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        party.setBlocking(TBaseUtil.unionFieldToEnum(partyBlocking,
                com.rbkmoney.analytics.domain.db.enums.Blocking.class));
        if (partyBlocking.isSetBlocked()) {
            party.setBlockedReason(partyBlocking.getBlocked().getReason());
            party.setBlockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getBlocked().getSince()));
        } else if (partyBlocking.isSetUnblocked()) {
            party.setUnblockedReason(partyBlocking.getUnblocked().getReason());
            party.setUnblockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getUnblocked().getSince()));
        }
        final Party mergedParty = partyEventMerger.mergeParty(partyId, party);
        partyDao.saveParty(mergedParty);

        log.debug("Party block event change saveParty: {}", mergedParty);
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_BLOCKING;
    }
}
