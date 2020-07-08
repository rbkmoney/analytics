package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PartyRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePartyRepository;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.domain.Blocking;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class PartyBlockingMapper implements Mapper<PartyChange, MachineEvent, PartyRow> {

    private final ClickHousePartyRepository clickHousePartyRepository;

    @Override
    public PartyRow map(PartyChange change, MachineEvent event) {
        Blocking partyBlocking = change.getPartyBlocking();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String partyId = event.getSourceId();

        PartyRow partyRow = clickHousePartyRepository.getParty(partyId);
        partyRow.setPartyId(partyId);
        partyRow.setEventTime(eventCreatedAt);
        partyRow.setBlocking(TBaseUtil.unionFieldToEnum(partyBlocking, BlockingType.class));
        if (partyBlocking.isSetBlocked()) {
            partyRow.setBlockedReason(partyBlocking.getBlocked().getReason());
            partyRow.setBlockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getBlocked().getSince()));
        } else if (partyBlocking.isSetUnblocked()) {
            partyRow.setUnblockedReason(partyBlocking.getUnblocked().getReason());
            partyRow.setUnblockedSince(TypeUtil.stringToLocalDateTime(partyBlocking.getUnblocked().getSince()));
        }

        return partyRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.PARTY_BLOCKING;
    }
}
