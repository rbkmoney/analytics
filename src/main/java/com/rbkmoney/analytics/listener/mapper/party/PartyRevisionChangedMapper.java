package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.dao.model.PartyRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePartyRepository;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.damsel.payment_processing.PartyRevisionChanged;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class PartyRevisionChangedMapper implements Mapper<PartyChange, MachineEvent, PartyRow> {

    private final ClickHousePartyRepository clickHousePartyRepository;

    @Override
    public PartyRow map(PartyChange change, MachineEvent event) {
        PartyRevisionChanged partyRevisionChanged = change.getRevisionChanged();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String partyId = event.getSourceId();

        PartyRow partyRow = clickHousePartyRepository.getParty(partyId);
        partyRow.setEventTime(eventCreatedAt);
        partyRow.setPartyId(partyId);
        partyRow.setRevisionId(String.valueOf(partyRevisionChanged.getRevision()));
        partyRow.setRevisionChangedAt(TypeUtil.stringToLocalDateTime(partyRevisionChanged.getTimestamp()));

        return partyRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.REVISION_CHANGED;
    }
}
