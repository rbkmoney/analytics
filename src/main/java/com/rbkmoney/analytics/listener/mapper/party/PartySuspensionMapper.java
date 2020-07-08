package com.rbkmoney.analytics.listener.mapper.party;

import com.rbkmoney.analytics.constant.EventType;
import com.rbkmoney.analytics.constant.SuspensionType;
import com.rbkmoney.analytics.dao.model.PartyRow;
import com.rbkmoney.analytics.dao.repository.clickhouse.ClickHousePartyRepository;
import com.rbkmoney.analytics.listener.mapper.Mapper;
import com.rbkmoney.damsel.domain.Suspension;
import com.rbkmoney.damsel.payment_processing.PartyChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class PartySuspensionMapper implements Mapper<PartyChange, MachineEvent, PartyRow> {

    private final ClickHousePartyRepository clickHousePartyRepository;

    @Override
    public PartyRow map(PartyChange change, MachineEvent event) {
        Suspension partySuspension = change.getPartySuspension();
        LocalDateTime eventCreatedAt = TypeUtil.stringToLocalDateTime(event.getCreatedAt());
        String partyId = event.getSourceId();

        PartyRow partyRow = clickHousePartyRepository.getParty(partyId);
        partyRow.setEventTime(eventCreatedAt);
        partyRow.setPartyId(partyId);
        partyRow.setSuspension(TBaseUtil.unionFieldToEnum(partySuspension, SuspensionType.class));
        if (partySuspension.isSetActive()) {
            partyRow.setSuspensionActiveSince(TypeUtil.stringToLocalDateTime(partySuspension.getActive().getSince()));
        } else if (partySuspension.isSetSuspended()) {
            partyRow.setSuspensionSuspendedSince(TypeUtil.stringToLocalDateTime(partySuspension.getSuspended().getSince()));
        }

        return partyRow;
    }

    @Override
    public EventType getChangeType() {
        return EventType.REVISION_CHANGED;
    }
}
