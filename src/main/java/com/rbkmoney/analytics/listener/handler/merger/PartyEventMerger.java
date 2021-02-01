package com.rbkmoney.analytics.listener.handler.merger;

import com.rbkmoney.analytics.domain.db.tables.pojos.Party;
import com.rbkmoney.analytics.service.PartyManagementService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class PartyEventMerger {

    private final PartyManagementService partyManagementService;

    public Party mergeParty(String partyId, List<Party> parties) {
        Party targetParty = partyManagementService.getParty(partyId);
        if (targetParty == null) {
            targetParty = new Party();
        }
        for (Party party : parties) {
            targetParty.setPartyId(partyId);
            targetParty.setEventId(party.getEventId());
            targetParty.setEventTime(party.getEventTime());
            targetParty.setCreatedAt(party.getCreatedAt() != null ? party.getCreatedAt() : targetParty.getCreatedAt());
            targetParty.setEmail(party.getEmail() != null ? party.getEmail() : targetParty.getEmail());
            targetParty.setBlocking(party.getBlocking() != null ? party.getBlocking() : targetParty.getBlocking());
            targetParty.setBlockedReason(party.getBlockedReason() != null ? party.getBlockedReason() : targetParty.getBlockedReason());
            targetParty.setBlockedSince(party.getBlockedSince() != null ? party.getBlockedSince() : targetParty.getBlockedSince());
            targetParty.setUnblockedReason(party.getUnblockedReason() != null ? party.getUnblockedReason() : targetParty.getUnblockedReason());
            targetParty.setUnblockedSince(party.getUnblockedSince() != null ? party.getUnblockedSince() : targetParty.getUnblockedSince());
            targetParty.setSuspension(party.getSuspension() != null ? party.getSuspension() : targetParty.getSuspension());
            targetParty.setSuspensionActiveSince(party.getSuspensionActiveSince() != null ? party.getSuspensionActiveSince() : targetParty.getSuspensionActiveSince());
            targetParty.setRevisionId(party.getRevisionId() != null ? party.getRevisionId() : targetParty.getRevisionId());
            targetParty.setRevisionChangedAt(party.getRevisionChangedAt() != null ? party.getRevisionChangedAt() : targetParty.getRevisionChangedAt());
        }

        return targetParty;
    }

}
