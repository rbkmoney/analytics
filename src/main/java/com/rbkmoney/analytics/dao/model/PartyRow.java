package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class PartyRow {

    private LocalDateTime eventTime;

    private String partyId;
    private LocalDateTime createdAt;
    private String email;
    private BlockingType blocking;
    private String blockedReason;
    private LocalDateTime blockedSince;
    private String unblockedReason;
    private LocalDateTime unblockedSince;
    private SuspensionType suspension;
    private LocalDateTime suspensionActiveSince;
    private LocalDateTime suspensionSuspendedSince;
    private String revisionId;
    private LocalDateTime revisionChangedAt;

}
