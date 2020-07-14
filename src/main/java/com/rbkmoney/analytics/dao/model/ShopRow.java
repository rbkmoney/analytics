package com.rbkmoney.analytics.dao.model;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.SuspensionType;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
public class ShopRow {

    private String partyId;
    private String shopdId;
    private String categoryId;
    private String contractId;
    private String payoutToolId;
    private String payoutScheduleId;
    private LocalDateTime createdAt;
    private BlockingType blocking;
    private String blockedReason;
    private LocalDateTime blockedSince;
    private String unblockedReason;
    private LocalDateTime unblockedSince;
    private SuspensionType suspension;
    private LocalDateTime suspensionActiveSince;
    private LocalDateTime suspensionSuspendedSince;
    private String detailsName;
    private String detailsDescription;
    private String locationUrl;
    private String accountCurrencyCode;
    private String accountSettlement;
    private String accountGuarantee;
    private String accountPayout;

}
