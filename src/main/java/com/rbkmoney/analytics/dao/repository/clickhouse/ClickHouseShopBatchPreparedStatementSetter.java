package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.ShopRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RequiredArgsConstructor
public class ClickHouseShopBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_shop (timestamp, eventTime, eventTimeHour," +
            " partyId, shopId, categoryId, contractId, payoutToolId, payoutScheduleId, createdAt, blocking, blockedReason," +
            " blockedSince, unblockedReason, unblockedSince, suspension, suspensionActiveSince," +
            " suspensionSuspendedSince, detailsName, detailsDescription, locationUrl, accountCurrencyCode, accountSettlement," +
            " accountGuarantee, accountPayout)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<ShopRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        ShopRow row = batch.get(i);
        int l = 1;

        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setString(l++, row.getPartyId());
        ps.setString(l++, row.getShopdId());
        ps.setString(l++, row.getCategoryId());
        ps.setString(l++, row.getContractId());
        ps.setString(l++, row.getPayoutToolId());
        ps.setString(l++, row.getPayoutScheduleId());
        ps.setLong(l++, row.getCreatedAt().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getBlocking().name());
        ps.setString(l++, row.getBlockedReason());
        ps.setLong(l++, row.getBlockedSince().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getUnblockedReason());
        ps.setLong(l++, row.getUnblockedSince().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getSuspension().name());
        ps.setLong(l++, row.getSuspensionActiveSince().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getSuspensionSuspendedSince() != null ? row.getSuspensionSuspendedSince().toEpochSecond(ZoneOffset.UTC) : 0L);
        ps.setString(l++, row.getDetailsName());
        ps.setString(l++, row.getDetailsDescription());
        ps.setString(l++, row.getLocationUrl());
        ps.setString(l++, row.getAccountCurrencyCode());
        ps.setString(l++, row.getAccountSettlement());
        ps.setString(l++, row.getAccountGuarantee());
        ps.setString(l, row.getAccountPayout());
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
