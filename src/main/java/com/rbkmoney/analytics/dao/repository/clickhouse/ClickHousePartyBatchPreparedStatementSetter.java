package com.rbkmoney.analytics.dao.repository.clickhouse;

import com.rbkmoney.analytics.dao.model.PartyRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

@RequiredArgsConstructor
public class ClickHousePartyBatchPreparedStatementSetter implements BatchPreparedStatementSetter {

    public static final String INSERT = "INSERT INTO analytic.events_sink_party (timestamp, eventTime, eventTimeHour," +
            " partyId, createdAt, email, blocking, blockedReason, blockedSince, unblockedReason, unblockedSince," +
            " suspension, suspensionActiveSince, suspensionSuspendedSince, revisionId, revisionChangedAt)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private final List<PartyRow> batch;

    @Override
    public void setValues(PreparedStatement ps, int i) throws SQLException {
        PartyRow row = batch.get(i);
        int l = 1;

        ps.setObject(l++, row.getEventTime().toLocalDate());
        ps.setLong(l++, row.getEventTime().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getEventTime().toInstant(ZoneOffset.UTC).truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        ps.setString(l++, row.getPartyId());
        ps.setLong(l++, row.getCreatedAt().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getEmail());
        ps.setString(l++, row.getBlocking().name());
        ps.setString(l++, row.getBlockedReason());
        ps.setLong(l++, row.getBlockedSince().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getUnblockedReason());
        ps.setLong(l++, row.getUnblockedSince().toEpochSecond(ZoneOffset.UTC));
        ps.setString(l++, row.getSuspension().name());
        ps.setLong(l++, row.getSuspensionActiveSince().toEpochSecond(ZoneOffset.UTC));
        ps.setLong(l++, row.getSuspensionSuspendedSince() != null ? row.getSuspensionSuspendedSince().toEpochSecond(ZoneOffset.UTC) : 0L);
        ps.setString(l++, row.getRevisionId());
        ps.setLong(l, row.getRevisionChangedAt().toEpochSecond(ZoneOffset.UTC));
    }

    @Override
    public int getBatchSize() {
        return batch.size();
    }
}
