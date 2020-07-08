package com.rbkmoney.analytics.dao.repository.clickhouse.mapper;

import com.rbkmoney.analytics.constant.BlockingType;
import com.rbkmoney.analytics.constant.SuspensionType;
import com.rbkmoney.analytics.dao.model.PartyRow;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Component
public class PartyRowMapper implements RowMapper<PartyRow> {

    @Override
    public PartyRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        PartyRow partyRow = new PartyRow();
        partyRow.setEventTime(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("eventTime")), ZoneOffset.UTC));
        partyRow.setCreatedAt(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("createdAt")), ZoneOffset.UTC));
        partyRow.setPartyId(rs.getString("partyId"));
        partyRow.setEmail(rs.getString("email"));
        partyRow.setBlocking(BlockingType.valueOf(rs.getString("blocking")));
        partyRow.setBlockedReason(rs.getString("blockedReason"));
        partyRow.setBlockedSince(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("blockedSince")), ZoneOffset.UTC));
        partyRow.setUnblockedReason(rs.getString("unblockedReason"));
        partyRow.setUnblockedSince(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("unblockedSince")), ZoneOffset.UTC));
        partyRow.setSuspension(SuspensionType.valueOf(rs.getString("suspension")));
        partyRow.setSuspensionActiveSince(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("suspensionActiveSince")), ZoneOffset.UTC));
        partyRow.setSuspensionSuspendedSince(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("suspensionSuspendedSince")), ZoneOffset.UTC));
        partyRow.setRevisionId(rs.getString("revisionId"));
        partyRow.setRevisionChangedAt(LocalDateTime.ofInstant(Instant.ofEpochSecond(rs.getLong("revisionChangedAt")), ZoneOffset.UTC));

        return partyRow;
    }

}
